#!/usr/bin/env python3
"""
Enhanced Data Comparison Module for Unit Testing Validator
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging
import re
import os
from typing import Dict, List, Set, Optional
from schema_compare import SchemaComparator
from collections import Counter
from datetime import datetime

class DataComparator:
    def __init__(self, args):
        self.args = args
        self.output_dir = os.path.dirname(self.args.output) or '.'
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure logging for the comparator"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(self.output_dir, 'data_comparison.log')),
                logging.StreamHandler()
            ]
        )
        
    def normalize_name(self, name: str) -> str:
        """Standardize names for comparison (case-insensitive, special chars removed)"""
        if not isinstance(name, str):
            name = str(name)
        # Handle quoted identifiers
        name = name.strip('"[]`')
        name = name.split('.')[-1]  # Remove schema/table prefix if present
        name = name.lower().strip().replace(' ', '_')
        return re.sub(r'[^a-z0-9_]', '', name)

def get_athena_data(self, table: str, columns: List[str], primary_keys: List[str], sample_size: int) -> pd.DataFrame:
    """Fetch sample data from Athena with proper column qualification"""
    try:
        logging.info(f"Fetching {sample_size} rows from Athena table {table}")
        conn = connect(
            region_name=self.args.aws_region,
            s3_staging_dir=self.args.s3_staging,
            schema_name=self.args.athena_db,
            work_group=self.args.athena_workgroup,
            cursor_class=PandasCursor
        )
        
        # Qualify all columns with table alias to avoid ambiguity
        qualified_columns = [f'a."{col}"' if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col) else f'a.{col}' 
                          for col in columns]
        col_list = ', '.join(qualified_columns)
        
        # Qualify primary keys for ORDER BY
        qualified_pks = [f'a."{pk}"' if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', pk) else f'a.{pk}'
                       for pk in primary_keys]
        order_by_clause = ', '.join(qualified_pks)
        
        query = f"""
            SELECT {col_list} 
            FROM "{self.args.athena_db}"."{table}" a
            ORDER BY {order_by_clause} 
            LIMIT {sample_size}
        """
        
        logging.debug(f"Executing Athena query: {query}")
        cursor = conn.cursor()
        df = cursor.execute(query).as_pandas()
        
        # Remove table alias from column names in the result
        df.columns = [col.split('.')[-1].strip('"') for col in df.columns]
        
        # Standardize data
        for col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', ''], 'NULL')
            
        return df
        
    except Exception as e:
        logging.error(f"Athena query failed for table {table}: {str(e)}")
        raise

    def get_sqlserver_data(self, table: str, columns: List[str], primary_keys: List[str], sample_size: int) -> pd.DataFrame:
        """Fetch sample data from SQL Server with robust error handling"""
        try:
            logging.info(f"Fetching {sample_size} rows from SQL Server table {table}")
            conn_str = (
                f"Driver={{{self.args.mssql_driver}}};"
                f"Server={self.args.mssql_server};"
                f"Database={self.args.mssql_db};"
                f"UID={self.args.mssql_user or ''};"
                f"PWD={self.args.mssql_password or ''};"
                "TrustServerCertificate=yes;"
                "Authentication=ActiveDirectoryIntegrated;" if not self.args.mssql_user else ""
                "Encrypt=yes;"
            )
            
            with pyodbc.connect(conn_str, timeout=30) as conn:
                # Properly quote all column names for SQL Server
                quoted_columns = [f'[{col}]' for col in columns]
                col_list = ', '.join(quoted_columns)
                
                # Build ORDER BY clause with proper quoting
                quoted_pks = [f'[{pk}]' for pk in primary_keys]
                order_by_clause = ', '.join(quoted_pks)
                
                query = f"""
                    SELECT TOP {sample_size} {col_list} 
                    FROM [{self.args.mssql_schema}].[{table}] 
                    ORDER BY {order_by_clause}
                """
                
                logging.debug(f"Executing SQL Server query: {query}")
                df = pd.read_sql(query, conn)
                
                # Standardize data types and handle nulls
                for col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', ''], 'NULL')
                    
                logging.info(f"Retrieved {len(df)} rows from SQL Server")
                return df
                
        except Exception as e:
            logging.error(f"SQL Server query failed for table {table}: {str(e)}")
            raise

    def _validate_primary_keys(self, df: pd.DataFrame, primary_keys: List[str], source: str) -> List[str]:
        """Validate that primary keys exist and have no nulls"""
        issues = []
        for pk in primary_keys:
            if pk not in df.columns:
                issues.append(f"Primary key {pk} not found in {source} data")
            elif df[pk].isnull().any():
                issues.append(f"Null values found in primary key {pk} in {source} data")
        return issues

    def compare_dataframes(self, athena_df: pd.DataFrame, sql_df: pd.DataFrame, 
                         athena_cols: List[str], sql_cols: List[str], 
                         primary_keys: List[str]) -> List[Dict]:
        """Compare two dataframes and return mismatches"""
        mismatches = []
        
        # Ensure we have the same number of rows
        if len(athena_df) != len(sql_df):
            return [{
                'issue_type': 'row_count_mismatch',
                'message': f"Row count mismatch (Athena: {len(athena_df)}, SQL Server: {len(sql_df)})"
            }]
        
        # Sort by primary keys to ensure alignment
        athena_df = athena_df.sort_values(primary_keys).reset_index(drop=True)
        sql_df = sql_df.sort_values(primary_keys).reset_index(drop=True)
        
        # Verify primary keys match exactly
        for idx in range(len(athena_df)):
            for pk in primary_keys:
                ath_val = athena_df.at[idx, pk]
                sql_val = sql_df.at[idx, pk]
                if ath_val != sql_val:
                    mismatches.append({
                        'row': idx + 1,
                        'column': pk,
                        'issue_type': 'primary_key_mismatch',
                        'athena_value': ath_val,
                        'sql_value': sql_val,
                        'message': f"Primary key mismatch in row {idx+1}: {pk} (Athena: {ath_val}, SQL Server: {sql_val})"
                    })
        
        # Compare all other columns
        for col_idx, (ath_col, sql_col) in enumerate(zip(athena_cols, sql_cols)):
            for row_idx in range(len(athena_df)):
                ath_val = athena_df.at[row_idx, ath_col]
                sql_val = sql_df.at[row_idx, sql_col]
                
                # Special handling for numeric/date fields that might have different string representations
                if ath_val != sql_val:
                    # Try numeric comparison if values appear numeric
                    try:
                        ath_num = float(ath_val) if '.' in ath_val else int(ath_val)
                        sql_num = float(sql_val) if '.' in sql_val else int(sql_val)
                        if abs(ath_num - sql_num) < 1e-9:  # Account for floating point precision
                            continue
                    except (ValueError, TypeError):
                        pass
                    
                    # Try date comparison
                    try:
                        ath_date = datetime.strptime(ath_val, '%Y-%m-%d %H:%M:%S')
                        sql_date = datetime.strptime(sql_val, '%Y-%m-%d %H:%M:%S.%f')  # SQL Server often has milliseconds
                        if ath_date == sql_date:
                            continue
                    except (ValueError, TypeError):
                        pass
                    
                    mismatches.append({
                        'row': row_idx + 1,
                        'column': ath_col,
                        'issue_type': 'value_mismatch',
                        'athena_value': ath_val,
                        'sql_value': sql_val,
                        'message': f"Value mismatch in row {row_idx+1}, column {ath_col}: (Athena: {ath_val}, SQL Server: {sql_val})"
                    })
        
        return mismatches

    def compare_data(self, mappings: Dict, sample_size: int = 100) -> Dict:
        """Main comparison method with comprehensive error handling"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'sample_size': sample_size,
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': [],
            'summary': {}
        }
        
        schema_comparator = SchemaComparator(self.args)
        athena_df = schema_comparator.get_athena_columns()
        sql_df = schema_comparator.get_sqlserver_columns()
        
        for athena_table, config in mappings.items():
            sql_table = config['sql_table']
            primary_keys = config.get('primary_keys', [])
            exclude_columns = config.get('exclude_columns', [])
            
            table_result = {
                'table_id': self.normalize_name(athena_table),
                'athena_name': athena_table,
                'sql_name': sql_table,
                'primary_keys': primary_keys,
                'excluded_columns': exclude_columns,
                'status': 'pending',
                'issues': [],
                'mismatches': [],
                'row_counts': {
                    'athena': 0,
                    'sql_server': 0
                },
                'columns_compared': []
            }
            
            try:
                # Normalize names for comparison
                norm_athena = self.normalize_name(athena_table)
                norm_sql = self.normalize_name(sql_table)
                
                # Validate table existence
                if norm_athena not in athena_df['normalized_table_name'].values:
                    raise ValueError(f"Table not found in Athena: {athena_table}")
                    
                if norm_sql not in sql_df['normalized_table_name'].values:
                    raise ValueError(f"Table not found in SQL Server: {sql_table}")
                
                # Get columns with proper handling
                athena_cols = athena_df[athena_df['normalized_table_name'] == norm_athena]
                sql_cols = sql_df[sql_df['normalized_table_name'] == norm_sql]
                
                # Handle column selection and exclusions
                athena_col_names = set(athena_cols['normalized_name'])
                sql_col_names = set(sql_cols['normalized_name'])
                exclude_col_names = {self.normalize_name(col) for col in exclude_columns}
                common_cols = athena_col_names & sql_col_names - exclude_col_names
                
                if not common_cols:
                    raise ValueError("No common columns after exclusions")
                
                # Validate primary keys
                if not primary_keys:
                    raise ValueError("No primary keys specified")
                
                norm_pks = [self.normalize_name(pk) for pk in primary_keys]
                if len(norm_pks) != len(set(norm_pks)):
                    dupes = [k for k, v in Counter(norm_pks).items() if v > 1]
                    raise ValueError(f"Duplicate primary keys: {dupes}")
                
                # Build column lists ensuring no duplicates
                selected_athena_cols = []
                selected_sql_cols = []
                seen_cols = set()
                
                for norm_col in common_cols:
                    if norm_col in seen_cols:
                        continue
                    
                    ath_col = athena_cols[athena_cols['normalized_name'] == norm_col]['column_name'].iloc[0]
                    sql_col = sql_cols[sql_cols['normalized_name'] == norm_col]['column_name'].iloc[0]
                    
                    selected_athena_cols.append(ath_col)
                    selected_sql_cols.append(sql_col)
                    seen_cols.add(norm_col)
                
                # Combine PKs and other columns
                all_athena_cols = primary_keys + selected_athena_cols
                all_sql_cols = primary_keys + selected_sql_cols
                
                # Fetch data
                athena_data = self.get_athena_data(athena_table, all_athena_cols, primary_keys, sample_size)
                sql_data = self.get_sqlserver_data(sql_table, all_sql_cols, primary_keys, sample_size)
                
                # Store row counts
                table_result['row_counts']['athena'] = len(athena_data)
                table_result['row_counts']['sql_server'] = len(sql_data)
                table_result['columns_compared'] = selected_athena_cols
                
                # Validate data
                pk_issues = []
                pk_issues.extend(self._validate_primary_keys(athena_data, primary_keys, "Athena"))
                pk_issues.extend(self._validate_primary_keys(sql_data, primary_keys, "SQL Server"))
                
                if pk_issues:
                    raise ValueError("\n".join(pk_issues))
                
                # Perform comparison
                mismatches = self.compare_dataframes(
                    athena_data, sql_data, 
                    selected_athena_cols, selected_sql_cols,
                    primary_keys
                )
                
                if mismatches:
                    table_result['mismatches'] = mismatches
                    table_result['status'] = 'mismatch'
                    table_result['issues'].append(f"Found {len(mismatches)} mismatches")
                    results['error_tables'] += 1
                else:
                    table_result['status'] = 'match'
                    results['valid_tables'] += 1
                
            except Exception as e:
                table_result['status'] = 'error'
                table_result['issues'].append(str(e))
                results['error_tables'] += 1
                logging.error(f"Error comparing {athena_table} to {sql_table}: {e}")
            
            results['tables'].append(table_result)
        
        # Generate summary statistics
        results['summary'] = {
            'match_percentage': round(results['valid_tables'] / results['total_tables'] * 100, 2) if results['total_tables'] > 0 else 0,
            'total_mismatches': sum(len(t['mismatches']) for t in results['tables']),
            'most_common_mismatch_columns': self._get_common_mismatch_columns(results)
        }
        
        return results

    def _get_common_mismatch_columns(self, results: Dict) -> List[Dict]:
        """Identify columns with the most mismatches"""
        column_counts = Counter()
        for table in results['tables']:
            for mismatch in table['mismatches']:
                column_counts[mismatch['column']] += 1
                
        return [{'column': col, 'count': cnt} for col, cnt in column_counts.most_common(5)]