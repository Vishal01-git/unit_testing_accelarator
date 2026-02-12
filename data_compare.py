#!/usr/bin/env python3
"""
Data Comparison Module V2.0 (Vectorized)
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging
import re
import os
from typing import Dict, List
from schema_compare import SchemaComparator
from datetime import datetime

class DataComparator:
    def __init__(self, args):
        self.args = args
        
    def normalize_name(self, name: str) -> str:
        """Standardize names (removes schema for pure name match)"""
        name = str(name).split('.')[-1]
        return re.sub(r'[^a-z0-9_]', '', name.lower().strip())

    def get_athena_data(self, table: str, columns: List[str], primary_keys: List[str], sample_size: int) -> pd.DataFrame:
        """Fetch sample data from Athena"""
        try:
            conn = connect(
                region_name=self.args.aws_region,
                s3_staging_dir=self.args.s3_staging,
                schema_name=self.args.athena_db,
                work_group=self.args.athena_workgroup,
                cursor_class=PandasCursor
            )
            
            # Qualify columns
            qualified_cols = [f'"{col}"' for col in columns]
            col_list = ', '.join(qualified_cols)
            
            # Order by PKs for consistent sampling
            order_by = ', '.join([f'"{pk}"' for pk in primary_keys])
            
            query = f"""
                SELECT {col_list} 
                FROM "{self.args.athena_db}"."{table}"
                ORDER BY {order_by} 
                LIMIT {sample_size}
            """
            
            df = conn.cursor().execute(query).as_pandas()
            
            # Normalize for comparison
            for col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', '<NA>'], 'NULL')
            
            return df
        except Exception as e:
            logging.error(f"Athena fetch failed for {table}: {e}")
            raise

    def get_sqlserver_data(self, table_str: str, columns: List[str], primary_keys: List[str], sample_size: int) -> pd.DataFrame:
        """Fetch sample data from SQL Server (handles schema.table)"""
        try:
            mssql_username = "admin-airliquide-sas-big-prod-sql-apac-001"
            mssql_password = "QAXwmFTaa35S94Y9"
            conn_str = (
                "Driver={ODBC Driver 17 for SQL Server};"
                f"Server={self.args.mssql_server};"
                f"Database={self.args.mssql_db};"
                f"UID={mssql_username};"
                f"PWD={mssql_password};"
                "Encrypt=yes;"
            )
            
            # V2.0: Parse schema.table
            if '.' in table_str:
                schema, table = table_str.split('.', 1)
            else:
                schema = 'dbo'
                table = table_str

            with pyodbc.connect(conn_str, timeout=30) as conn:
                quoted_cols = [f'[{col}]' for col in columns]
                col_list = ', '.join(quoted_cols)
                
                quoted_pks = [f'[{pk}]' for pk in primary_keys]
                order_by = ', '.join(quoted_pks)
                
                query = f"""
                    SELECT TOP {sample_size} {col_list} 
                    FROM [{schema}].[{table}] 
                    ORDER BY {order_by}
                """
                
                df = pd.read_sql(query, conn)
                
                # Normalize for comparison
                for col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', '<NA>'], 'NULL')
                    
                return df
        except Exception as e:
            logging.error(f"SQL Server fetch failed for {table_str}: {e}")
            raise

    def compare_dataframes(self, athena_df: pd.DataFrame, sql_df: pd.DataFrame, 
                         athena_cols: List[str], sql_cols: List[str], 
                         primary_keys: List[str]) -> List[Dict]:
        """Vectorized Comparison"""
        mismatches = []
        
        # 1. Row Count check
        if len(athena_df) != len(sql_df):
            return [{'issue_type': 'row_count', 'message': f"Row count mismatch (Ath: {len(athena_df)}, SQL: {len(sql_df)})"}]
        
        if len(athena_df) == 0:
            return []

        # 2. Alignment
        try:
            # Set index to PKs for alignment
            athena_df = athena_df.set_index(primary_keys).sort_index()
            sql_df = sql_df.set_index(primary_keys).sort_index()
            
            # Rename SQL cols to match Athena cols
            sql_df.columns = athena_df.columns
            
        except KeyError as e:
            return [{'issue_type': 'pk_error', 'message': f"Primary key mismatch or missing column: {e}"}]

        # 3. Vectorized Diff
        # Create boolean mask where values differ
        ne_stacked = (athena_df != sql_df).stack()
        changed = ne_stacked[ne_stacked]
        
        # 4. Extract details
        for index, col in changed.index:
            ath_val = athena_df.loc[index, col]
            sql_val = sql_df.loc[index, col]
            
            # Formatting the PK for display (it's a tuple if multiple keys, scalar if one)
            pk_display = str(index)
            
            mismatches.append({
                'row': pk_display,
                'column': col,
                'athena_value': ath_val,
                'sql_value': sql_val,
                'message': f"Mismatch at PK {pk_display}, Col '{col}': '{ath_val}' != '{sql_val}'"
            })
            
            if len(mismatches) > 500:
                mismatches.append({'message': 'Mismatch limit reached (500+)'})
                break
                
        return mismatches

    def compare_data(self, mappings: Dict, sample_size: int = 100) -> Dict:
        """Main comparison flow"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        # Helper to get valid column pairs
        schema_comparator = SchemaComparator(self.args)
        athena_df_meta = schema_comparator.get_athena_columns()
        
        # We need a list of SQL tables for the batch fetch in schema comparator
        sql_targets = [m['sql_table'] for m in mappings.values()]
        sql_df_meta = schema_comparator.get_sqlserver_columns(sql_targets)
        
        for athena_table, config in mappings.items():
            sql_table = config['sql_table']
            primary_keys = config.get('primary_keys', [])
            
            table_result = {
                'id': self.normalize_name(athena_table),
                'athena_name': athena_table,
                'sql_name': sql_table,
                'status': 'Pending',
                'issues': [],
                'mismatches': []
            }
            
            try:
                if not primary_keys:
                    raise ValueError("No primary keys defined")

                # Normalize table names for meta-lookup
                norm_athena = self.normalize_name(athena_table)
                norm_sql_table = self.normalize_name(sql_table) # Extracts table name from schema.table
                
                # Filter meta DFs
                athena_cols = athena_df_meta[athena_df_meta['normalized_table_name'] == norm_athena]
                sql_cols = sql_df_meta[sql_df_meta['normalized_table_name'] == norm_sql_table]
                
                if athena_cols.empty or sql_cols.empty:
                    raise ValueError("Could not fetch schema metadata for comparison")

                # Find common columns
                common_norm_names = set(athena_cols['normalized_name']) & set(sql_cols['normalized_name'])
                
                # Build fetch lists
                fetch_athena_cols = []
                fetch_sql_cols = []
                
                for norm in common_norm_names:
                    # Skip PKs in this list, we'll add them specifically
                    # (This implementation logic can vary, simpler to just re-add PKs if missing or handle duplicates)
                    pass

                # Simplified column selection:
                # 1. Map normalized -> actual name for both
                ath_map = dict(zip(athena_cols['normalized_name'], athena_cols['column_name']))
                sql_map = dict(zip(sql_cols['normalized_name'], sql_cols['column_name']))
                
                final_athena_cols = []
                final_sql_cols = []
                
                # Ensure PKs are included first
                for pk in primary_keys:
                    norm_pk = self.normalize_name(pk)
                    if norm_pk in ath_map and norm_pk in sql_map:
                        pass # Valid
                    else:
                        raise ValueError(f"Primary Key {pk} not found in both tables")
                
                # Add all common columns
                for norm in common_norm_names:
                    final_athena_cols.append(ath_map[norm])
                    final_sql_cols.append(sql_map[norm])
                
                # Fetch
                df_ath = self.get_athena_data(athena_table, final_athena_cols, primary_keys, sample_size)
                df_sql = self.get_sqlserver_data(sql_table, final_sql_cols, primary_keys, sample_size)
                
                # Compare
                mismatches = self.compare_dataframes(df_ath, df_sql, final_athena_cols, final_sql_cols, primary_keys)
                
                if mismatches:
                    table_result['status'] = 'Mismatch'
                    table_result['issues'].append(f"Found {len(mismatches)} data mismatches")
                    table_result['mismatches'] = mismatches
                    results['error_tables'] += 1
                else:
                    table_result['status'] = 'Match'
                    results['valid_tables'] += 1
                    
            except Exception as e:
                table_result['status'] = 'Error'
                table_result['issues'].append(str(e))
                results['error_tables'] += 1
            
            results['tables'].append(table_result)
            
        return results