#!/usr/bin/env python3
"""
Schema Comparison Module V2.1
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import re
import logging

class SchemaComparator:
    def __init__(self, args):
        self.args = args
        
        self.IGNORE_COLUMNS = {
            'country_code','_start_time','_window_end','__lk_validation_failures'
        }

        self.DATA_TYPE_MAPPING = {
            'nvarchar': 'varchar', 
            'string': 'varchar',
            'nchar': 'varchar',
            'char': 'varchar',
            'text': 'varchar',
            'int': 'integer',
            'bigint': 'integer',
            'smallint': 'integer',
            'tinyint': 'integer',
            'datetime': 'timestamp',
            'datetime2': 'timestamp', 
            'date': 'date',
            'time': 'time',
            'float': 'float',
            'real': 'float',
            'bit': 'boolean',
            'binary': 'binary',
            'varbinary': 'binary',
            'uniqueidentifier': 'varchar',
            'money': 'decimal(19,4)',
            'smalldatetime': 'timestamp',
            'uuid': 'varchar'
        }

    def normalize_name(self, name: str) -> str:
        """Standardize names for comparison (removes schema prefix)"""
        if not isinstance(name, str):
            name = str(name)
        name = name.split('.')[-1]
        name = name.lower().strip().replace(' ', '_')
        return re.sub(r'[^a-z0-9_]', '', name)

    def normalize_data_type(self, dtype: str) -> str:
        """Standardize data type representations"""
        if not isinstance(dtype, str):
            return str(dtype)
        dtype = dtype.lower().strip()
        
        if dtype.startswith('decimal') or dtype.startswith('numeric'):
            dtype = dtype.replace('numeric', 'decimal')
            return re.sub(r'\s+', '', dtype) 

        for pattern, normalized in self.DATA_TYPE_MAPPING.items():
            if re.fullmatch(pattern, dtype):
                return normalized
        return dtype.split('(')[0]

    def get_athena_columns(self) -> pd.DataFrame:
        """Fetch Athena schema"""
        try:
            conn = connect(
                region_name=self.args.aws_region,
                s3_staging_dir=self.args.s3_staging,
                schema_name=self.args.athena_db,
                work_group=self.args.athena_workgroup,
                cursor_class=PandasCursor
            )
            query = f"""
                SELECT 
                    table_name as original_name,
                    column_name,
                    data_type
                FROM information_schema.columns
                WHERE table_schema = '{self.args.athena_db}'
            """
            df = conn.cursor().execute(query).as_pandas()
            df['normalized_table_name'] = df['original_name'].apply(self.normalize_name)
            df['normalized_name'] = df['column_name'].apply(self.normalize_name)
            df['data_type'] = df['data_type'].apply(self.normalize_data_type)
            logging.info(f"Fetched {len(df)} columns from Athena database {self.args.athena_db}")
            return df
        except Exception as e:
            logging.error(f"Athena query failed: {str(e)}")
            raise

    def get_sqlserver_columns(self, target_tables: list) -> pd.DataFrame:
        """Fetch SQL Server schema for specific tables."""
        try:
            conn_str = (
                "Driver={ODBC Driver 17 for SQL Server};"
                f"Server={self.args.mssql_server};"
                f"Database={self.args.mssql_db};"
                "UID=;PWD=;"
                "Authentication=ActiveDirectoryInteractive;"
                # "Encrypt=yes;"
            )
            logging.info("Initiating SQL Server connection with MFA...")

        # try:
        #     mssql_username = ""
        #     mssql_password = ""
        #     conn_str = (
        #         "Driver={ODBC Driver 17 for SQL Server};"
        #         f"Server={self.args.mssql_server};"
        #         f"Database={self.args.mssql_db};"
        #         f"UID={mssql_username};"
        #         f"PWD={mssql_password};"
        #         # "Encrypt=yes;"
        #     )
            
            where_clauses = []
            params = []
            unique_targets = set(target_tables)
            
            for table_str in unique_targets:
                if '.' in table_str:
                    schema, table = table_str.split('.', 1)
                    where_clauses.append("(TABLE_SCHEMA = ? AND TABLE_NAME = ?)")
                    params.extend([schema, table])
                else:
                    where_clauses.append("(TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?)")
                    params.append(table_str)

            if not where_clauses:
                return pd.DataFrame()

            query_where = "WHERE " + " OR ".join(where_clauses)

            query = f"""
                SELECT 
                    TABLE_SCHEMA as schema_name,
                    TABLE_NAME as original_name,
                    COLUMN_NAME as column_name,
                    DATA_TYPE as raw_data_type,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM information_schema.columns
                {query_where}
            """
            
            with pyodbc.connect(conn_str, timeout=30) as conn:
                df = pd.read_sql(query, conn, params=params)
                
                def format_sql_type(row):
                    dtype = row['raw_data_type'].lower()
                    if dtype in ['decimal', 'numeric'] and pd.notnull(row['NUMERIC_PRECISION']):
                        prec = int(row['NUMERIC_PRECISION'])
                        scale = int(row['NUMERIC_SCALE']) if pd.notnull(row['NUMERIC_SCALE']) else 0
                        return f"decimal({prec},{scale})"
                    return dtype

                df['data_type'] = df.apply(format_sql_type, axis=1)
                
                df['normalized_table_name'] = df['original_name'].apply(self.normalize_name)
                df['normalized_schema'] = df['schema_name'].apply(self.normalize_name)
                df['normalized_name'] = df['column_name'].apply(self.normalize_name)
                df['data_type'] = df['data_type'].apply(self.normalize_data_type)
                
                return df
                
        except Exception as e:
            logging.error(f"SQL Server connection failed: {str(e)}")
            raise

    def compare_schemas(self, mappings: dict) -> dict:
        """Compare schemas with exclusions and custom exceptions"""
        
        sql_target_list = [config['sql_table'] for config in mappings.values()]
        
        athena_df = self.get_athena_columns()
        sql_df = self.get_sqlserver_columns(sql_target_list)
        
        results = {
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        for athena_table, config in mappings.items():
            sql_table_full = config['sql_table']
            
            table_result = {
                'id': self.normalize_name(athena_table),
                'athena_name': athena_table,
                'sql_name': sql_table_full,
                'has_issues': False,
                'issues': [],
                'columns': []
            }
            
            norm_athena_table = self.normalize_name(athena_table)
            
            if '.' in sql_table_full:
                sql_schema_part, sql_table_part = sql_table_full.split('.', 1)
                norm_sql_schema = self.normalize_name(sql_schema_part)
                norm_sql_table = self.normalize_name(sql_table_part)
            else:
                norm_sql_schema = 'dbo'
                norm_sql_table = self.normalize_name(sql_table_full)
            
            # Existence Checks
            athena_exists = norm_athena_table in athena_df['normalized_table_name'].values
            sql_cols_subset = sql_df[
                (sql_df['normalized_schema'] == norm_sql_schema) & 
                (sql_df['normalized_table_name'] == norm_sql_table)
            ]
            sql_exists = not sql_cols_subset.empty
            
            if not athena_exists:
                table_result['issues'].append(f"Table missing in Athena: {athena_table}")
                table_result['has_issues'] = True
                results['tables'].append(table_result)
                results['error_tables'] += 1
                continue
                
            if not sql_exists:
                table_result['issues'].append(f"Table missing in SQL Server: {sql_table_full}")
                table_result['has_issues'] = True
                results['tables'].append(table_result)
                results['error_tables'] += 1
                continue
            
            # Get Columns
            athena_cols = athena_df[athena_df['normalized_table_name'] == norm_athena_table]
            sql_cols = sql_cols_subset
            
            athena_col_names = set(athena_cols['normalized_name'])
            sql_col_names = set(sql_cols['normalized_name'])

            # --- NEW: Apply Exclusion Filter ---
            # Columns in IGNORE_COLUMNS are completely removed from consideration
            athena_col_names = {c for c in athena_col_names if c not in self.IGNORE_COLUMNS}
            sql_col_names = {c for c in sql_col_names if c not in self.IGNORE_COLUMNS}
            
            common_cols = athena_col_names & sql_col_names
            athena_only = athena_col_names - sql_col_names
            sql_only = sql_col_names - athena_col_names
            
            for norm_col in common_cols:
                athena_row = athena_cols[athena_cols['normalized_name'] == norm_col].iloc[0]
                sql_row = sql_cols[sql_cols['normalized_name'] == norm_col].iloc[0]
                
                col_result = {
                    'normalized_name': norm_col,
                    'athena_column': athena_row['column_name'],
                    'sql_column': sql_row['column_name'],
                    'athena_type': athena_row['data_type'],
                    'sql_type': sql_row['data_type'],
                    'status': 'Match',
                    'status_class': 'match'
                }
                
                # --- NEW: Custom Type Logic ---
                types_match = athena_row['data_type'] == sql_row['data_type']
                
                if not types_match:
                    # Exception: row_version (Athena: integer, SQL: binary/varbinary)
                    if (norm_col == 'row_version' and 
                        athena_row['data_type'] == 'integer' and 
                        sql_row['data_type'] == 'binary'):
                        types_match = True
                    
                    # Add more exceptions here if needed
                
                if not types_match:
                    col_result.update({'status': 'Type Mismatch', 'status_class': 'error'})
                    table_result['issues'].append(
                        f"Type mismatch: {athena_row['column_name']} ({athena_row['data_type']}) vs "
                        f"{sql_row['column_name']} ({sql_row['data_type']})"
                    )
                
                table_result['columns'].append(col_result)
            
            for norm_col in sql_only:
                sql_row = sql_cols[sql_cols['normalized_name'] == norm_col].iloc[0]
                table_result['issues'].append(f"Column missing in Athena: {sql_row['column_name']}")
                table_result['columns'].append({
                    'normalized_name': norm_col,
                    'athena_column': '—', 'sql_column': sql_row['column_name'],
                    'athena_type': '—', 'sql_type': sql_row['data_type'],
                    'status': 'Missing in Athena', 'status_class': 'warning'
                })
            
            for norm_col in athena_only:
                athena_row = athena_cols[athena_cols['normalized_name'] == norm_col].iloc[0]
                table_result['issues'].append(f"Column missing in SQL Server: {athena_row['column_name']}")
                table_result['columns'].append({
                    'normalized_name': norm_col,
                    'athena_column': athena_row['column_name'], 'sql_column': '—',
                    'athena_type': athena_row['data_type'], 'sql_type': '—',
                    'status': 'Missing in SQL Server', 'status_class': 'warning'
                })
            
            table_result['has_issues'] = len(table_result['issues']) > 0
            if table_result['has_issues']:
                results['error_tables'] += 1
            else:
                results['valid_tables'] += 1
            
            results['tables'].append(table_result)
        
        return results