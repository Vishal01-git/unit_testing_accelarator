#!/usr/bin/env python3
"""
Schema Comparison Module for Unit Testing Validator
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
        self.DATA_TYPE_MAPPING = {
            r'decimal\(\d+,\d+\)': 'decimal',
            r'numeric\(\d+,\d+\)': 'decimal',
            'nvarchar': 'varchar', 
            'string': 'varchar',
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
            'uniqueidentifier': 'uuid'
        }

    def normalize_name(self, name: str) -> str:
        """Standardize names for comparison"""
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

    def get_sqlserver_columns(self) -> pd.DataFrame:
        """Fetch SQL Server schema"""
        try:
            conn_str = (
                "Driver={ODBC Driver 17 for SQL Server};"
                f"Server={self.args.mssql_server};"
                f"Database={self.args.mssql_db};"
                "UID=;PWD=;"
                "Authentication=ActiveDirectoryInteractive;"
                "Encrypt=yes;"
            )
            logging.info("Initiating SQL Server connection with MFA...")
            with pyodbc.connect(conn_str, timeout=30) as conn:
                query = f"""
                    SELECT 
                        table_name as original_name,
                        column_name,
                        data_type
                    FROM information_schema.columns
                    WHERE table_schema = ?
                """
                df = pd.read_sql(query, conn, params=[self.args.mssql_schema])
                df['normalized_table_name'] = df['original_name'].apply(self.normalize_name)
                df['normalized_name'] = df['column_name'].apply(self.normalize_name)
                df['data_type'] = df['data_type'].apply(self.normalize_data_type)
                logging.info(f"Fetched {len(df)} columns from SQL Server database {self.args.mssql_db}, schema {self.args.mssql_schema}")
                return df
        except Exception as e:
            logging.error(f"SQL Server connection failed: {str(e)}")
            raise

    def compare_schemas(self, mappings: dict) -> dict:
        """Compare schemas and prepare results"""
        athena_df = self.get_athena_columns()
        sql_df = self.get_sqlserver_columns()
        results = {
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        for athena_table, config in mappings.items():
            sql_table = config['sql_table']
            table_result = {
                'id': self.normalize_name(athena_table),
                'athena_name': athena_table,
                'sql_name': sql_table,
                'has_issues': False,
                'issues': [],
                'columns': []
            }
            
            norm_athena = self.normalize_name(athena_table)
            norm_sql = self.normalize_name(sql_table)
            
            athena_exists = norm_athena in athena_df['normalized_table_name'].values
            sql_exists = norm_sql in sql_df['normalized_table_name'].values
            
            if not athena_exists:
                table_result['issues'].append(f"Table missing in Athena: {athena_table}")
                table_result['has_issues'] = True
                results['tables'].append(table_result)
                results['error_tables'] += 1
                logging.warning(f"Table {athena_table} not found in Athena")
                continue
                
            if not sql_exists:
                table_result['issues'].append(f"Table missing in SQL Server: {sql_table}")
                table_result['has_issues'] = True
                results['tables'].append(table_result)
                results['error_tables'] += 1
                logging.warning(f"Table {sql_table} not found in SQL Server")
                continue
                
            athena_cols = athena_df[athena_df['normalized_table_name'] == norm_athena]
            sql_cols = sql_df[sql_df['normalized_table_name'] == norm_sql]
            
            athena_col_names = set(athena_cols['normalized_name'])
            sql_col_names = set(sql_cols['normalized_name'])
            
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
                
                if athena_row['data_type'] != sql_row['data_type']:
                    col_result.update({
                        'status': 'Type Mismatch',
                        'status_class': 'error'
                    })
                    table_result['issues'].append(
                        f"Data type mismatch: {athena_table}.{athena_row['column_name']} (Athena: {athena_row['data_type']}) vs "
                        f"{sql_table}.{sql_row['column_name']} (SQL Server: {sql_row['data_type']})"
                    )
                
                table_result['columns'].append(col_result)
            
            for norm_col in sql_only:
                sql_row = sql_cols[sql_cols['normalized_name'] == norm_col].iloc[0]
                col_result = {
                    'normalized_name': norm_col,
                    'athena_column': '—',
                    'sql_column': sql_row['column_name'],
                    'athena_type': '—',
                    'sql_type': sql_row['data_type'],
                    'status': 'Missing in Athena',
                    'status_class': 'warning'
                }
                table_result['issues'].append(
                    f"Column missing in Athena: {sql_table}.{sql_row['column_name']} (Type: {sql_row['data_type']})"
                )
                table_result['columns'].append(col_result)
            
            for norm_col in athena_only:
                athena_row = athena_cols[athena_cols['normalized_name'] == norm_col].iloc[0]
                col_result = {
                    'normalized_name': norm_col,
                    'athena_column': athena_row['column_name'],
                    'sql_column': '—',
                    'athena_type': athena_row['data_type'],
                    'sql_type': '—',
                    'status': 'Missing in SQL Server',
                    'status_class': 'warning'
                }
                table_result['issues'].append(
                    f"Column missing in SQL Server: {athena_table}.{athena_row['column_name']} (Type: {athena_row['data_type']})"
                )
                table_result['columns'].append(col_result)
            
            table_result['has_issues'] = len(table_result['issues']) > 0
            if table_result['has_issues']:
                results['error_tables'] += 1
            else:
                results['valid_tables'] += 1
            
            results['tables'].append(table_result)
        
        return results