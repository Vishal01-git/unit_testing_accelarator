#!/usr/bin/env python3
"""
Null Check Module V2.0
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging

class NullChecker:
    def __init__(self, args):
        self.args = args

    def get_athena_nulls(self, table: str, primary_keys: list) -> dict:
        """Check for nulls in Athena primary key columns"""
        try:
            conn = connect(
                region_name=self.args.aws_region,
                s3_staging_dir=self.args.s3_staging,
                schema_name=self.args.athena_db,
                work_group=self.args.athena_workgroup,
                cursor_class=PandasCursor
            )
            results = {}
            for key in primary_keys:
                query = f"""
                    SELECT COUNT(*) as cnt
                    FROM {self.args.athena_db}.{table}
                    WHERE {key} IS NULL
                """
                df = conn.cursor().execute(query).as_pandas()
                results[key] = int(df['cnt'].iloc[0])
            return results
        except Exception as e:
            logging.error(f"Failed to check Athena nulls for {table}: {str(e)}")
            raise

    def get_sqlserver_nulls(self, table_str: str, primary_keys: list) -> dict:
        """Check for nulls in SQL Server primary key columns"""
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
                results = {}
                for key in primary_keys:
                    query = f"""
                        SELECT COUNT(*) as cnt
                        FROM [{schema}].[{table}]
                        WHERE [{key}] IS NULL
                    """
                    df = pd.read_sql(query, conn)
                    results[key] = int(df['cnt'].iloc[0])
                return results
        except Exception as e:
            logging.error(f"Failed to check SQL Server nulls for {table_str}: {str(e)}")
            raise

    def check_nulls(self, mappings: dict) -> dict:
        """Check for nulls in primary key columns"""
        results = {
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        for athena_table, config in mappings.items():
            sql_table = config['sql_table']
            primary_keys = config.get('primary_keys', [])
            table_result = {
                'id': athena_table.lower().replace(' ', '_'),
                'athena_name': athena_table,
                'sql_name': sql_table,
                'has_issues': False,
                'issues': [],
                'nulls': {}
            }
            
            if not primary_keys:
                table_result['issues'].append("Skipped: No primary keys specified")
                table_result['has_issues'] = True
                results['tables'].append(table_result)
                results['error_tables'] += 1
                continue
            
            try:
                athena_nulls = self.get_athena_nulls(athena_table, primary_keys)
                sql_nulls = self.get_sqlserver_nulls(sql_table, primary_keys)
                
                has_nulls = any(v > 0 for v in athena_nulls.values()) or any(v > 0 for v in sql_nulls.values())
                
                table_result['nulls'] = {
                    'athena_nulls': athena_nulls,
                    'sql_nulls': sql_nulls,
                    'status': 'Nulls Found' if has_nulls else 'No Nulls',
                    'status_class': 'error' if has_nulls else 'match'
                }
                
                for key, count in athena_nulls.items():
                    if count > 0:
                        table_result['issues'].append(f"Athena: Column '{key}' has {count} NULLs")
                        table_result['has_issues'] = True
                
                for key, count in sql_nulls.items():
                    if count > 0:
                        table_result['issues'].append(f"SQL Server: Column '{key}' has {count} NULLs")
                        table_result['has_issues'] = True
                
                if table_result['has_issues']:
                    results['error_tables'] += 1
                else:
                    results['valid_tables'] += 1
            
            except Exception as e:
                table_result['issues'].append(str(e))
                table_result['has_issues'] = True
                results['error_tables'] += 1
            
            results['tables'].append(table_result)
        
        return results