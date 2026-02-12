#!/usr/bin/env python3
"""
Duplicate Check Module V2.0
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging

class DuplicateChecker:
    def __init__(self, args):
        self.args = args

    def get_athena_duplicates(self, table: str, primary_keys: list) -> list:
        """Check for duplicates in Athena table"""
        try:
            conn = connect(
                region_name=self.args.aws_region,
                s3_staging_dir=self.args.s3_staging,
                schema_name=self.args.athena_db,
                work_group=self.args.athena_workgroup,
                cursor_class=PandasCursor
            )
            key_list = ', '.join(primary_keys)
            # Limit to 100 to avoid memory overflow in UI
            query = f"""
                SELECT {key_list}, COUNT(*) as cnt
                FROM {self.args.athena_db}.{table}
                GROUP BY {key_list}
                HAVING COUNT(*) > 1
                LIMIT 100
            """
            df = conn.cursor().execute(query).as_pandas()
            return df.to_dict('records')
        except Exception as e:
            logging.error(f"Failed to check Athena duplicates for {table}: {str(e)}")
            raise

    def get_sqlserver_duplicates(self, table_str: str, primary_keys: list) -> list:
        """Check for duplicates in SQL Server table"""
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
                key_list = ', '.join([f"[{k}]" for k in primary_keys])
                query = f"""
                    SELECT TOP 100 {key_list}, COUNT(*) as cnt
                    FROM [{schema}].[{table}]
                    GROUP BY {key_list}
                    HAVING COUNT(*) > 1
                """
                df = pd.read_sql(query, conn)
                return df.to_dict('records')
        except Exception as e:
            logging.error(f"Failed to check SQL Server duplicates for {table_str}: {str(e)}")
            raise

    def check_duplicates(self, mappings: dict) -> dict:
        """Check for duplicates based on primary keys"""
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
                'duplicates': {}
            }
            
            if not primary_keys:
                table_result['issues'].append("Skipped: No primary keys specified")
                table_result['has_issues'] = True # Mark as issue so user notices
                results['tables'].append(table_result)
                results['error_tables'] += 1
                continue
            
            try:
                athena_dupes = self.get_athena_duplicates(athena_table, primary_keys)
                sql_dupes = self.get_sqlserver_duplicates(sql_table, primary_keys)
                
                has_dupes = bool(athena_dupes or sql_dupes)
                
                table_result['duplicates'] = {
                    'athena_duplicates': athena_dupes,
                    'sql_duplicates': sql_dupes,
                    'status': 'Duplicate Found' if has_dupes else 'No Duplicates',
                    'status_class': 'error' if has_dupes else 'match'
                }
                
                if athena_dupes:
                    table_result['issues'].append(f"Found {len(athena_dupes)} duplicate sets in Athena (showing top 100)")
                    table_result['has_issues'] = True
                if sql_dupes:
                    table_result['issues'].append(f"Found {len(sql_dupes)} duplicate sets in SQL Server (showing top 100)")
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