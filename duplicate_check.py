#!/usr/bin/env python3
"""
Duplicate Check Module for Unit Testing Validator
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
            query = f"""
                SELECT {key_list}, COUNT(*) as cnt
                FROM {self.args.athena_db}.{table}
                GROUP BY {key_list}
                HAVING COUNT(*) > 1
            """
            df = conn.cursor().execute(query).as_pandas()
            duplicates = df.to_dict('records')
            return duplicates
        except Exception as e:
            logging.error(f"Failed to check Athena duplicates for {table}: {str(e)}")
            raise

    def get_sqlserver_duplicates(self, table: str, primary_keys: list) -> list:
        """Check for duplicates in SQL Server table"""
        try:
            conn_str = (
                "Driver={ODBC Driver 17 for SQL Server};"
                f"Server={self.args.mssql_server};"
                f"Database={self.args.mssql_db};"
                "UID=;PWD=;"
                "Authentication=ActiveDirectoryInteractive;"
                "Encrypt=yes;"
            )
            with pyodbc.connect(conn_str, timeout=30) as conn:
                key_list = ', '.join(primary_keys)
                query = f"""
                    SELECT {key_list}, COUNT(*) as cnt
                    FROM {self.args.mssql_schema}.{table}
                    GROUP BY {key_list}
                    HAVING COUNT(*) > 1
                """
                df = pd.read_sql(query, conn)
                duplicates = df.to_dict('records')
                return duplicates
        except Exception as e:
            logging.error(f"Failed to check SQL Server duplicates for {table}: {str(e)}")
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
                table_result['issues'].append("No primary keys specified for duplicate check")
                table_result['has_issues'] = True
                results['tables'].append(table_result)
                results['error_tables'] += 1
                continue
            
            try:
                athena_duplicates = self.get_athena_duplicates(athena_table, primary_keys)
                sql_duplicates = self.get_sqlserver_duplicates(sql_table, primary_keys)
                
                table_result['duplicates'] = {
                    'athena_duplicates': athena_duplicates,
                    'sql_duplicates': sql_duplicates,
                    'status': 'No Duplicates' if not (athena_duplicates or sql_duplicates) else 'Duplicates Found',
                    'status_class': 'match' if not (athena_duplicates or sql_duplicates) else 'error'
                }
                
                if athena_duplicates:
                    table_result['issues'].append(f"Found {len(athena_duplicates)} duplicate rows in Athena")
                    table_result['has_issues'] = True
                if sql_duplicates:
                    table_result['issues'].append(f"Found {len(sql_duplicates)} duplicate rows in SQL Server")
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