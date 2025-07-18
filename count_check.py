#!/usr/bin/env python3
"""
Row Count Check Module for Unit Testing Validator
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging

class CountChecker:
    def __init__(self, args):
        self.args = args

    def get_athena_count(self, table: str) -> int:
        """Fetch row count from Athena table"""
        try:
            conn = connect(
                region_name=self.args.aws_region,
                s3_staging_dir=self.args.s3_staging,
                schema_name=self.args.athena_db,
                work_group=self.args.athena_workgroup,
                cursor_class=PandasCursor
            )
            query = f"SELECT COUNT(*) as cnt FROM {self.args.athena_db}.{table}"
            df = conn.cursor().execute(query).as_pandas()
            count = int(df['cnt'].iloc[0])
            return count
        except Exception as e:
            logging.error(f"Failed to fetch Athena count for {table}: {str(e)}")
            raise

    def get_sqlserver_count(self, table: str) -> int:
        """Fetch row count from SQL Server table"""
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
                query = f"SELECT COUNT(*) as cnt FROM {self.args.mssql_schema}.{table}"
                df = pd.read_sql(query, conn)
                count = int(df['cnt'].iloc[0])
                return count
        except Exception as e:
            logging.error(f"Failed to fetch SQL Server count for {table}: {str(e)}")
            raise

    def check_counts(self, mappings: dict) -> dict:
        """Compare row counts"""
        results = {
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        for athena_table, config in mappings.items():
            sql_table = config['sql_table']
            table_result = {
                'id': athena_table.lower().replace(' ', '_'),
                'athena_name': athena_table,
                'sql_name': sql_table,
                'has_issues': False,
                'issues': [],
                'counts': {}
            }
            
            try:
                athena_count = self.get_athena_count(athena_table)
                sql_count = self.get_sqlserver_count(sql_table)
                table_result['counts'] = {
                    'athena_count': athena_count,
                    'sql_count': sql_count,
                    'status': 'Match' if athena_count == sql_count else 'Mismatch',
                    'status_class': 'match' if athena_count == sql_count else 'error'
                }
                if athena_count != sql_count:
                    table_result['issues'].append(
                        f"Row count mismatch: Athena ({athena_count}) vs SQL Server ({sql_count})"
                    )
                    table_result['has_issues'] = True
                    results['error_tables'] += 1
                else:
                    results['valid_tables'] += 1
            except Exception as e:
                table_result['issues'].append(str(e))
                table_result['has_issues'] = True
                results['error_tables'] += 1
            
            results['tables'].append(table_result)
        
        return results