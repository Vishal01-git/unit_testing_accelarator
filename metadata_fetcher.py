import logging
import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc

class MetadataFetcher:
    def __init__(self, args):
        self.args = args

    def get_athena_tables(self):
        """Fetches list of tables from Athena database."""
        try:
            conn = connect(
                region_name=self.args['aws_region'],
                s3_staging_dir=self.args['s3_staging'],
                schema_name=self.args['athena_db'],
                work_group=self.args['athena_workgroup'],
                cursor_class=PandasCursor
            )
            query = f"SHOW TABLES IN `{self.args['athena_db']}`"
            df = conn.cursor().execute(query).as_pandas()
            # Athena usually returns 'tab_name'
            return sorted(df.iloc[:, 0].tolist())
        except Exception as e:
            logging.error(f"Failed to fetch Athena tables: {e}")
            raise

    def get_sql_tables(self):
        """Fetches list of tables (schema.table) from SQL Server."""
        try:
            # Handle Auth Method for Fetching
            if self.args.get('auth_method') == 'mfa':
                conn_str = (
                    f"Driver={{{self.args['mssql_driver']}}};"
                    f"Server={self.args['mssql_server']};"
                    f"Database={self.args['mssql_db']};"
                    f"UID={self.args['mssql_user']};"
                    "Authentication=ActiveDirectoryInteractive;"
                )
            else:
                conn_str = (
                    f"Driver={{{self.args['mssql_driver']}}};"
                    f"Server={self.args['mssql_server']};"
                    f"Database={self.args['mssql_db']};"
                    f"UID={self.args['mssql_user']};"
                    f"PWD={self.args['mssql_password']};"
                )

            query = """
                SELECT TABLE_SCHEMA + '.' + TABLE_NAME as full_name
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME
            """
            
            with pyodbc.connect(conn_str, timeout=30) as conn:
                df = pd.read_sql(query, conn)
                return sorted(df['full_name'].tolist())
        except Exception as e:
            logging.error(f"Failed to fetch SQL Server tables: {e}")
            raise
        
    def get_sql_primary_keys(self):
        """Fetches primary key columns for SQL Server tables and returns a dictionary."""
        try:
            if self.args.get('auth_method') == 'mfa':
                conn_str = (
                    f"Driver={{{self.args['mssql_driver']}}};"
                    f"Server={self.args['mssql_server']};"
                    f"Database={self.args['mssql_db']};"
                    f"UID={self.args['mssql_user']};"
                    "Authentication=ActiveDirectoryInteractive;"
                )
            else:
                conn_str = (
                    f"Driver={{{self.args['mssql_driver']}}};"
                    f"Server={self.args['mssql_server']};"
                    f"Database={self.args['mssql_db']};"
                    f"UID={self.args['mssql_user']};"
                    f"PWD={self.args['mssql_password']};"
                )

            query = """
                SELECT 
                    kcu.TABLE_SCHEMA + '.' + kcu.TABLE_NAME as full_name,
                    kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                    AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA 
                    AND tc.TABLE_NAME = kcu.TABLE_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION
            """
            
            with pyodbc.connect(conn_str, timeout=30) as conn:
                df = pd.read_sql(query, conn)
                # Group by table name and combine primary key columns with a comma
                if not df.empty:
                    pk_dict = df.groupby('full_name')['COLUMN_NAME'].apply(lambda x: ', '.join(x)).to_dict()
                    return pk_dict
                return {}
        except Exception as e:
            logging.error(f"Failed to fetch SQL Server primary keys: {e}")
            return {}