import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging
import concurrent.futures

class CountChecker:
    def __init__(self, args):
        self.args = args

    def get_athena_count(self, table: str) -> int:
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
            return int(df['cnt'].iloc[0])
        except Exception as e:
            logging.error(f"Failed to fetch Athena count for {table}: {str(e)}")
            raise

    def get_sqlserver_count(self, table_str: str) -> int:
        try:
            if self.args.auth_method == 'mfa':
                conn_str = (
                    f"Driver={{{self.args.mssql_driver}}};"
                    f"Server={self.args.mssql_server};"
                    f"Database={self.args.mssql_db};"
                    f"UID={self.args.mssql_user};"
                    "Authentication=ActiveDirectoryInteractive;"
                )
            else:
                conn_str = (
                    f"Driver={{{self.args.mssql_driver}}};"
                    f"Server={self.args.mssql_server};"
                    f"Database={self.args.mssql_db};"
                    f"UID={self.args.mssql_user};"
                    f"PWD={self.args.mssql_password};"
                )
            
            if '.' in table_str: 
                full_obj_name = table_str
            else: 
                full_obj_name = f"dbo.{table_str}"

            with pyodbc.connect(conn_str, timeout=30) as conn:
                query = "SELECT COALESCE(SUM(rows), 0) as cnt FROM sys.partitions WHERE object_id = OBJECT_ID(?) AND index_id IN (0, 1)"
                df = pd.read_sql(query, conn, params=[full_obj_name])
                return int(df['cnt'].iloc[0])
                
        except Exception as e:
            logging.error(f"Failed to fetch SQL Server count for {table_str}: {str(e)}")
            raise

    def _process_single_table(self, athena_table: str, sql_table: str) -> tuple:
        """Worker function to process a single table mapping. NO CALLBACKS HERE (Thread-Safe)."""
        table_result = {
            'id': athena_table.lower().replace(' ', '_'), 
            'athena_name': athena_table, 
            'sql_name': sql_table, 
            'has_issues': False, 
            'issues': [], 
            'counts': {
                'athena_count': 'Error', 
                'sql_count': 'Error', 
                'status': 'Error', 
                'status_class': 'error'
            }
        }
        
        try:
            athena_count = self.get_athena_count(athena_table)
            sql_count = self.get_sqlserver_count(sql_table)
            
            status = 'Match' if athena_count == sql_count else 'Mismatch'
            status_class = 'match' if athena_count == sql_count else 'error'
            
            table_result['counts'] = {
                'athena_count': athena_count, 
                'sql_count': sql_count, 
                'status': status, 
                'status_class': status_class
            }
            
            if athena_count != sql_count:
                diff = abs(athena_count - sql_count)
                table_result['issues'].append(f"Row count mismatch: Athena ({athena_count}) vs SQL Server ({sql_count}). Diff: {diff}")
                table_result['has_issues'] = True
                return table_result, False
            else:
                return table_result, True
                
        except Exception as e:
            table_result['issues'].append(str(e))
            table_result['has_issues'] = True
            return table_result, False

    def check_counts(self, mappings: dict, callback=None) -> dict:
        results = {'total_tables': len(mappings), 'valid_tables': 0, 'error_tables': 0, 'tables': []}
        
        max_workers = min(10, len(mappings)) if len(mappings) > 0 else 1
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(self._process_single_table, ath_table, config['sql_table']): ath_table
                for ath_table, config in mappings.items()
            }
            
            # The UI callback is now safely executed in the main thread as futures complete
            for future in concurrent.futures.as_completed(future_to_table):
                athena_table = future_to_table[future]
                
                try:
                    table_result, is_valid = future.result()
                    results['tables'].append(table_result)
                    
                    if is_valid:
                        results['valid_tables'] += 1
                        if callback: callback(f"Count Match: {athena_table}")
                    else:
                        results['error_tables'] += 1
                        if callback: callback(f"Count Failed/Mismatch: {athena_table}")
                        
                except Exception as exc:
                    logging.error(f"{athena_table} generated an exception: {exc}")
                    results['error_tables'] += 1
                    if callback: callback(f"ERROR on {athena_table}: {str(exc)}")
                    
                    results['tables'].append({
                        'id': athena_table.lower().replace(' ', '_'),
                        'athena_name': athena_table,
                        'sql_name': mappings[athena_table]['sql_table'],
                        'has_issues': True,
                        'issues': [f"Critical execution error: {str(exc)}"],
                        'counts': {'athena_count': 'Error', 'sql_count': 'Error', 'status': 'Error', 'status_class': 'error'}
                    })
        
        return results