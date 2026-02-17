import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging
import re
import os
import xlsxwriter
from typing import Dict, List
from schema_compare import SchemaComparator
from datetime import datetime
import numpy as np

class DataComparator:
    def __init__(self, args):
        self.args = args
        self.report_dir = os.path.dirname(self.args.output)
        
    def normalize_name(self, name: str) -> str:
        name = str(name).split('.')[-1]
        return re.sub(r'[^a-z0-9_]', '', name.lower().strip())

    def normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes data types to reduce false mismatches.
        1. Rounds timestamps to nearest second to ignore millisecond/formatting diffs.
        2. Converts strings numbers to floats to handle '0.00000' vs '0.0'.
        """
        df = df.copy()
        for col in df.columns:
            # --- 1. Handle Datetimes (Rounding to Seconds) ---
            is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
            if not is_datetime and df[col].dtype == object:
                # Try to detect if it's a date string
                try:
                    sample = df[col].dropna()
                    if not sample.empty:
                        # Heuristic: Check first non-null value for date-like characters
                        val = str(sample.iloc[0])
                        if ('-' in val or '/' in val) and ':' in val:
                            df[col] = pd.to_datetime(df[col], errors='ignore')
                            is_datetime = pd.api.types.is_datetime64_any_dtype(df[col])
                except:
                    pass

            if is_datetime:
                # Round to nearest second to handle .653 vs .000 or rounded up diffs
                try:
                    df[col] = df[col].dt.round('1s')
                except:
                    pass
            
            # --- 2. Handle Numerics (Standardizing Precision) ---
            else:
                try:
                    # Convert to numeric (float) to handle "0.0000" vs "0.0" or "10" vs "10.0"
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
                    
        return df

    def get_athena_data(self, table: str, columns: List[str], order_by_cols: List[str], sample_size: int) -> pd.DataFrame:
        try:
            conn = connect(
                region_name=self.args.aws_region,
                s3_staging_dir=self.args.s3_staging,
                schema_name=self.args.athena_db,
                work_group=self.args.athena_workgroup,
                cursor_class=PandasCursor
            )
            qualified_cols = [f'"{col}"' for col in columns]
            col_list = ', '.join(qualified_cols)
            
            order_clause = ', '.join([f'"{col}"' for col in order_by_cols])
            
            query = f"""
                SELECT {col_list} 
                FROM "{self.args.athena_db}"."{table}"
                ORDER BY {order_clause} 
                LIMIT {sample_size}
            """
            return conn.cursor().execute(query).as_pandas()
        except Exception as e:
            logging.error(f"Athena fetch failed for {table}: {e}")
            raise

    def get_sqlserver_data(self, table_str: str, columns: List[str], order_by_cols: List[str], sample_size: int) -> pd.DataFrame:
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
                schema, table = table_str.split('.', 1)
            else:
                schema = 'dbo'
                table = table_str

            with pyodbc.connect(conn_str, timeout=30) as conn:
                quoted_cols = [f'[{col}]' for col in columns]
                col_list = ', '.join(quoted_cols)
                
                quoted_order = [f'[{col}]' for col in order_by_cols]
                order_clause = ', '.join(quoted_order)
                
                query = f"""
                    SELECT TOP {sample_size} {col_list} 
                    FROM [{schema}].[{table}] 
                    ORDER BY {order_clause}
                """
                return pd.read_sql(query, conn)
        except Exception as e:
            logging.error(f"SQL Server fetch failed for {table_str}: {e}")
            raise

    def generate_excel_report(self, df_src: pd.DataFrame, df_tgt: pd.DataFrame, filename: str) -> str:
        try:
            if not os.path.exists(self.report_dir):
                os.makedirs(self.report_dir)
            file_path = os.path.join(self.report_dir, filename)
            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            workbook = writer.book

            # Updated Sheet Names
            sheet_src = 'Aws_Data'
            sheet_tgt = 'Azure_Data'

            df_src.to_excel(writer, sheet_name=sheet_src, index=False)
            df_tgt.to_excel(writer, sheet_name=sheet_tgt, index=False)

            ws_val = workbook.add_worksheet('Validation_Check')
            writer.sheets['Validation_Check'] = ws_val
            
            headers = df_src.columns.tolist()
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
            
            for idx, val in enumerate(headers):
                ws_val.write(0, idx, val, header_fmt)

            green_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            red_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            
            num_rows = len(df_src)
            num_cols = len(headers)
            
            if num_rows > 0 and num_cols > 0:
                last_col = xlsxwriter.utility.xl_col_to_name(num_cols - 1)
                rng = f"A2:{last_col}{num_rows + 1}"
                ws_val.conditional_format(rng, {'type': 'cell', 'criteria': '==', 'value': True, 'format': green_fmt})
                ws_val.conditional_format(rng, {'type': 'cell', 'criteria': '==', 'value': False, 'format': red_fmt})
                
                # Updated Formulas for new sheet names
                for row in range(1, num_rows + 1):
                    for col in range(num_cols):
                        cell_ref = xlsxwriter.utility.xl_rowcol_to_cell(row, col)
                        # Formula: =Aws_Data!A1=Azure_Data!A1
                        ws_val.write_formula(row, col, f'={sheet_src}!{cell_ref}={sheet_tgt}!{cell_ref}')

            writer.close()
            return filename
        except Exception as e:
            logging.error(f"Excel generation failed: {e}")
            raise

    def compare_data(self, mappings: Dict, sample_size: int, callback=None) -> Dict:
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        schema_comparator = SchemaComparator(self.args)
        if callback: callback("Fetching schema metadata for Data Comparison...")
        athena_df_meta = schema_comparator.get_athena_columns()
        sql_targets = [m['sql_table'] for m in mappings.values()]
        sql_df_meta = schema_comparator.get_sqlserver_columns(sql_targets)
        
        for athena_table, config in mappings.items():
            if callback: callback(f"Comparing Data: {athena_table} (Size: {sample_size})...")
            
            sql_table = config['sql_table']
            primary_keys = config.get('primary_keys', [])
            
            table_result = {
                'id': self.normalize_name(athena_table),
                'athena_name': athena_table,
                'sql_name': sql_table,
                'status': 'Pending',
                'has_issues': False,
                'issues': [],
                'excel_report': None,
                'mismatch_count': 0
            }
            
            try:
                norm_athena = self.normalize_name(athena_table)
                norm_sql_table = self.normalize_name(sql_table)
                
                athena_cols = athena_df_meta[athena_df_meta['normalized_table_name'] == norm_athena]
                sql_cols = sql_df_meta[sql_df_meta['normalized_table_name'] == norm_sql_table]
                
                if athena_cols.empty or sql_cols.empty:
                    raise ValueError("Could not fetch schema metadata")

                common_norm_names = sorted(list(set(athena_cols['normalized_name']) & set(sql_cols['normalized_name'])))
                ath_map = dict(zip(athena_cols['normalized_name'], athena_cols['column_name']))
                sql_map = dict(zip(sql_cols['normalized_name'], sql_cols['column_name']))
                
                final_athena_cols = []
                final_sql_cols = []
                
                athena_pks = []
                sql_pks = []

                for pk in primary_keys:
                    norm_pk = self.normalize_name(pk)
                    if norm_pk not in ath_map or norm_pk not in sql_map:
                         raise ValueError(f"Primary Key {pk} not found in both tables")
                    athena_pks.append(ath_map[norm_pk])
                    sql_pks.append(sql_map[norm_pk])
                
                for norm in common_norm_names:
                    final_athena_cols.append(ath_map[norm])
                    final_sql_cols.append(sql_map[norm])
                
                if primary_keys:
                    athena_sort = athena_pks
                    sql_sort = sql_pks
                else:
                    athena_sort = final_athena_cols
                    sql_sort = final_sql_cols

                df_ath = self.get_athena_data(athena_table, final_athena_cols, athena_sort, sample_size)
                df_sql = self.get_sqlserver_data(sql_table, final_sql_cols, sql_sort, sample_size)
                
                # --- 1. Apply Smart Normalization (Floats & Timestamps) ---
                df_ath = self.normalize_df(df_ath)
                df_sql = self.normalize_df(df_sql)

                # --- 2. String Conversion & Null Handling ---
                # Added replacement for empty strings '' alongside other null indicators
                df_ath = df_ath.astype(str).apply(lambda x: x.str.strip().replace(['nan', 'None', '<NA>', 'NaT', ''], 'NULL'))
                df_sql = df_sql.astype(str).apply(lambda x: x.str.strip().replace(['nan', 'None', '<NA>', 'NaT', ''], 'NULL'))

                if not df_ath.empty and not df_sql.empty:
                    if primary_keys:
                        df_ath = df_ath.set_index(athena_pks).sort_index()
                        df_sql = df_sql.set_index(sql_pks).sort_index()
                        df_sql.index.names = df_ath.index.names
                    else:
                        df_ath = df_ath.reset_index(drop=True)
                        df_sql = df_sql.reset_index(drop=True)
                    
                    df_sql.columns = df_ath.columns
                    common_index = df_ath.index.intersection(df_sql.index)
                    df_ath = df_ath.loc[common_index]
                    df_sql = df_sql.loc[common_index]
                
                if df_ath.equals(df_sql):
                    table_result['status'] = 'Match'
                    table_result['has_issues'] = False
                    results['valid_tables'] += 1
                else:
                    table_result['status'] = 'Mismatch'
                    table_result['has_issues'] = True
                    results['error_tables'] += 1
                    try:
                        diff = df_ath.compare(df_sql)
                        table_result['mismatch_count'] = len(diff)
                    except ValueError:
                         table_result['mismatch_count'] = "Structure Mismatch"

                excel_filename = f"validation_{self.normalize_name(athena_table)}_{datetime.now().strftime('%H%M%S')}.xlsx"
                self.generate_excel_report(df_ath.reset_index(), df_sql.reset_index(), excel_filename)
                table_result['excel_report'] = excel_filename
                    
            except Exception as e:
                msg = f"Data Compare Error on {athena_table}: {str(e)}"
                if callback: callback(f"ERROR: {msg}")
                table_result['status'] = 'Error'
                table_result['has_issues'] = True
                table_result['issues'].append(str(e))
                results['error_tables'] += 1
            
            results['tables'].append(table_result)
            
        return results