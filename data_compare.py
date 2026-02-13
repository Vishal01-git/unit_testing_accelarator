#!/usr/bin/env python3
"""
Data Comparison Module V3.2 (Auto-Sort Support)
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
import pyodbc
import logging
import re
import os
import xlsxwriter
from typing import Dict, List, Optional
from schema_compare import SchemaComparator
from datetime import datetime

class DataComparator:
    def __init__(self, args):
        self.args = args
        self.report_dir = os.path.dirname(self.args.output)
        
    def normalize_name(self, name: str) -> str:
        """Standardize names"""
        name = str(name).split('.')[-1]
        return re.sub(r'[^a-z0-9_]', '', name.lower().strip())

    def get_athena_data(self, table: str, columns: List[str], order_by_cols: List[str], sample_size: int) -> pd.DataFrame:
        """Fetch sample data from Athena with dynamic ordering"""
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
            
            # Use provided columns for sorting
            order_clause = ', '.join([f'"{col}"' for col in order_by_cols])
            
            query = f"""
                SELECT {col_list} 
                FROM "{self.args.athena_db}"."{table}"
                ORDER BY {order_clause} 
                LIMIT {sample_size}
            """
            
            df = conn.cursor().execute(query).as_pandas()
            return df
        except Exception as e:
            logging.error(f"Athena fetch failed for {table}: {e}")
            raise

    def get_sqlserver_data(self, table_str: str, columns: List[str], order_by_cols: List[str], sample_size: int) -> pd.DataFrame:
        """Fetch sample data from SQL Server with dynamic ordering"""
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
        #     mssql_username = "admin-airliquide-sas-big-prod-sql-apac-001"
        #     mssql_password = "QAXwmFTaa35S94Y9"
        #     conn_str = (
        #         "Driver={ODBC Driver 17 for SQL Server};"
        #         f"Server={self.args.mssql_server};"
        #         f"Database={self.args.mssql_db};"
        #         f"UID={mssql_username};"
        #         f"PWD={mssql_password};"
        #         "Encrypt=yes;"
        #     )
            
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
                
                df = pd.read_sql(query, conn)
                return df
        except Exception as e:
            logging.error(f"SQL Server fetch failed for {table_str}: {e}")
            raise

    def generate_excel_report(self, df_src: pd.DataFrame, df_tgt: pd.DataFrame, filename: str) -> str:
        """Generates the Excel report"""
        try:
            if not os.path.exists(self.report_dir):
                os.makedirs(self.report_dir)
                
            file_path = os.path.join(self.report_dir, filename)
            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            workbook = writer.book

            df_src.to_excel(writer, sheet_name='Source_Data', index=False)
            df_tgt.to_excel(writer, sheet_name='Target_Data', index=False)

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
            
            for row in range(1, num_rows + 1):
                for col in range(num_cols):
                    cell_ref = xlsxwriter.utility.xl_rowcol_to_cell(row, col)
                    ws_val.write_formula(row, col, f'=Source_Data!{cell_ref}=Target_Data!{cell_ref}')

            if num_rows > 0 and num_cols > 0:
                last_col = xlsxwriter.utility.xl_col_to_name(num_cols - 1)
                rng = f"A2:{last_col}{num_rows + 1}"
                ws_val.conditional_format(rng, {'type': 'cell', 'criteria': '==', 'value': True, 'format': green_fmt})
                ws_val.conditional_format(rng, {'type': 'cell', 'criteria': '==', 'value': False, 'format': red_fmt})

            writer.close()
            return filename
        except Exception as e:
            logging.error(f"Excel generation failed: {e}")
            raise

    def compare_data(self, mappings: Dict, sample_size: int = 1000) -> Dict:
        """Main comparison flow"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(mappings),
            'valid_tables': 0,
            'error_tables': 0,
            'tables': []
        }
        
        schema_comparator = SchemaComparator(self.args)
        athena_df_meta = schema_comparator.get_athena_columns()
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
                'has_issues': False,
                'issues': [],
                'excel_report': None,
                'mismatch_count': 0
            }
            
            try:
                # --- 1. Column Resolution ---
                norm_athena = self.normalize_name(athena_table)
                norm_sql_table = self.normalize_name(sql_table)
                
                athena_cols = athena_df_meta[athena_df_meta['normalized_table_name'] == norm_athena]
                sql_cols = sql_df_meta[sql_df_meta['normalized_table_name'] == norm_sql_table]
                
                if athena_cols.empty or sql_cols.empty:
                    raise ValueError("Could not fetch schema metadata")

                # Find common normalized names
                common_norm_names = sorted(list(set(athena_cols['normalized_name']) & set(sql_cols['normalized_name'])))
                
                ath_map = dict(zip(athena_cols['normalized_name'], athena_cols['column_name']))
                sql_map = dict(zip(sql_cols['normalized_name'], sql_cols['column_name']))
                
                final_athena_cols = []
                final_sql_cols = []
                
                # If PKs provided, check them
                for pk in primary_keys:
                    norm_pk = self.normalize_name(pk)
                    if norm_pk not in ath_map or norm_pk not in sql_map:
                         raise ValueError(f"Primary Key {pk} not found in both tables")
                
                # Build column lists
                for norm in common_norm_names:
                    final_athena_cols.append(ath_map[norm])
                    final_sql_cols.append(sql_map[norm])
                
                # --- 2. Determine Sort Columns ---
                if primary_keys:
                    # Sort by defined PKs
                    athena_sort = primary_keys
                    sql_sort = primary_keys
                else:
                    # No PK? Sort by ALL common columns
                    athena_sort = final_athena_cols
                    sql_sort = final_sql_cols

                # --- 3. Fetch Data ---
                df_ath = self.get_athena_data(athena_table, final_athena_cols, athena_sort, sample_size)
                df_sql = self.get_sqlserver_data(sql_table, final_sql_cols, sql_sort, sample_size)
                
                # --- 4. Align Data ---
                # Normalize values
                df_ath = df_ath.astype(str).apply(lambda x: x.str.strip().replace(['nan', 'None', '<NA>'], 'NULL'))
                df_sql = df_sql.astype(str).apply(lambda x: x.str.strip().replace(['nan', 'None', '<NA>'], 'NULL'))

                if not df_ath.empty and not df_sql.empty:
                    if primary_keys:
                        # If PKs exist, align on them
                        df_ath = df_ath.set_index(primary_keys).sort_index()
                        df_sql = df_sql.set_index(primary_keys).sort_index()
                    else:
                        # No PKs: Trust the SQL 'ORDER BY' and reset index to 0..N for row-by-row comparison
                        df_ath = df_ath.reset_index(drop=True)
                        df_sql = df_sql.reset_index(drop=True)
                    
                    # Ensure columns match for report
                    df_sql.columns = df_ath.columns
                    
                    # Align rows
                    common_index = df_ath.index.intersection(df_sql.index)
                    df_ath = df_ath.loc[common_index]
                    df_sql = df_sql.loc[common_index]
                
                # --- 5. Comparison ---
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

                # --- 6. Generate Excel Report ---
                excel_filename = f"validation_{self.normalize_name(athena_table)}_{datetime.now().strftime('%H%M%S')}.xlsx"
                
                self.generate_excel_report(
                    df_ath.reset_index(), 
                    df_sql.reset_index(), 
                    excel_filename
                )
                
                table_result['excel_report'] = excel_filename
                    
            except Exception as e:
                table_result['status'] = 'Error'
                table_result['has_issues'] = True
                table_result['issues'].append(str(e))
                results['error_tables'] += 1
            
            results['tables'].append(table_result)
            
        return results