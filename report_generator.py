import os
from pathlib import Path
from datetime import datetime
import dominate
from dominate.tags import div, h1, h2, h3, h4, p, span, strong, ul, li, table, thead, tbody, tr, th, td, meta, link, style, script, details, summary, a, i
import logging

class ReportGenerator:
    def __init__(self):
        self.template_dir = Path(__file__).parent / "templates"
        self._load_templates()
    
    def _load_templates(self):
        """Load CSS and JS from external files"""
        try:
            self.css = (self.template_dir / "report.css").read_text(encoding="utf-8")
            self.js = (self.template_dir / "report.js").read_text(encoding="utf-8")
            logging.info("Loaded CSS and JS templates")
        except Exception as e:
            logging.error(f"Failed to load templates: {str(e)}")
            raise
    
    def generate(self, results, output_path):
        """Generate the HTML report"""
        if not isinstance(results, dict) or 'tests' not in results:
            logging.error("Invalid results structure: 'tests' key missing or results is not a dictionary")
            raise ValueError("Invalid results structure")
        
        logging.info("Generating HTML report")
        doc = dominate.document(title='Unit Testing Validation Report')
        
        with doc.head:
            meta(charset="utf-8")
            meta(name="viewport", content="width=device-width, initial-scale=1")
            link(rel="stylesheet", href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css")
            style(self.css)
        
        with doc:
            self._build_header(doc)
            self._build_summary(doc, results)
            self._build_results(doc, results)
            script(self.js)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(doc.render())
        logging.info(f"Report saved to {output_path}")
    
    def _build_header(self, doc):
        with doc:
            with div(cls="header"):
                h1("Unit Testing Validation Report")
                p(f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    def _build_summary(self, doc, results):
        with doc:
            with div(cls="summary-card"):
                h2("Validation Summary")
                with div(cls="summary-stats"):
                    span(f"Tables Tested: {results['total_tables']}", cls="badge")
                    for test_type, test_results in results['tests'].items():
                        test_name = test_type.capitalize()
                        span(f"{test_name} Valid: {test_results['valid_tables']}", cls="badge badge-success")
                        span(f"{test_name} Issues: {test_results['error_tables']}", 
                             cls="badge badge-danger" if test_results['error_tables'] else "badge")
    
    def _build_results(self, doc, results):
        with doc:
            h2("Detailed Validation Results")
            for test_type, test_results in results['tests'].items():
                with div(cls="test-section"):
                    h3(f"{test_type.capitalize()} Test Results")
                    for table_result in test_results['tables']:
                        with div(cls=f"table-card {'error' if table_result['has_issues'] or table_result.get('status') == 'Mismatch' else 'match'}"):
                            with details():
                                with summary(cls="table-header"):
                                    strong(f"{table_result['athena_name']} ↔ {table_result['sql_name']}")
                                    if table_result['has_issues']:
                                        span(f"{len(table_result['issues'])} issues", cls="badge badge-danger")
                                    elif table_result.get('status') == 'Mismatch':
                                        span(f"Mismatch Found", cls="badge badge-danger")
                                    span("▶", cls="toggle-icon")
                                
                                with div(cls="table-details"):
                                    if table_result['has_issues']:
                                        with div(cls="issue-section"):
                                            h4("Validation Issues")
                                            with ul():
                                                for issue in table_result['issues']:
                                                    li(issue)
                                    
                                    if test_type == 'schema':
                                        self._build_schema_results(table_result)
                                    elif test_type == 'count':
                                        self._build_count_results(table_result)
                                    elif test_type == 'duplicates':
                                        self._build_duplicate_results(table_result)
                                    elif test_type == 'nulls':
                                        self._build_null_results(table_result)
                                    elif test_type == 'data':
                                        self._build_data_results(table_result)
    
    def _build_schema_results(self, table_result):
        with div(cls="schema-section"):
            h4("Column Comparison")
            with table(cls="schema-table"):
                with thead():
                    with tr():
                        th("Column (Athena)")
                        th("Column (SQL Server)")
                        th("Athena Type")
                        th("SQL Server Type")
                        th("Status")
                with tbody():
                    for col in table_result['columns']:
                        with tr(cls=col['status_class']):
                            td(col['athena_column'])
                            td(col['sql_column'])
                            td(col['athena_type'])
                            td(col['sql_type'])
                            td(col['status'])

    def _build_count_results(self, table_result):
        with div(cls="count-section"):
            h4("Row Count Comparison")
            with table(cls="schema-table"):
                with thead():
                    with tr():
                        th("Athena Count")
                        th("SQL Server Count")
                        th("Status")
                with tbody():
                    with tr(cls=table_result['counts']['status_class']):
                        td(str(table_result['counts']['athena_count']))
                        td(str(table_result['counts']['sql_count']))
                        td(table_result['counts']['status'])

    def _build_duplicate_results(self, table_result):
        with div(cls="duplicate-section"):
            h4("Duplicate Check")
            with table(cls="schema-table"):
                with thead():
                    with tr():
                        th("Database")
                        th("Duplicate Count")
                        th("Details")
                with tbody():
                    with tr(cls=table_result['duplicates']['status_class']):
                        td("Athena")
                        td(str(len(table_result['duplicates']['athena_duplicates'])))
                        td(", ".join([f"{d['cnt']} rows for {d}" for d in table_result['duplicates']['athena_duplicates']]) or "None")
                    with tr(cls=table_result['duplicates']['status_class']):
                        td("SQL Server")
                        td(str(len(table_result['duplicates']['sql_duplicates'])))
                        td(", ".join([f"{d['cnt']} rows for {d}" for d in table_result['duplicates']['sql_duplicates']]) or "None")

    def _build_null_results(self, table_result):
        with div(cls="null-section"):
            h4("Null Check")
            with table(cls="schema-table"):
                with thead():
                    with tr():
                        th("Database")
                        th("Column")
                        th("Null Count")
                with tbody():
                    for key, count in table_result['nulls']['athena_nulls'].items():
                        with tr(cls='error' if count > 0 else 'match'):
                            td("Athena")
                            td(key)
                            td(str(count))
                    for key, count in table_result['nulls']['sql_nulls'].items():
                        with tr(cls='error' if count > 0 else 'match'):
                            td("SQL Server")
                            td(key)
                            td(str(count))

    def _build_data_results(self, table_result):
        with div(cls="data-section"):
            h4("Sample Data Comparison")
            
            # Display Status and Mismatch Count
            with div(style="margin-bottom: 15px;"):
                if table_result['status'] == 'Match':
                    span("Status: MATCH", cls="badge badge-success", style="font-size: 14px; margin-right: 10px;")
                else:
                    span("Status: MISMATCH", cls="badge badge-danger", style="font-size: 14px; margin-right: 10px;")
                    span(f"Mismatched Rows: {table_result['mismatch_count']}", style="font-weight: bold; color: #dc3545;")

            # Download Link for Excel
            if table_result.get('excel_report'):
                # Assuming app.py serves /reports/<filename>
                report_url = f"/reports/{table_result['excel_report']}"
                with a(href=report_url, cls="report-btn download", style="background-color: #28a745; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; display: inline-block;"):
                    i(cls="fa fa-file-excel", style="margin-right: 8px;")
                    span("Download Validation Excel")
            else:
                p("No Excel report generated.")