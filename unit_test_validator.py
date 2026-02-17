import logging
from schema_compare import SchemaComparator
from count_check import CountChecker
from duplicate_check import DuplicateChecker
from null_check import NullChecker
from data_compare import DataComparator
from report_generator import ReportGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Validator:
    def __init__(self, config, output_path, tests='all', sample_size=100, verbose=False):
        self.config = config
        self.output_path = output_path
        self.sample_size = sample_size
        self.verbose = verbose
        
        valid_tests = {'schema', 'count', 'duplicates', 'nulls', 'data', 'all'}
        self.selected_tests = tests.lower().split(',')
        if 'all' in self.selected_tests:
            self.selected_tests = ['schema', 'count', 'duplicates', 'nulls', 'data']
        
        if not all(t in valid_tests for t in self.selected_tests):
            raise ValueError(f"Invalid test specified. Choose from: {', '.join(valid_tests)}")

        self.args = type('Args', (), {
            'aws_region': config.get('aws-region'),
            's3_staging': config.get('s3-staging'),
            'athena_db': config.get('athena-db'),
            'athena_workgroup': config.get('athena-workgroup', 'primary'),
            'mssql_server': config.get('mssql-server'),
            'mssql_db': config.get('mssql-db'),
            'mssql_driver': config.get('mssql-driver'),
            'auth_method': config.get('auth-method'),
            'mssql_user': config.get('mssql-user'),
            'mssql_password': config.get('mssql-password'),
            'output': output_path
        })()

    def run(self, progress_callback=None):
        """
        Runs the validation suite.
        progress_callback: A function that accepts a string message for real-time logging.
        """
        def log(msg):
            if self.verbose: logging.info(msg)
            if progress_callback: progress_callback(msg)

        try:
            results = {'total_tables': len(self.config['mappings']), 'tests': {}}
            log(f"Starting validation for {len(self.config['mappings'])} tables...")

            if 'schema' in self.selected_tests:
                log("--- Schema Comparison ---")
                results['tests']['schema'] = SchemaComparator(self.args).compare_schemas(self.config['mappings'], callback=progress_callback)

            if 'count' in self.selected_tests:
                log("--- Row Count Check ---")
                results['tests']['count'] = CountChecker(self.args).check_counts(self.config['mappings'], callback=progress_callback)

            if 'duplicates' in self.selected_tests:
                log("--- Duplicate Check ---")
                results['tests']['duplicates'] = DuplicateChecker(self.args).check_duplicates(self.config['mappings'], callback=progress_callback)

            if 'nulls' in self.selected_tests:
                log("--- Null Check ---")
                results['tests']['nulls'] = NullChecker(self.args).check_nulls(self.config['mappings'], callback=progress_callback)

            if 'data' in self.selected_tests:
                log(f"--- Data Comparison (Sample: {self.sample_size}) ---")
                results['tests']['data'] = DataComparator(self.args).compare_data(self.config['mappings'], self.sample_size, callback=progress_callback)

            log("Generating HTML Report...")
            ReportGenerator().generate(results, self.output_path)
            log(f"Report generated successfully.")
            
            return True, f"Validation complete!"
            
        except Exception as e:
            error_msg = f"Fatal error during validation: {str(e)}"
            logging.error(error_msg, exc_info=True)
            log(error_msg)
            return False, str(e)