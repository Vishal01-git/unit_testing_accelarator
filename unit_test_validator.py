# unit_test_validator.py
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

        # V2.1: Add Authentication params to args
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

    def run(self):
        try:
            results = {'total_tables': len(self.config['mappings']), 'tests': {}}

            if 'schema' in self.selected_tests:
                if self.verbose: logging.info("Running schema comparison...")
                results['tests']['schema'] = SchemaComparator(self.args).compare_schemas(self.config['mappings'])

            if 'count' in self.selected_tests:
                if self.verbose: logging.info("Running count check...")
                results['tests']['count'] = CountChecker(self.args).check_counts(self.config['mappings'])

            if 'duplicates' in self.selected_tests:
                if self.verbose: logging.info("Running duplicate check...")
                results['tests']['duplicates'] = DuplicateChecker(self.args).check_duplicates(self.config['mappings'])

            if 'nulls' in self.selected_tests:
                if self.verbose: logging.info("Running null check...")
                results['tests']['nulls'] = NullChecker(self.args).check_nulls(self.config['mappings'])

            if 'data' in self.selected_tests:
                if self.verbose: logging.info(f"Running data comparison with sample size {self.sample_size}...")
                results['tests']['data'] = DataComparator(self.args).compare_data(self.config['mappings'], self.sample_size)

            if self.verbose: logging.info("Generating HTML report...")
            ReportGenerator().generate(results, self.output_path)
            
            return True, f"Validation complete! Report saved to {self.output_path}"
            
        except Exception as e:
            logging.error(f"Fatal error during validation: {str(e)}", exc_info=True)
            return False, str(e)