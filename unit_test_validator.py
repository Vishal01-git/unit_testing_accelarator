#!/usr/bin/env python3
"""
Unit Testing Validator V2.0 - Core Orchestrator
"""

import logging
import sys
import os
from schema_compare import SchemaComparator
from count_check import CountChecker
from duplicate_check import DuplicateChecker
from null_check import NullChecker
from data_compare import DataComparator
from report_generator import ReportGenerator

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Validator:
    def __init__(self, config, output_path, tests='all', sample_size=100, verbose=False):
        """
        Initialize Validator V2.0
        
        Args:
            config (dict): Configuration dictionary containing connection details and mappings.
            output_path (str): Path to save the HTML report.
            tests (str): Comma-separated list of tests to run (or 'all').
            sample_size (int): Number of rows to sample for data comparison.
            verbose (bool): Enable verbose logging.
        """
        self.config = config
        self.output_path = output_path
        self.sample_size = sample_size
        self.verbose = verbose
        
        # Parse and validate tests
        valid_tests = {'schema', 'count', 'duplicates', 'nulls', 'data', 'all'}
        self.selected_tests = tests.lower().split(',')
        if 'all' in self.selected_tests:
            self.selected_tests = ['schema', 'count', 'duplicates', 'nulls', 'data']
        
        if not all(t in valid_tests for t in self.selected_tests):
            raise ValueError(f"Invalid test specified. Choose from: {', '.join(valid_tests)}")

        # Construct 'args' object for sub-modules
        # Note: 'mssql_schema' is explicitly REMOVED in V2.0
        self.args = type('Args', (), {
            'aws_region': config.get('aws-region'),
            's3_staging': config.get('s3-staging'),
            'athena_db': config.get('athena-db'),
            'athena_workgroup': config.get('athena-workgroup', 'primary'),
            'mssql_server': config.get('mssql-server'),
            'mssql_db': config.get('mssql-db'),
            'mssql_driver': config.get('mssql-driver', 'ODBC Driver 17 for SQL Server'),
            'mssql_user': config.get('mssql-user'),      # Optional: for SQL Auth
            'mssql_password': config.get('mssql-password'), # Optional: for SQL Auth
            'output': output_path
        })()

    def run(self):
        """Execute the validation suite"""
        try:
            results = {
                'total_tables': len(self.config['mappings']),
                'tests': {}
            }

            # 1. Schema Comparison
            if 'schema' in self.selected_tests:
                if self.verbose: logging.info("Running schema comparison...")
                comparator = SchemaComparator(self.args)
                results['tests']['schema'] = comparator.compare_schemas(self.config['mappings'])

            # 2. Row Count Check
            if 'count' in self.selected_tests:
                if self.verbose: logging.info("Running count check...")
                checker = CountChecker(self.args)
                results['tests']['count'] = checker.check_counts(self.config['mappings'])

            # 3. Duplicate Check
            if 'duplicates' in self.selected_tests:
                if self.verbose: logging.info("Running duplicate check...")
                checker = DuplicateChecker(self.args)
                results['tests']['duplicates'] = checker.check_duplicates(self.config['mappings'])

            # 4. Null Check
            if 'nulls' in self.selected_tests:
                if self.verbose: logging.info("Running null check...")
                checker = NullChecker(self.args)
                results['tests']['nulls'] = checker.check_nulls(self.config['mappings'])

            # 5. Data Comparison
            if 'data' in self.selected_tests:
                if self.verbose: logging.info("Running data comparison...")
                comparator = DataComparator(self.args)
                results['tests']['data'] = comparator.compare_data(self.config['mappings'], self.sample_size)

            # Generate Report
            if self.verbose: logging.info("Generating HTML report...")
            reporter = ReportGenerator()
            reporter.generate(results, self.output_path)
            
            return True, f"Validation complete! Report saved to {self.output_path}"
            
        except Exception as e:
            logging.error(f"Fatal error during validation: {str(e)}", exc_info=True)
            return False, str(e)