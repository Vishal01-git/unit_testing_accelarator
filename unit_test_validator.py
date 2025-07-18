#!/usr/bin/env python3
"""
Unit Testing Validator for Athena and SQL Server
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from schema_compare import SchemaComparator
from count_check import CountChecker
from duplicate_check import DuplicateChecker
from null_check import NullChecker
from data_compare import DataComparator
from report_generator import ReportGenerator

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_file: str) -> dict:
    """Load and validate configuration"""
    try:
        with open(config_file) as f:
            config = json.load(f)
        if not isinstance(config, dict) or 'mappings' not in config:
            raise ValueError("Config file must be a dictionary with a 'mappings' key")
        logging.info(f"Loaded {len(config['mappings'])} table mappings")
        return config
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description='Unit testing validator for Athena and SQL Server',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Athena parameters
    parser.add_argument('--aws-region', required=True, help='AWS region name')
    parser.add_argument('--s3-staging', required=True, help='S3 staging directory')
    parser.add_argument('--athena-db', required=True, help='Athena database name')
    parser.add_argument('--athena-workgroup', default='primary', help='Athena workgroup name')
    
    # SQL Server parameters
    parser.add_argument('--mssql-server', required=True, help='SQL Server hostname')
    parser.add_argument('--mssql-db', required=True, help='SQL Server database name')
    parser.add_argument('--mssql-schema', default='dbo', help='SQL Server schema name')
    
    # Configuration
    parser.add_argument('--config-file', required=True, help='JSON config file with table mappings and primary keys')
    parser.add_argument('--output', required=True, help='Output HTML file path')
    parser.add_argument('--tests', default='all', help='Comma-separated tests to run: schema,count,duplicates,nulls,data')
    parser.add_argument('--sample-size', type=int, default=100, help='Number of rows for data comparison')
    parser.add_argument('--verbose', action='store_true', help='Show detailed progress')
    
    args = parser.parse_args()

    # Validate arguments
    if not args.output.endswith('.html'):
        logging.error("Output file must have .html extension")
        sys.exit(1)

    valid_tests = {'schema', 'count', 'duplicates', 'nulls', 'data', 'all'}
    selected_tests = args.tests.lower().split(',')
    if 'all' in selected_tests:
        selected_tests = ['schema', 'count', 'duplicates', 'nulls', 'data']
    if not all(t in valid_tests for t in selected_tests):
        logging.error(f"Invalid test specified. Choose from: {', '.join(valid_tests)}")
        sys.exit(1)

    try:
        # Load config
        if args.verbose:
            logging.info("Loading configuration...")
        config = load_config(args.config_file)
        
        # Initialize results
        results = {
            'total_tables': len(config['mappings']),
            'tests': {}
        }

        # Run selected tests
        if 'schema' in selected_tests:
            if args.verbose:
                logging.info("Running schema comparison...")
            comparator = SchemaComparator(args)
            results['tests']['schema'] = comparator.compare_schemas(config['mappings'])

        if 'count' in selected_tests:
            if args.verbose:
                logging.info("Running count check...")
            checker = CountChecker(args)
            results['tests']['count'] = checker.check_counts(config['mappings'])

        if 'duplicates' in selected_tests:
            if args.verbose:
                logging.info("Running duplicate check...")
            checker = DuplicateChecker(args)
            results['tests']['duplicates'] = checker.check_duplicates(config['mappings'])

        if 'nulls' in selected_tests:
            if args.verbose:
                logging.info("Running null check...")
            checker = NullChecker(args)
            results['tests']['nulls'] = checker.check_nulls(config['mappings'])

        if 'data' in selected_tests:
            if args.verbose:
                logging.info("Running data comparison...")
            comparator = DataComparator(args)
            results['tests']['data'] = comparator.compare_data(config['mappings'], args.sample_size)

        # Generate report
        if args.verbose:
            logging.info("Generating report...")
        reporter = ReportGenerator()
        reporter.generate(results, args.output)
        
        logging.info(f"Validation complete! Report saved to {args.output}")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()