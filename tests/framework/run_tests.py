#!/usr/bin/env python3
"""
Unified test runner for the data processing framework.

This script provides a clean interface to run tests using the new framework.
It replaces the old test_runner.py and test_pydantic_validation.py files.

Usage:
    python3 tests/framework/run_tests.py [CATEGORY] [OPTIONS]
    ./run.sh test [CATEGORY] [OPTIONS]
"""

import sys
import argparse
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.framework import TestFrameworkRunner, TestFrameworkConfig, TestFrameworkDiscovery, TestFrameworkReporter


def main():
    """Main function for command-line interface."""
    parser = argparse.ArgumentParser(description='Data Processing Framework Test Runner')
    parser.add_argument('category', nargs='?', default='all', 
                       help='Test category to run (default: all)')
    parser.add_argument('--list', action='store_true', 
                       help='List available test categories')
    parser.add_argument('--no-coverage', action='store_true', 
                       help='Disable coverage analysis')
    parser.add_argument('--quiet', action='store_true', 
                       help='Quiet mode (less verbose output)')
    parser.add_argument('--create', nargs=2, metavar=('CATEGORY', 'NAME'),
                       help='Create a test template for the specified category and name')
    
    args = parser.parse_args()
    
    # Initialize framework components
    config = TestFrameworkConfig(project_root)
    discovery = TestFrameworkDiscovery(config)
    reporter = TestFrameworkReporter(config.results_dir)
    runner = TestFrameworkRunner(project_root)
    
    if args.list:
        reporter.list_categories(config.categories, discovery)
        return 0
    
    if args.create:
        category, name = args.create
        runner.create_test_template(category, name)
        return 0
    
    # Run tests
    category = args.category
    verbose = not args.quiet
    coverage = not args.no_coverage
    
    if category not in config.get_available_categories():
        print(f"Error: Unknown category '{category}'")
        print("Use --list to see available categories")
        return 1
    
    if category == 'all':
        summary = runner.run_all_tests(verbose, coverage)
    else:
        summary = runner.run_tests(category, verbose, coverage)
    
    # Print and save results
    reporter.print_summary(summary)
    results_file = reporter.save_results(summary)
    print(f"\nTest results saved to: {results_file}")
    
    # Return appropriate exit code
    if summary['summary']['tests_failed'] > 0:
        return 1
    else:
        return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
