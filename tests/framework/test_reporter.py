"""
Test reporting utilities for the data processing framework.
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List


class TestFrameworkReporter:
    """Handles test result reporting and output formatting."""
    
    def __init__(self, results_dir: Path):
        self.results_dir = results_dir
        self.results_dir.mkdir(exist_ok=True)
    
    def format_test_results(self, summary: Dict[str, Any]) -> str:
        """Format test results for console output."""
        category = summary['category']
        test_files = summary['test_files']
        results = summary['results']
        stats = summary['summary']
        
        output = []
        output.append("\n" + "="*80)
        output.append(f"TEST RESULTS - {category.upper()}")
        output.append("="*80)
        
        output.append(f"Category: {category}")
        output.append(f"Test Files: {len(test_files)}")
        output.append(f"Tests Run: {stats['tests_run']}")
        output.append(f"Passed: {stats['tests_passed']}")
        output.append(f"Failed: {stats['tests_failed']}")
        output.append(f"Skipped: {stats['tests_skipped']}")
        coverage = stats.get('coverage_percentage', 0)
        output.append(f"Coverage: {coverage:.2f}%")
        output.append(f"Duration: {stats['duration']:.2f}s")
        
        output.append(f"\nTest Files:")
        for test_file in test_files:
            output.append(f"  - {test_file}")
        
        if results.get('returncode') != 0:
            output.append(f"\nErrors:")
            if results.get('stderr'):
                output.append(f"  {results['stderr'][:500]}...")
        
        output.append("="*80)
        return "\n".join(output)
    
    def save_results(self, summary: Dict[str, Any]) -> Path:
        """Save test results to file."""
        timestamp = datetime.now().isoformat()
        summary['timestamp'] = timestamp
        
        # Save category-specific results
        category = summary['category']
        results_file = self.results_dir / f'test_summary_{category}.json'
        with open(results_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Save overall results
        overall_file = self.results_dir / 'test_summary_overall.json'
        if overall_file.exists():
            with open(overall_file, 'r') as f:
                overall_data = json.load(f)
        else:
            overall_data = {'categories': {}}
        
        overall_data['categories'][category] = summary
        overall_data['last_updated'] = timestamp
        
        with open(overall_file, 'w') as f:
            json.dump(overall_data, f, indent=2)
        
        return results_file
    
    def generate_coverage_report(self, category: str, coverage_data: Dict[str, Any]) -> str:
        """Generate a coverage report summary."""
        if not coverage_data:
            return "No coverage data available"
        
        totals = coverage_data.get('totals', {})
        percent_covered = totals.get('percent_covered', 0)
        total_lines = totals.get('num_statements', 0)
        covered_lines = totals.get('covered_lines', 0)
        missing_lines = totals.get('missing_lines', 0)
        
        report = []
        report.append(f"Coverage Report for {category}:")
        report.append(f"  Total Lines: {total_lines}")
        report.append(f"  Covered Lines: {covered_lines}")
        report.append(f"  Missing Lines: {missing_lines}")
        report.append(f"  Coverage: {percent_covered:.2f}%")
        
        return "\n".join(report)
    
    def print_summary(self, summary: Dict[str, Any]):
        """Print formatted test results to console."""
        print(self.format_test_results(summary))
    
    def list_categories(self, categories: Dict[str, Any], discovery):
        """List all available test categories with file counts."""
        print("Available Test Categories:")
        print("="*50)
        
        for category_name, category_info in categories.items():
            print(f"\n{category_name}:")
            print(f"  Name: {category_info.name}")
            print(f"  Description: {category_info.description}")
            print(f"  Type: {category_info.test_type}")
            
            # Get actual test files
            test_files = discovery.discover_test_files(category_name)
            print(f"  Test Files: {len(test_files)}")
            
            if test_files:
                for test_file in test_files:
                    print(f"    - {test_file}")
            else:
                print("    - No test files found")
