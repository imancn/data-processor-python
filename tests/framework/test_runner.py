"""
Unified test runner for the data processing framework.
"""

import subprocess
import json
import time
from pathlib import Path
from typing import Dict, Any, List

from .test_config import TestFrameworkConfig
from .test_discovery import TestFrameworkDiscovery
from .test_reporter import TestFrameworkReporter


class TestFrameworkRunner:
    """Unified test runner for all test categories."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.config = TestFrameworkConfig(project_root)
        self.discovery = TestFrameworkDiscovery(self.config)
        self.reporter = TestFrameworkReporter(self.config.results_dir)
    
    def run_tests(self, category: str, verbose: bool = True, coverage: bool = True) -> Dict[str, Any]:
        """Run tests for a specific category."""
        print(f"Running {category} tests...")
        
        # Discover test files
        test_files = self.discovery.discover_test_files(category)
        
        if not test_files:
            print(f"No test files found for category: {category}")
            return self._create_empty_summary(category)
        
        print(f"Found {len(test_files)} test files:")
        for test_file in test_files:
            print(f"  - {test_file}")
        
        # Run tests
        results = self._run_pytest_tests(test_files, category, verbose, coverage)
        
        # Get coverage information
        coverage_percentage = self._get_coverage_percentage(category)
        
        # Create summary
        summary = {
            'category': category,
            'test_files': test_files,
            'results': results,
            'summary': {
                'tests_run': results.get('tests_run', 0),
                'tests_passed': results.get('tests_passed', 0),
                'tests_failed': results.get('tests_failed', 0),
                'tests_skipped': results.get('tests_skipped', 0),
                'duration': results.get('duration', 0),
                'coverage_percentage': coverage_percentage
            }
        }
        
        return summary
    
    def _run_pytest_tests(self, test_files: List[str], category: str, verbose: bool = True, 
                         coverage: bool = True) -> Dict[str, Any]:
        """Run pytest tests for a specific category."""
        if not test_files:
            return {
                'returncode': 0,
                'stdout': 'No test files found',
                'stderr': '',
                'duration': 0,
                'command': '',
                'tests_run': 0,
                'tests_passed': 0,
                'tests_failed': 0,
                'tests_skipped': 0
            }
        
        cmd = ['python3', '-m', 'pytest']
        
        if verbose:
            cmd.append('-v')
        
        cmd.extend([
            '--tb=short',
            '--strict-markers',
            '--disable-warnings'
        ])
        
        # Add JSON report if pytest-json-report is available
        try:
            import pytest_jsonreport
            cmd.extend([
                '--json-report',
                f'--json-report-file={self.config.results_dir}/test_results_{category}.json'
            ])
        except ImportError:
            print("Warning: pytest-json-report not available, skipping JSON report")
        
        # Add coverage if requested
        if coverage:
            category_info = self.config.get_category(category)
            if category_info and category_info.coverage_paths:
                cmd.extend(['--cov=' + ','.join(category_info.coverage_paths)])
                cmd.extend([
                    f'--cov-report=html:{self.config.results_dir}/coverage_html_{category}',
                    f'--cov-report=json:{self.config.results_dir}/coverage_{category}.json',
                    '--cov-report=term-missing'
                ])
        
        cmd.extend(test_files)
        
        print(f"Running command: {' '.join(cmd)}")
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        # Parse test results from JSON report or stdout
        json_file = self.config.results_dir / f'test_results_{category}.json'
        test_stats = {'tests_run': 0, 'tests_passed': 0, 'tests_failed': 0, 'tests_skipped': 0}
        
        if json_file.exists():
            try:
                with open(json_file, 'r') as f:
                    json_data = json.load(f)
                summary = json_data.get('summary', {})
                test_stats = {
                    'tests_run': summary.get('total', 0),
                    'tests_passed': summary.get('passed', 0),
                    'tests_failed': summary.get('failed', 0),
                    'tests_skipped': summary.get('skipped', 0)
                }
            except Exception as e:
                print(f"Warning: Could not parse test results JSON: {e}")
        
        # If no JSON report, try to parse from stdout
        if test_stats['tests_run'] == 0 and result.stdout:
            try:
                # Look for pytest summary line like "11 passed, 9 warnings in 0.03s"
                lines = result.stdout.strip().split('\n')
                for line in reversed(lines):
                    if 'passed' in line and ('in ' in line or 'warnings' in line):
                        # Parse line like "11 passed, 9 warnings in 0.03s" or "5 passed in 2.77s"
                        import re
                        # Extract numbers before 'passed', 'failed', 'skipped'
                        passed_match = re.search(r'(\d+)\s+passed', line)
                        failed_match = re.search(r'(\d+)\s+failed', line)
                        skipped_match = re.search(r'(\d+)\s+skipped', line)
                        
                        if passed_match:
                            test_stats['tests_passed'] = int(passed_match.group(1))
                        if failed_match:
                            test_stats['tests_failed'] = int(failed_match.group(1))
                        if skipped_match:
                            test_stats['tests_skipped'] = int(skipped_match.group(1))
                        
                        test_stats['tests_run'] = test_stats['tests_passed'] + test_stats['tests_failed'] + test_stats['tests_skipped']
                        break
            except Exception as e:
                print(f"Warning: Could not parse test results from stdout: {e}")
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'duration': end_time - start_time,
            'command': ' '.join(cmd),
            **test_stats
        }
    
    def _get_coverage_percentage(self, category: str) -> float:
        """Get coverage percentage for a category."""
        coverage_file = self.config.results_dir / f'coverage_{category}.json'
        if coverage_file.exists():
            try:
                with open(coverage_file, 'r') as f:
                    coverage_data = json.load(f)
                return coverage_data.get('totals', {}).get('percent_covered', 0)
            except Exception as e:
                print(f"Warning: Could not parse coverage data: {e}")
        return 0.0
    
    def _create_empty_summary(self, category: str) -> Dict[str, Any]:
        """Create an empty summary for categories with no tests."""
        return {
            'category': category,
            'test_files': [],
            'results': {},
            'summary': {
                'tests_run': 0,
                'tests_passed': 0,
                'tests_failed': 0,
                'tests_skipped': 0,
                'duration': 0,
                'coverage_percentage': 0
            }
        }
    
    def run_all_tests(self, verbose: bool = True, coverage: bool = True) -> Dict[str, Any]:
        """Run all available tests."""
        all_summaries = {}
        total_stats = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'tests_skipped': 0,
            'duration': 0
        }
        
        categories = [cat for cat in self.config.get_available_categories() if cat != 'all']
        
        for category in categories:
            summary = self.run_tests(category, verbose, coverage)
            all_summaries[category] = summary
            
            # Aggregate stats
            stats = summary['summary']
            total_stats['tests_run'] += stats['tests_run']
            total_stats['tests_passed'] += stats['tests_passed']
            total_stats['tests_failed'] += stats['tests_failed']
            total_stats['tests_skipped'] += stats['tests_skipped']
            total_stats['duration'] += stats['duration']
        
        # Calculate overall coverage percentage
        total_coverage = 0
        coverage_count = 0
        for category_summary in all_summaries.values():
            if 'summary' in category_summary and 'coverage_percentage' in category_summary['summary']:
                total_coverage += category_summary['summary']['coverage_percentage']
                coverage_count += 1
        
        overall_coverage = total_coverage / coverage_count if coverage_count > 0 else 0
        
        # Create overall summary
        overall_summary = {
            'category': 'all',
            'test_files': [],
            'results': {},
            'summary': {
                **total_stats,
                'coverage_percentage': overall_coverage
            },
            'categories': all_summaries
        }
        
        return overall_summary
    
    def create_test_template(self, category: str, test_name: str):
        """Create a test template for a new test."""
        category_info = self.config.get_category(category)
        if not category_info:
            print(f"Error: Unknown category '{category}'")
            return
        
        # Determine test directory based on test type
        if category_info.test_type == 'unit':
            test_dir = self.project_root / 'tests' / 'unit'
        elif category_info.test_type == 'integration':
            test_dir = self.project_root / 'tests' / 'integration'
        elif category_info.test_type == 'performance':
            test_dir = self.project_root / 'tests' / 'performance'
        else:
            test_dir = self.project_root / 'tests' / 'unit'
        
        test_dir.mkdir(exist_ok=True)
        
        # Create test file
        test_file = test_dir / f'test_{test_name}.py'
        
        if test_file.exists():
            print(f"Error: Test file already exists: {test_file}")
            return
        
        # Create test template
        template = f'''"""
{category_info.name} - {test_name.title()}

{category_info.description}
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

class Test{test_name.title()}:
    """Test class for {test_name}."""
    
    def test_example(self):
        """Example test method."""
        # Add your test logic here
        assert True
    
    def test_another_example(self):
        """Another example test method."""
        # Add your test logic here
        assert True

if __name__ == '__main__':
    pytest.main([__file__])
'''
        
        with open(test_file, 'w') as f:
            f.write(template)
        
        print(f"Created test template: {test_file}")
        print(f"Category: {category}")
        print(f"Test Name: {test_name}")
        print(f"Description: {category_info.description}")
