"""
Test configuration and fixtures for the data processing framework.

This module provides shared fixtures and configuration for all tests.
It's designed to be minimal and focused on core testing utilities.
"""
import os
import sys
import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
import pytest

# Add src to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from core.config import config
from core.logging import setup_logging

# Test results directory
RESULTS_DIR = PROJECT_ROOT / 'tests' / 'results'
RESULTS_DIR.mkdir(exist_ok=True)


class TestResultsCollector:
    """Collects and saves test results for tracking."""
    
    def __init__(self):
        self.results = {
            'test_run': {
                'timestamp': datetime.now().isoformat(),
                'framework_version': '1.0.0',
                'python_version': sys.version.split()[0]
            },
            'tests': []
        }
    
    def add_result(self, test_name, status, duration, details=None):
        """Add a test result."""
        self.results['tests'].append({
            'name': test_name,
            'status': status,  # 'PASSED', 'FAILED', 'SKIPPED'
            'duration': duration,
            'details': details or {}
        })
    
    def save_results(self):
        """Save results to JSON file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_file = RESULTS_DIR / f'test_results_{timestamp}.json'
        
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        # Also save as latest
        latest_file = RESULTS_DIR / 'latest_test_results.json'
        with open(latest_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        return results_file


@pytest.fixture(scope="session")
def test_results_collector():
    """Pytest fixture for collecting test results."""
    collector = TestResultsCollector()
    yield collector
    # Save results at the end of the session
    results_file = collector.save_results()
    print(f"\nTest results saved to: {results_file}")


@pytest.fixture(scope="session")
def test_env():
    """Set up test environment with proper isolation."""
    # Create temporary directory for test files
    temp_dir = tempfile.mkdtemp(prefix='data_processor_test_')
    
    # Set test environment variables
    test_env_vars = {
        'CLICKHOUSE_HOST': 'localhost',
        'CLICKHOUSE_PORT': '8123',
        'CLICKHOUSE_USER': 'default',
        'CLICKHOUSE_PASSWORD': '',
        'CLICKHOUSE_DATABASE': 'test_data_warehouse',
        'LOG_DIR': os.path.join(temp_dir, 'logs'),
        'LOG_LEVEL': 'DEBUG',
        'TIMEOUT': '10',
        'BATCH_SIZE': '100'
    }
    
    # Store original env vars
    original_env = {}
    for key, value in test_env_vars.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value
    
    # Setup logging for tests
    os.makedirs(test_env_vars['LOG_DIR'], exist_ok=True)
    setup_logging('DEBUG')
    
    yield {
        'temp_dir': temp_dir,
        'env_vars': test_env_vars
    }
    
    # Cleanup
    for key, value in original_env.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    
    # Remove temp directory
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_pipeline_data():
    """Sample data for testing pipelines."""
    return {
        'test_data': [
            {'id': 1, 'name': 'Test Item 1', 'value': 100.5, 'timestamp': datetime.now()},
            {'id': 2, 'name': 'Test Item 2', 'value': 200.75, 'timestamp': datetime.now()},
            {'id': 3, 'name': 'Test Item 3', 'value': 300.25, 'timestamp': datetime.now()}
        ]
    }


@pytest.fixture
def mock_http_response():
    """Mock HTTP response for testing extractors."""
    return {
        'data': [
            {'id': 1, 'name': 'Item 1', 'value': 10.5},
            {'id': 2, 'name': 'Item 2', 'value': 20.75}
        ],
        'status': 'success',
        'timestamp': datetime.now().isoformat()
    }


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "unit: mark test as unit test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "database: mark test as requiring database")
    config.addinivalue_line("markers", "network: mark test as requiring network access")
