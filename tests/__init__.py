"""
Test suite for the data processing framework.

This package contains comprehensive tests including unit tests,
integration tests, and end-to-end tests for all framework components.

The test framework is organized as follows:
- tests/framework/ - Core testing framework components
- tests/unit/ - Unit tests for individual components
- tests/integration/ - Integration tests for component interactions
- tests/performance/ - Performance and load tests
- tests/results/ - Test execution results and reports

Usage:
    # Run all tests
    python3 tests/run_tests.py
    
    # Run specific category
    python3 tests/run_tests.py core
    
    # List available categories
    python3 tests/run_tests.py --list
    
    # Run with pytest directly
    pytest tests/
"""

# Import framework components for easy access
from .framework import TestFrameworkRunner, TestFrameworkConfig, TestFrameworkDiscovery, TestFrameworkReporter

# Public API
__all__ = [
    'TestFrameworkRunner',
    'TestFrameworkConfig', 
    'TestFrameworkDiscovery',
    'TestFrameworkReporter'
]
