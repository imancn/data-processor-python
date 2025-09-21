"""
Test framework for the data processing framework.

This package provides a unified testing framework with:
- Test discovery and execution
- Result collection and reporting
- Coverage analysis
- Test utilities and fixtures
"""

from .test_runner import TestFrameworkRunner
from .test_discovery import TestFrameworkDiscovery
from .test_reporter import TestFrameworkReporter
from .test_config import TestFrameworkConfig

__all__ = [
    'TestFrameworkRunner',
    'TestFrameworkDiscovery', 
    'TestFrameworkReporter',
    'TestFrameworkConfig'
]
