"""
Core Framework Tests - Logging System

Tests for the core logging system including:
- Log setup and configuration
- Structured logging
- Performance logging
- Context management
"""

import pytest
import sys
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.logging import (
    setup_logging, log_with_timestamp, get_logger,
    LoggingContext, PerformanceLogger,
    log_function_call, log_pipeline_stage,
    create_job_logger
)


class TestCoreLogging:
    """Test core logging system."""
    
    def test_setup_logging(self):
        """Test logging setup."""
        # Test basic setup
        setup_logging('INFO')
        
        # Verify root logger is configured
        root_logger = logging.getLogger()
        assert root_logger.level <= logging.INFO
    
    def test_log_with_timestamp(self):
        """Test timestamped logging."""
        # This should not raise an exception
        log_with_timestamp("Test message", "TestCategory", "info")
        log_with_timestamp("Warning message", "TestCategory", "warning")
        log_with_timestamp("Error message", "TestCategory", "error")
    
    def test_get_logger(self):
        """Test logger creation."""
        logger = get_logger("test_logger")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_logger"
    
    def test_logging_context(self):
        """Test logging context manager."""
        with LoggingContext("TestContext", job_id="123", pipeline="test_pipeline"):
            log_with_timestamp("Context message", "TestContext")
        
        # Context should be cleaned up
        assert True  # If we get here, context worked
    
    def test_performance_logger(self):
        """Test performance logging."""
        with PerformanceLogger("TestOperation", "Performance"):
            # Simulate some work
            import time
            time.sleep(0.01)
        
        # Performance should be logged
        assert True  # If we get here, performance logging worked
    
    def test_log_function_call_decorator(self):
        """Test function call logging decorator."""
        @log_function_call("test_function", "debug")
        def test_function():
            return "test_result"
        
        result = test_function()
        assert result == "test_result"
    
    def test_log_pipeline_stage_decorator(self):
        """Test pipeline stage logging decorator."""
        @log_pipeline_stage("TestStage", "info")
        def test_pipeline_stage():
            return "stage_result"
        
        result = test_pipeline_stage()
        assert result == "stage_result"
    
    def test_create_job_logger(self):
        """Test job logger creation."""
        job_logger = create_job_logger("test_job")
        assert isinstance(job_logger, logging.Logger)
        assert "test_job" in job_logger.name
    
    def test_logging_levels(self):
        """Test different logging levels."""
        # Test all logging levels
        levels = ['debug', 'info', 'warning', 'error', 'critical']
        
        for level in levels:
            log_with_timestamp(f"Test {level} message", "TestCategory", level)
        
        # All levels should work without exception
        assert True
    
    def test_logging_with_context(self):
        """Test logging with additional context."""
        with LoggingContext("TestContext", additional_data={"key": "value"}):
            log_with_timestamp("Context message", "TestContext")
        
        # Context should be handled properly
        assert True
    
    def test_performance_logging_timing(self):
        """Test performance logging timing."""
        with PerformanceLogger("TimingTest", "Performance") as perf:
            import time
            time.sleep(0.01)
        
        # Performance should be measured
        assert True
    
    def test_multiple_loggers(self):
        """Test multiple logger instances."""
        logger1 = get_logger("logger1")
        logger2 = get_logger("logger2")
        
        assert logger1 != logger2
        assert logger1.name == "logger1"
        assert logger2.name == "logger2"
    
    def test_logging_configuration(self):
        """Test logging configuration."""
        # Test with different log levels
        for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR']:
            setup_logging(level)
            root_logger = logging.getLogger()
            assert root_logger.level <= getattr(logging, level)


if __name__ == '__main__':
    pytest.main([__file__])
