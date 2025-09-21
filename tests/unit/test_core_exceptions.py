"""
Core Framework Tests - Exception System

Tests for the core exception system including:
- Exception hierarchy
- Error handling
- Exception creation
- Error context
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.exceptions import (
    FrameworkError, ConfigurationError, DatabaseError,
    PipelineError, ExtractionError, TransformationError,
    LoadingError, MigrationError, ValidationError,
    create_exception
)


class TestCoreExceptions:
    """Test core exception system."""
    
    def test_framework_error_basic(self):
        """Test basic FrameworkError functionality."""
        error = FrameworkError("Test error message")
        assert str(error) == "Test error message"
        assert error.details == {}
    
    def test_framework_error_with_details(self):
        """Test FrameworkError with details."""
        details = {"field": "value", "code": 123}
        error = FrameworkError("Test error message", details)
        assert str(error) == "Test error message | Details: {'field': 'value', 'code': 123}"
        assert error.details == details
    
    def test_configuration_error(self):
        """Test ConfigurationError."""
        error = ConfigurationError("Configuration error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Configuration error"
    
    def test_database_error(self):
        """Test DatabaseError."""
        error = DatabaseError("Database error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Database error"
    
    def test_pipeline_error(self):
        """Test PipelineError."""
        error = PipelineError("Pipeline error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Pipeline error"
    
    def test_extraction_error(self):
        """Test ExtractionError."""
        error = ExtractionError("Extraction error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Extraction error"
    
    def test_transformation_error(self):
        """Test TransformationError."""
        error = TransformationError("Transformation error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Transformation error"
    
    def test_loading_error(self):
        """Test LoadingError."""
        error = LoadingError("Loading error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Loading error"
    
    def test_migration_error(self):
        """Test MigrationError."""
        error = MigrationError("Migration error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Migration error"
    
    def test_validation_error(self):
        """Test ValidationError."""
        error = ValidationError("Validation error")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Validation error"
    
    def test_exception_inheritance(self):
        """Test exception inheritance hierarchy."""
        # Test that specific errors inherit from base classes
        config_error = ConfigurationError("test")
        assert isinstance(config_error, FrameworkError)
        
        db_error = DatabaseError("test")
        assert isinstance(db_error, FrameworkError)
        
        pipeline_error = PipelineError("test")
        assert isinstance(pipeline_error, FrameworkError)
        
        extraction_error = ExtractionError("test")
        assert isinstance(extraction_error, FrameworkError)
    
    def test_create_exception_factory(self):
        """Test exception factory function."""
        # Test creating known exception types
        config_error = create_exception("config", "Test message")
        assert isinstance(config_error, ConfigurationError)
        assert str(config_error) == "Test message"
        
        db_error = create_exception("database", "Test message")
        assert isinstance(db_error, DatabaseError)
        assert str(db_error) == "Test message"
        
        pipeline_error = create_exception("pipeline", "Test message")
        assert isinstance(pipeline_error, PipelineError)
        assert str(pipeline_error) == "Test message"
    
    def test_create_exception_unknown_type(self):
        """Test exception factory with unknown type."""
        # Should fall back to FrameworkError
        error = create_exception("UnknownError", "Test message")
        assert isinstance(error, FrameworkError)
        assert str(error) == "Test message"
    
    def test_exception_with_details(self):
        """Test exceptions with detailed information."""
        details = {
            "field": "test_field",
            "value": "invalid_value",
            "expected": "valid_value",
            "code": 400
        }
        
        error = ValidationError("Validation failed", details)
        assert error.details == details
        assert "Details:" in str(error)
    
    def test_exception_string_representation(self):
        """Test exception string representation."""
        # Test without details
        error = FrameworkError("Simple error")
        assert str(error) == "Simple error"
        
        # Test with details
        error_with_details = FrameworkError("Error with details", {"key": "value"})
        assert "Details:" in str(error_with_details)
        assert "key" in str(error_with_details)
        assert "value" in str(error_with_details)
    
    def test_exception_context(self):
        """Test exception context preservation."""
        try:
            raise ConfigurationError("Test error", {"config_file": "test.conf"})
        except ConfigurationError as e:
            assert e.details == {"config_file": "test.conf"}
            assert "config_file" in str(e)
    
    def test_exception_chaining(self):
        """Test exception chaining."""
        try:
            try:
                raise ValueError("Original error")
            except ValueError as e:
                raise ConfigurationError("Configuration error") from e
        except ConfigurationError as e:
            assert isinstance(e.__cause__, ValueError)
            assert str(e.__cause__) == "Original error"
    
    def test_all_exception_types(self):
        """Test all exception types can be created."""
        exception_types = [
            "FrameworkError",
            "ConfigurationError", 
            "DatabaseError",
            "PipelineError",
            "ExtractionError",
            "TransformationError",
            "LoadingError",
            "MigrationError",
            "ValidationError"
        ]
        
        for exc_type in exception_types:
            error = create_exception(exc_type, f"Test {exc_type}")
            assert isinstance(error, FrameworkError)
            assert str(error) == f"Test {exc_type}"


if __name__ == '__main__':
    pytest.main([__file__])
