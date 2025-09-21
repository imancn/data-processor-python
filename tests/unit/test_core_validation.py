"""
Core Framework Tests - Validation System

Tests for the core validation system including:
- Legacy validators
- Pydantic validators
- Data validation
- Error handling
"""

import pytest
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.validators import (
    validate_config as validate_config_legacy,
    validate_log_level, validate_time_scope,
    validate_url, validate_file_path,
    validate_dataframe as validate_dataframe_legacy
)
from core.pydantic_validators import (
    validate_config, validate_pipeline_config,
    validate_extractor_config, validate_transformer_config,
    validate_loader_config, validate_dataframe,
    safe_validate
)
from core.exceptions import ValidationError


class TestCoreValidation:
    """Test core validation system."""
    
    def test_legacy_config_validation(self):
        """Test legacy configuration validation."""
        # Test valid configuration
        valid_config = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        from core.validators import validate_config as validate_config_legacy
        result = validate_config_legacy(valid_config, ['log_level', 'timeout', 'batch_size'])
        assert result is None  # validate_config doesn't return anything, it raises on error
    
    def test_legacy_config_validation_required_keys(self):
        """Test legacy configuration validation with required keys."""
        config = {'log_level': 'INFO'}
        required_keys = ['log_level', 'timeout']
        
        with pytest.raises(ValidationError):
            validate_config_legacy(config, required_keys)
    
    def test_validate_log_level(self):
        """Test log level validation."""
        # Valid log levels
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        for level in valid_levels:
            # validate_log_level doesn't return anything, it raises on error
            validate_log_level(level)  # Should not raise
        
        # Invalid log level
        with pytest.raises(ValidationError):
            validate_log_level('INVALID_LEVEL')
    
    def test_validate_time_scope(self):
        """Test time scope validation."""
        # Valid time scopes
        valid_scopes = ['latest', 'hourly', 'daily', 'weekly', 'monthly', 'yearly']
        for scope in valid_scopes:
            # validate_time_scope doesn't return anything, it raises on error
            validate_time_scope(scope)  # Should not raise
        
        # Invalid time scope
        with pytest.raises(ValidationError):
            validate_time_scope('invalid_scope')
    
    def test_validate_url(self):
        """Test URL validation."""
        # Valid URLs
        valid_urls = [
            'https://api.example.com/data',
            'http://localhost:8080/api',
            'https://subdomain.example.com/path?param=value'
        ]
        for url in valid_urls:
            # validate_url doesn't return anything, it raises on error
            validate_url(url)  # Should not raise
        
        # Invalid URLs
        invalid_urls = ['invalid_url', 'not-a-url', '']
        for url in invalid_urls:
            with pytest.raises(ValidationError):
                validate_url(url)
    
    def test_validate_file_path(self):
        """Test file path validation."""
        # Test with existing file (this test file)
        test_file = __file__
        # validate_file_path doesn't return anything, it raises on error
        validate_file_path(test_file, must_exist=True)  # Should not raise
        
        # Test with non-existent file
        with pytest.raises(ValidationError):
            validate_file_path('non_existent_file.txt', must_exist=True)
    
    def test_validate_dataframe_legacy(self):
        """Test legacy DataFrame validation."""
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.3, 15.7]
        })
        
        # Test valid DataFrame
        # validate_dataframe_legacy doesn't return anything, it raises on error
        validate_dataframe_legacy(df, required_columns=['id', 'name', 'value'])  # Should not raise
        
        # Test with missing columns
        with pytest.raises(ValidationError):
            validate_dataframe_legacy(df, required_columns=['id', 'name', 'value', 'missing'])
    
    def test_pydantic_config_validation(self):
        """Test Pydantic configuration validation."""
        # Test valid configuration
        valid_config = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        result = validate_config(valid_config)
        assert hasattr(result, 'log_level')
        assert result.log_level == 'INFO'
    
    def test_pydantic_pipeline_validation(self):
        """Test Pydantic pipeline validation."""
        # Test valid pipeline configuration
        valid_pipeline = {
            'name': 'test_pipeline',
            'schedule': '0 * * * *',
            'time_scope': 'hourly'
        }
        
        result = validate_pipeline_config(valid_pipeline)
        assert hasattr(result, 'name')
        assert result.name == 'test_pipeline'
    
    def test_pydantic_extractor_validation(self):
        """Test Pydantic extractor validation."""
        # Test valid extractor configuration
        valid_extractor = {
            'type': 'http',
            'url': 'https://api.example.com/data',
            'timeout': 30
        }
        
        result = validate_extractor_config(valid_extractor)
        assert hasattr(result, 'type')
        assert result.type == 'http'
    
    def test_pydantic_transformer_validation(self):
        """Test Pydantic transformer validation."""
        # Test valid transformer configuration
        valid_transformer = {
            'type': 'lambda',
            'function': 'lambda df: df.dropna()'
        }
        
        result = validate_transformer_config(valid_transformer)
        assert hasattr(result, 'type')
        assert result.type == 'lambda'
    
    def test_pydantic_loader_validation(self):
        """Test Pydantic loader validation."""
        # Test valid loader configuration
        valid_loader = {
            'type': 'clickhouse',
            'table': 'test_table',
            'batch_size': 1000
        }
        
        result = validate_loader_config(valid_loader)
        assert hasattr(result, 'type')
        assert result.type == 'clickhouse'
    
    def test_pydantic_dataframe_validation(self):
        """Test Pydantic DataFrame validation."""
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.3, 15.7]
        })
        
        # Test valid DataFrame
        result = validate_dataframe(df, required_columns=['id', 'name', 'value'])
        assert hasattr(result, 'shape')
        assert result.shape[0] == 3  # num_rows
    
    def test_safe_validation(self):
        """Test safe validation."""
        # Test valid data
        valid_data = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        result = safe_validate(valid_data, 'framework_settings')
        assert result.is_valid is True
        assert len(result.errors) == 0
        
        # Test invalid data
        invalid_data = {
            'log_level': 'INVALID_LEVEL',
            'timeout': -10
        }
        
        result = safe_validate(invalid_data, 'framework_settings')
        assert result.is_valid is False
        assert len(result.errors) > 0
    
    def test_validation_error_handling(self):
        """Test validation error handling."""
        # Test that validation errors include proper context
        with pytest.raises(ValidationError) as exc_info:
            validate_config({'log_level': 'INVALID_LEVEL'})
        
        error = exc_info.value
        assert hasattr(error, 'details')
        assert error.details is not None
    
    def test_type_conversion(self):
        """Test automatic type conversion."""
        # Test string to int conversion
        config_data = {
            'log_level': 'INFO',
            'timeout': '30',  # String
            'batch_size': '1000'  # String
        }
        
        result = validate_config(config_data)
        assert isinstance(result.timeout, int)
        assert isinstance(result.batch_size, int)
        assert result.timeout == 30
        assert result.batch_size == 1000


if __name__ == '__main__':
    pytest.main([__file__])
