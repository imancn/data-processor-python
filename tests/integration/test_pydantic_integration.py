"""
Integration tests for Pydantic validation system.

This module tests the integration of Pydantic validation with:
- Configuration system
- Pipeline system
- Main application
- End-to-end validation workflows
"""

import pytest
import pandas as pd
from datetime import datetime
from typing import Dict, Any
from unittest.mock import patch, MagicMock

from core import (
    config, validate_config, validate_pipeline_config,
    validate_extractor_config, validate_transformer_config,
    validate_loader_config, validate_dataframe, safe_validate,
    FrameworkSettings, PipelineConfig, ValidationError, DataFrameInfo
)
from main import register_cron_job, run_cron_job
from pipelines import create_etl_pipeline, register_pipeline


class TestConfigurationIntegration:
    """Test Pydantic validation integration with configuration system."""
    
    def test_config_validation_integration(self):
        """Test that configuration system uses Pydantic validation."""
        # Test that config object uses Pydantic validation
        assert isinstance(config.settings, FrameworkSettings)
        
        # Test configuration access with type safety
        log_level = config.log_level
        assert isinstance(log_level, str)
        assert log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        
        timeout = config.timeout
        assert isinstance(timeout, int)
        assert timeout > 0
    
    def test_config_validation_error_handling(self):
        """Test configuration validation error handling."""
        # Test with invalid configuration data
        invalid_config = {
            'log_level': 'INVALID_LEVEL',
            'timeout': -10,
            'clickhouse_port': 70000
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_config(invalid_config)
        
        error = exc_info.value
        assert hasattr(error, 'details')
        assert error.details is not None
    
    def test_config_environment_loading(self):
        """Test configuration loading from environment variables."""
        with patch.dict('os.environ', {
            'LOG_LEVEL': 'DEBUG',
            'TIMEOUT': '60',
            'BATCH_SIZE': '2000'
        }):
            # Reload configuration to pick up environment variables
            config.reload()
            
            assert config.log_level == 'DEBUG'
            assert config.timeout == 60
            assert config.batch_size == 2000
    
    def test_config_type_conversion(self):
        """Test automatic type conversion in configuration."""
        with patch.dict('os.environ', {
            'TIMEOUT': '45',  # String
            'BATCH_SIZE': '1500'  # String
        }):
            config.reload()
            
            # Should be automatically converted to integers
            assert isinstance(config.timeout, int)
            assert isinstance(config.batch_size, int)
            assert config.timeout == 45
            assert config.batch_size == 1500


class TestPipelineIntegration:
    """Test Pydantic validation integration with pipeline system."""
    
    def test_pipeline_registration_validation(self):
        """Test that pipeline registration uses Pydantic validation."""
        def test_pipeline():
            return True
        
        # Test successful pipeline registration
        try:
            register_cron_job(
                job_name='test_pipeline',
                pipeline=test_pipeline,
                schedule='0 * * * *',
                description='Test pipeline',
                timeout=300,
                time_scope='hourly'
            )
            
            # Pipeline should be registered successfully
            assert True  # If we get here, validation passed
            
        except ValidationError as e:
            pytest.fail(f"Pipeline registration failed with validation error: {e}")
    
    def test_pipeline_registration_invalid_schedule(self):
        """Test pipeline registration with invalid cron schedule."""
        def test_pipeline():
            return True
        
        with pytest.raises(ValidationError) as exc_info:
            register_cron_job(
                job_name='test_pipeline',
                pipeline=test_pipeline,
                schedule='invalid_schedule',
                description='Test pipeline'
            )
        
        error = exc_info.value
        assert "Invalid cron schedule" in str(error) or "schedule" in str(error).lower()
    
    def test_pipeline_registration_invalid_time_scope(self):
        """Test pipeline registration with invalid time scope."""
        def test_pipeline():
            return True
        
        with pytest.raises(ValidationError) as exc_info:
            register_cron_job(
                job_name='test_pipeline',
                pipeline=test_pipeline,
                schedule='0 * * * *',
                description='Test pipeline',
                time_scope='invalid_scope'
            )
        
        error = exc_info.value
        assert "Invalid time scope" in str(error) or "time_scope" in str(error).lower()
    
    def test_pipeline_registration_missing_required_fields(self):
        """Test pipeline registration with missing required fields."""
        def test_pipeline():
            return True
        
        with pytest.raises(ValidationError) as exc_info:
            register_cron_job(
                job_name='',  # Empty name
                pipeline=test_pipeline,
                schedule='0 * * * *'
            )
        
        error = exc_info.value
        assert "name" in str(error).lower() or "min_length" in str(error).lower()


class TestToolValidationIntegration:
    """Test Pydantic validation integration with pipeline tools."""
    
    def test_extractor_validation_integration(self):
        """Test extractor validation integration."""
        # Test HTTP extractor validation
        http_config = {
            'type': 'http',
            'url': 'https://api.example.com/data',
            'headers': {'Authorization': 'Bearer token'},
            'timeout': 30
        }
        
        result = validate_extractor_config(http_config)
        assert isinstance(result, type(validate_extractor_config(http_config)))
        assert result.type == 'http'
        assert result.url == 'https://api.example.com/data'
    
    def test_transformer_validation_integration(self):
        """Test transformer validation integration."""
        transform_config = {
            'type': 'lambda',
            'function': 'lambda df: df.dropna()',
            'params': {'drop_duplicates': True}
        }
        
        result = validate_transformer_config(transform_config)
        assert isinstance(result, type(validate_transformer_config(transform_config)))
        assert result.type == 'lambda'
        assert result.function == 'lambda df: df.dropna()'
    
    def test_loader_validation_integration(self):
        """Test loader validation integration."""
        loader_config = {
            'type': 'clickhouse',
            'table': 'test_table',
            'columns': ['id', 'name', 'value'],
            'batch_size': 1000,
            'mode': 'upsert'
        }
        
        result = validate_loader_config(loader_config)
        assert isinstance(result, type(validate_loader_config(loader_config)))
        assert result.type == 'clickhouse'
        assert result.table == 'test_table'
        assert result.mode == 'upsert'


class TestDataFrameValidationIntegration:
    """Test DataFrame validation integration."""
    
    def test_dataframe_validation_integration(self):
        """Test DataFrame validation integration."""
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'value': [10.5, 20.3, 15.7, 8.9, 12.1],
            'timestamp': [datetime.now() for _ in range(5)]
        })
        
        # Test validation with required columns
        result = validate_dataframe(df, required_columns=['id', 'name', 'value'])
        assert isinstance(result, type(validate_dataframe(df)))
        assert result.shape[0] == 5  # num_rows
        assert result.shape[1] == 4  # num_columns
        assert 'id' in result.columns
        assert 'name' in result.columns
        assert 'value' in result.columns
    
    def test_dataframe_validation_missing_columns(self):
        """Test DataFrame validation with missing required columns."""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
        })
        
        with pytest.raises(ValidationError) as exc_info:
            validate_dataframe(df, required_columns=['id', 'name', 'value'])
        
        error = exc_info.value
        assert "missing required columns" in str(error)
    
    def test_dataframe_validation_success(self):
        """Test DataFrame validation with valid data."""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
        })
        
        # This should not raise an error
        result = validate_dataframe(df)
        assert isinstance(result, DataFrameInfo)
        assert result.shape[0] == 5
        assert result.shape[1] == 2


class TestSafeValidationIntegration:
    """Test safe validation integration."""
    
    def test_safe_validation_success(self):
        """Test successful safe validation."""
        data = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        result = safe_validate(data, 'framework_settings')
        assert isinstance(result, type(safe_validate(data, 'framework_settings')))
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    def test_safe_validation_failure(self):
        """Test safe validation with validation failure."""
        data = {
            'log_level': 'INVALID_LEVEL',
            'timeout': -10,
            'batch_size': 1000
        }
        
        result = safe_validate(data, 'framework_settings')
        assert isinstance(result, type(safe_validate(data, 'framework_settings')))
        assert result.is_valid is False
        assert len(result.errors) > 0
    
    def test_safe_validation_pipeline_config(self):
        """Test safe validation with pipeline configuration."""
        data = {
            'name': 'test_pipeline',
            'schedule': '0 * * * *',
            'time_scope': 'hourly'
        }
        
        result = safe_validate(data, 'pipeline_config')
        assert isinstance(result, type(safe_validate(data, 'pipeline_config')))
        assert result.is_valid is True
        assert len(result.errors) == 0


class TestEndToEndValidation:
    """Test end-to-end validation workflows."""
    
    def test_complete_pipeline_validation_workflow(self):
        """Test complete pipeline validation workflow."""
        # Step 1: Validate configuration
        config_data = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        config_result = validate_config(config_data)
        assert isinstance(config_result, FrameworkSettings)
        
        # Step 2: Validate pipeline configuration
        pipeline_data = {
            'name': 'test_pipeline',
            'description': 'Test pipeline',
            'schedule': '0 * * * *',
            'time_scope': 'hourly',
            'timeout': 300,
            'enabled': True
        }
        
        pipeline_result = validate_pipeline_config(pipeline_data)
        assert isinstance(pipeline_result, PipelineConfig)
        
        # Step 3: Validate extractor configuration
        extractor_data = {
            'type': 'http',
            'url': 'https://api.example.com/data',
            'timeout': 30
        }
        
        extractor_result = validate_extractor_config(extractor_data)
        assert isinstance(extractor_result, type(validate_extractor_config(extractor_data)))
        
        # Step 4: Validate transformer configuration
        transformer_data = {
            'type': 'lambda',
            'function': 'lambda df: df.dropna()'
        }
        
        transformer_result = validate_transformer_config(transformer_data)
        assert isinstance(transformer_result, type(validate_transformer_config(transformer_data)))
        
        # Step 5: Validate loader configuration
        loader_data = {
            'type': 'clickhouse',
            'table': 'test_table',
            'batch_size': 1000
        }
        
        loader_result = validate_loader_config(loader_data)
        assert isinstance(loader_result, type(validate_loader_config(loader_data)))
        
        # Step 6: Validate DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10.5, 20.3, 15.7]
        })
        
        df_result = validate_dataframe(df, required_columns=['id', 'name', 'value'])
        assert isinstance(df_result, type(validate_dataframe(df)))
    
    def test_validation_error_propagation(self):
        """Test that validation errors propagate correctly through the system."""
        # Test with invalid pipeline configuration
        invalid_pipeline_data = {
            'name': 'test_pipeline',
            'schedule': 'invalid_schedule',
            'time_scope': 'invalid_scope'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_pipeline_config(invalid_pipeline_data)
        
        error = exc_info.value
        assert isinstance(error, ValidationError)
        assert hasattr(error, 'details')
    
    def test_validation_performance(self):
        """Test validation performance with large datasets."""
        # Create large DataFrame
        large_df = pd.DataFrame({
            'id': range(10000),
            'name': [f'User_{i}' for i in range(10000)],
            'value': [i * 0.1 for i in range(10000)]
        })
        
        # Test validation performance
        import time
        start_time = time.time()
        
        result = validate_dataframe(large_df, required_columns=['id', 'name', 'value'])
        
        end_time = time.time()
        validation_time = end_time - start_time
        
        # Validation should complete quickly (less than 1 second)
        assert validation_time < 1.0
        assert isinstance(result, type(validate_dataframe(large_df)))


class TestValidationErrorHandling:
    """Test validation error handling and recovery."""
    
    def test_validation_error_context(self):
        """Test that validation errors include proper context."""
        data = {
            'log_level': 'INVALID_LEVEL',
            'timeout': -10,
            'clickhouse_port': 70000
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_config(data)
        
        error = exc_info.value
        assert hasattr(error, 'details')
        assert error.details is not None
        
        # Check that error details contain field-specific information
        if isinstance(error.details, list):
            assert len(error.details) > 0
        elif isinstance(error.details, dict):
            assert len(error.details) > 0
    
    def test_validation_error_recovery(self):
        """Test validation error recovery mechanisms."""
        # Test with invalid data that can be corrected
        invalid_data = {
            'log_level': 'INFO',
            'timeout': '30',  # String that can be converted
            'batch_size': '1000'  # String that can be converted
        }
        
        # Should succeed with automatic type conversion
        result = validate_config(invalid_data)
        assert isinstance(result, FrameworkSettings)
        assert isinstance(result.timeout, int)
        assert isinstance(result.batch_size, int)
    
    def test_validation_error_logging(self):
        """Test that validation errors are properly logged."""
        # This would require mocking the logging system
        # For now, we'll test that validation errors are raised correctly
        data = {
            'name': 'test_pipeline',
            'schedule': 'invalid_schedule'
        }
        
        with pytest.raises(ValidationError) as exc_info:
            validate_pipeline_config(data)
        
        error = exc_info.value
        assert isinstance(error, ValidationError)
        assert str(error) is not None


if __name__ == '__main__':
    pytest.main([__file__])
