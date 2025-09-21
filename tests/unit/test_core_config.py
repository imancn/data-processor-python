"""
Core Framework Tests - Configuration System

Tests for the core configuration system including:
- Configuration loading
- Environment variable handling
- Type conversion
- Validation
"""

import pytest
import sys
from pathlib import Path
from unittest.mock import patch

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.config import config, Config
from core.exceptions import ConfigurationError


class TestCoreConfig:
    """Test core configuration system."""
    
    def test_config_initialization(self):
        """Test that configuration initializes correctly."""
        assert isinstance(config, Config)
        assert hasattr(config, 'settings')
    
    def test_config_properties(self):
        """Test configuration properties."""
        # Test that properties are accessible
        assert hasattr(config, 'log_level')
        assert hasattr(config, 'timeout')
        assert hasattr(config, 'batch_size')
        assert hasattr(config, 'log_dir')
        assert hasattr(config, 'log_file')
    
    def test_config_get_method(self):
        """Test configuration get method."""
        # Test getting existing values
        log_level = config.get('log_level')
        assert log_level is not None
        
        # Test getting non-existent values with default
        non_existent = config.get('non_existent_key', 'default_value')
        assert non_existent == 'default_value'
    
    def test_config_type_methods(self):
        """Test configuration type conversion methods."""
        # Test string conversion
        log_level = config.get_str('log_level', 'INFO')
        assert isinstance(log_level, str)
        
        # Test integer conversion
        timeout = config.get_int('timeout', 30)
        assert isinstance(timeout, int)
        
        # Test boolean conversion
        enabled = config.get_bool('enabled', True)
        assert isinstance(enabled, bool)
    
    def test_clickhouse_config(self):
        """Test ClickHouse configuration."""
        ch_config = config.get_clickhouse_config()
        assert isinstance(ch_config, dict)
        assert 'host' in ch_config
        assert 'port' in ch_config
        assert 'user' in ch_config
        assert 'database' in ch_config
    
    def test_api_config(self):
        """Test API configuration."""
        api_config = config.get_api_config()
        # API config might be None if not configured
        if api_config is not None:
            assert isinstance(api_config, dict)
    
    def test_config_to_dict(self):
        """Test configuration to dictionary conversion."""
        config_dict = config.to_dict()
        assert isinstance(config_dict, dict)
        assert 'log_level' in config_dict
        assert 'timeout' in config_dict
    
    def test_config_reload(self):
        """Test configuration reload."""
        # This should not raise an exception
        config.reload()
        assert isinstance(config, Config)
    
    def test_config_update(self):
        """Test configuration update."""
        # Test updating configuration
        original_timeout = config.timeout
        config.update(timeout=60)
        assert config.timeout == 60
        
        # Restore original value
        config.update(timeout=original_timeout)
        assert config.timeout == original_timeout
    
    def test_config_validation(self):
        """Test configuration validation."""
        # Test with valid configuration
        valid_config = {
            'log_level': 'INFO',
            'timeout': 30,
            'batch_size': 1000
        }
        
        # This should not raise an exception
        config.update(**valid_config)
    
    def test_config_error_handling(self):
        """Test configuration error handling."""
        # Test with invalid configuration
        with pytest.raises(ConfigurationError):
            config.update(invalid_field='invalid_value')


if __name__ == '__main__':
    pytest.main([__file__])
