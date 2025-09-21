# src/core/config.py
"""
Configuration management for the data processing framework.

This module provides centralized configuration management using Pydantic
BaseSettings for automatic validation, type safety, and environment
variable loading.
"""

import os
from typing import Dict, Any, Optional, Union, TypeVar
from pathlib import Path

from .models import FrameworkSettings
from .exceptions import ConfigurationError

# Type variable for generic configuration access
T = TypeVar('T')

class Config:
    """
    Configuration manager using Pydantic BaseSettings.
    
    This class wraps the Pydantic FrameworkSettings model to provide
    backward compatibility while leveraging Pydantic's validation
    and environment variable loading capabilities.
    """
    
    def __init__(self) -> None:
        """Initialize configuration with Pydantic validation."""
        try:
            self._settings = FrameworkSettings()
        except Exception as e:
            raise ConfigurationError(
                f"Failed to load configuration: {e}",
                {'error': str(e)}
            )

    @property
    def settings(self) -> FrameworkSettings:
        """Get the underlying Pydantic settings model."""
        return self._settings
    
    def reload(self) -> None:
        """Reload configuration from environment variables."""
        try:
            self._settings = FrameworkSettings()
        except Exception as e:
            raise ConfigurationError(
                f"Failed to reload configuration: {e}",
                {'error': str(e)}
            )

    def get(self, key: str, default: Optional[T] = None) -> Union[T, Any]:
        """
        Get a configuration value from the Pydantic model.
        
        Args:
            key: Configuration key (case-insensitive)
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        # Convert key to lowercase to match Pydantic field names
        key_lower = key.lower()
        
        # Try to get the value from the Pydantic model
        if hasattr(self._settings, key_lower):
            return getattr(self._settings, key_lower)
        
        # Fallback to default
        return default
    
    def get_str(self, key: str, default: str = '') -> str:
        """Get a string configuration value."""
        value = self.get(key, default)
        return str(value) if value is not None else default
    
    def get_int(self, key: str, default: int = 0) -> int:
        """Get an integer configuration value."""
        value = self.get(key, default)
        if isinstance(value, int):
            return value
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """Get a boolean configuration value."""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', '1', 'yes', 'on')
        return bool(value) if value is not None else default

    @property
    def log_level(self) -> str:
        """Get the logging level."""
        return self._settings.log_level

    @property
    def log_dir(self) -> str:
        """Get the log directory."""
        return self._settings.log_dir
    
    @property
    def log_file(self) -> Optional[str]:
        """Get the log file path."""
        return self._settings.log_file

    @property
    def timeout(self) -> int:
        """Get the request timeout."""
        return self._settings.timeout

    @property
    def batch_size(self) -> int:
        """Get the batch size."""
        return self._settings.batch_size

    def get_clickhouse_config(self) -> Dict[str, Any]:
        """
        Get ClickHouse configuration as a dictionary.
        
        Returns:
            Dictionary with ClickHouse connection parameters
        """
        return {
            'host': self._settings.clickhouse_host,
            'port': self._settings.clickhouse_port,
            'user': self._settings.clickhouse_user,
            'password': self._settings.clickhouse_password,
            'database': self._settings.clickhouse_database,
            'timeout': self._settings.clickhouse_timeout
        }

    def get_api_config(self) -> Optional[Dict[str, Any]]:
        """
        Get generic API configuration.
        
        Returns:
            Dictionary with API configuration or None if not configured
        """
        if not self._settings.api_base_url:
            return None
            
        return {
            'base_url': self._settings.api_base_url,
            'api_key': self._settings.api_key,
            'timeout': self._settings.timeout
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of configuration
        """
        return self._settings.model_dump()
    
    def update(self, **kwargs) -> None:
        """
        Update configuration values.
        
        Args:
            **kwargs: Configuration values to update
            
        Raises:
            ConfigurationError: If validation fails
        """
        try:
            # Create new settings with updated values
            current_dict = self._settings.model_dump()
            current_dict.update(kwargs)
            self._settings = FrameworkSettings(**current_dict)
        except Exception as e:
            raise ConfigurationError(
                f"Failed to update configuration: {e}",
                {'error': str(e), 'kwargs': kwargs}
            )


# Global configuration instance
config = Config()