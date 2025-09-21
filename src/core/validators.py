# src/core/validators.py
"""
Validation utilities for the data processing framework.

This module provides validation functions for configuration, data,
and other framework components to ensure data integrity and
proper error handling.
"""

import re
import os
from typing import Any, Dict, List, Optional, Union, Callable
from urllib.parse import urlparse
import pandas as pd

from .constants import TIME_SCOPES, LOG_LEVELS, ENV_VARS
from .exceptions import ValidationError


def validate_config(config: Dict[str, Any], required_keys: List[str]) -> None:
    """
    Validate configuration dictionary.
    
    Args:
        config: Configuration dictionary to validate
        required_keys: List of required configuration keys
        
    Raises:
        ValidationError: If validation fails
    """
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValidationError(
            f"Missing required configuration keys: {missing_keys}",
            {'missing_keys': missing_keys, 'provided_keys': list(config.keys())}
        )
    
    # Validate specific configuration values
    if 'LOG_LEVEL' in config:
        validate_log_level(config['LOG_LEVEL'])
    
    if 'TIMEOUT' in config:
        validate_positive_number(config['TIMEOUT'], 'TIMEOUT')
    
    if 'BATCH_SIZE' in config:
        validate_positive_integer(config['BATCH_SIZE'], 'BATCH_SIZE')


def validate_log_level(level: str) -> None:
    """
    Validate log level.
    
    Args:
        level: Log level to validate
        
    Raises:
        ValidationError: If log level is invalid
    """
    if level.upper() not in LOG_LEVELS:
        raise ValidationError(
            f"Invalid log level: {level}. Must be one of: {LOG_LEVELS}",
            {'provided_level': level, 'valid_levels': LOG_LEVELS}
        )


def validate_time_scope(scope: str) -> None:
    """
    Validate time scope.
    
    Args:
        scope: Time scope to validate
        
    Raises:
        ValidationError: If time scope is invalid
    """
    if scope not in TIME_SCOPES:
        raise ValidationError(
            f"Invalid time scope: {scope}. Must be one of: {TIME_SCOPES}",
            {'provided_scope': scope, 'valid_scopes': TIME_SCOPES}
        )


def validate_positive_number(value: Union[int, float], name: str) -> None:
    """
    Validate that a value is a positive number.
    
    Args:
        value: Value to validate
        name: Name of the value for error messages
        
    Raises:
        ValidationError: If value is not a positive number
    """
    if not isinstance(value, (int, float)) or value <= 0:
        raise ValidationError(
            f"{name} must be a positive number, got: {value}",
            {'name': name, 'value': value, 'type': type(value).__name__}
        )


def validate_positive_integer(value: int, name: str) -> None:
    """
    Validate that a value is a positive integer.
    
    Args:
        value: Value to validate
        name: Name of the value for error messages
        
    Raises:
        ValidationError: If value is not a positive integer
    """
    if not isinstance(value, int) or value <= 0:
        raise ValidationError(
            f"{name} must be a positive integer, got: {value}",
            {'name': name, 'value': value, 'type': type(value).__name__}
        )


def validate_url(url: str, name: str = "URL") -> None:
    """
    Validate URL format.
    
    Args:
        url: URL to validate
        name: Name of the URL for error messages
        
    Raises:
        ValidationError: If URL is invalid
    """
    try:
        result = urlparse(url)
        if not all([result.scheme, result.netloc]):
            raise ValidationError(
                f"Invalid {name}: {url}. Must include scheme and netloc",
                {'url': url, 'parsed': result._asdict()}
            )
    except Exception as e:
        raise ValidationError(
            f"Invalid {name}: {url}. Error: {e}",
            {'url': url, 'error': str(e)}
        )


def validate_file_path(path: str, must_exist: bool = True) -> None:
    """
    Validate file path.
    
    Args:
        path: File path to validate
        must_exist: Whether the file must exist
        
    Raises:
        ValidationError: If file path is invalid
    """
    if not isinstance(path, str) or not path.strip():
        raise ValidationError(
            f"File path must be a non-empty string, got: {path}",
            {'path': path, 'type': type(path).__name__}
        )
    
    if must_exist and not os.path.exists(path):
        raise ValidationError(
            f"File does not exist: {path}",
            {'path': path, 'exists': False}
        )


def validate_directory_path(path: str, must_exist: bool = True, create_if_missing: bool = False) -> None:
    """
    Validate directory path.
    
    Args:
        path: Directory path to validate
        must_exist: Whether the directory must exist
        create_if_missing: Whether to create directory if missing
        
    Raises:
        ValidationError: If directory path is invalid
    """
    if not isinstance(path, str) or not path.strip():
        raise ValidationError(
            f"Directory path must be a non-empty string, got: {path}",
            {'path': path, 'type': type(path).__name__}
        )
    
    if not os.path.exists(path):
        if create_if_missing:
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                raise ValidationError(
                    f"Failed to create directory: {path}. Error: {e}",
                    {'path': path, 'error': str(e)}
                )
        elif must_exist:
            raise ValidationError(
                f"Directory does not exist: {path}",
                {'path': path, 'exists': False}
            )


def validate_dataframe(df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> None:
    """
    Validate pandas DataFrame.
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Raises:
        ValidationError: If DataFrame is invalid
    """
    if not isinstance(df, pd.DataFrame):
        raise ValidationError(
            f"Expected pandas DataFrame, got: {type(df).__name__}",
            {'type': type(df).__name__}
        )
    
    if df.empty:
        raise ValidationError(
            "DataFrame is empty",
            {'shape': df.shape}
        )
    
    if required_columns:
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValidationError(
                f"DataFrame missing required columns: {missing_columns}",
                {
                    'missing_columns': missing_columns,
                    'available_columns': list(df.columns),
                    'shape': df.shape
                }
            )


def validate_cron_expression(expression: str) -> None:
    """
    Validate cron expression format.
    
    Args:
        expression: Cron expression to validate
        
    Raises:
        ValidationError: If cron expression is invalid
    """
    if not isinstance(expression, str) or not expression.strip():
        raise ValidationError(
            f"Cron expression must be a non-empty string, got: {expression}",
            {'expression': expression, 'type': type(expression).__name__}
        )
    
    parts = expression.strip().split()
    if len(parts) != 5:
        raise ValidationError(
            f"Cron expression must have 5 parts, got {len(parts)}: {expression}",
            {'expression': expression, 'parts': parts}
        )
    
    # Basic validation of each part (simplified)
    patterns = [
        r'^(\*|[0-9]|[1-5][0-9]|[0-9]/[0-9]+|\*/[0-9]+|[0-9]-[0-9]+)$',  # minute
        r'^(\*|[0-9]|1[0-9]|2[0-3]|[0-9]/[0-9]+|\*/[0-9]+|[0-9]-[0-9]+)$',  # hour
        r'^(\*|[1-9]|[12][0-9]|3[01]|[0-9]/[0-9]+|\*/[0-9]+|[0-9]-[0-9]+)$',  # day
        r'^(\*|[1-9]|1[0-2]|[0-9]/[0-9]+|\*/[0-9]+|[0-9]-[0-9]+)$',  # month
        r'^(\*|[0-6]|[0-9]/[0-9]+|\*/[0-9]+|[0-9]-[0-9]+)$',  # day of week
    ]
    
    for i, (part, pattern) in enumerate(zip(parts, patterns)):
        if not re.match(pattern, part):
            field_names = ['minute', 'hour', 'day', 'month', 'day_of_week']
            raise ValidationError(
                f"Invalid cron {field_names[i]} field: {part}",
                {'expression': expression, 'field': field_names[i], 'value': part}
            )


def validate_environment_variables(required_vars: Optional[List[str]] = None) -> None:
    """
    Validate environment variables.
    
    Args:
        required_vars: List of required environment variable names
        
    Raises:
        ValidationError: If required environment variables are missing
    """
    if not required_vars:
        required_vars = [
            ENV_VARS['CLICKHOUSE_HOST'],
            ENV_VARS['CLICKHOUSE_USER'],
            ENV_VARS['CLICKHOUSE_PASSWORD'],
            ENV_VARS['CLICKHOUSE_DATABASE'],
        ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValidationError(
            f"Missing required environment variables: {missing_vars}",
            {'missing_vars': missing_vars, 'required_vars': required_vars}
        )


def validate_with_custom_function(value: Any, validator: Callable[[Any], bool], name: str, error_message: str = None) -> None:
    """
    Validate value with custom validation function.
    
    Args:
        value: Value to validate
        validator: Validation function that returns True if valid
        name: Name of the value for error messages
        error_message: Custom error message
        
    Raises:
        ValidationError: If validation fails
    """
    try:
        if not validator(value):
            message = error_message or f"Validation failed for {name}: {value}"
            raise ValidationError(
                message,
                {'name': name, 'value': value}
            )
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(
            f"Validation error for {name}: {e}",
            {'name': name, 'value': value, 'error': str(e)}
        )


# Common validators
def is_valid_email(email: str) -> bool:
    """Check if email format is valid."""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def is_valid_port(port: Union[int, str]) -> bool:
    """Check if port number is valid."""
    try:
        port_num = int(port)
        return 1 <= port_num <= 65535
    except (ValueError, TypeError):
        return False


def is_valid_host(host: str) -> bool:
    """Check if host format is valid."""
    if not isinstance(host, str) or not host.strip():
        return False
    
    # Check if it's an IP address
    ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
    if re.match(ip_pattern, host):
        parts = host.split('.')
        return all(0 <= int(part) <= 255 for part in parts)
    
    # Check if it's a valid hostname
    hostname_pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
    return re.match(hostname_pattern, host) is not None
