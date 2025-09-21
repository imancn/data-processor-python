# src/core/__init__.py
"""
Core utilities for the data processing framework.

This package provides essential utilities for configuration management,
logging, validation, and other core functionality used throughout the
data processing framework.

Example:
    >>> from core import config, setup_logging, log_with_timestamp
    >>> setup_logging('INFO')
    >>> log_with_timestamp('Application started', 'Main')
"""

from .config import config
from .logging import (
    setup_logging, log_with_timestamp, get_logger,
    LoggingContext, PerformanceLogger,
    log_function_call, log_pipeline_stage,
    create_job_logger
)
from .constants import (
    FRAMEWORK_NAME, FRAMEWORK_VERSION, DEFAULT_CONFIG,
    TIME_SCOPES, STATUS_CODES, ERROR_MESSAGES, SUCCESS_MESSAGES
)
from .exceptions import (
    FrameworkError, ConfigurationError, DatabaseError,
    PipelineError, ExtractionError, TransformationError,
    LoadingError, MigrationError, ValidationError
)
from .validators import (
    validate_config as validate_config_legacy, validate_log_level, validate_time_scope,
    validate_url, validate_file_path, validate_dataframe as validate_dataframe_legacy
)
from .pydantic_validators import (
    PydanticValidator, validate_config, validate_pipeline_config,
    validate_extractor_config, validate_transformer_config, validate_loader_config,
    validate_database_config, validate_api_config, validate_dataframe,
    validate_job_execution, safe_validate
)
from .models import (
    FrameworkSettings, PipelineConfig, PipelineData,
    ExtractorConfig, TransformerConfig, LoaderConfig,
    JobExecution, DatabaseConfig, APIConfig, ValidationResult, DataFrameInfo
)

# Package metadata
__version__ = FRAMEWORK_VERSION
__author__ = "Data Processing Framework Team"
__email__ = "support@dataprocessor.com"
__description__ = "Enterprise-grade data processing framework"

# Public API
__all__ = [
    # Core functionality
    'config',
    'setup_logging', 
    'log_with_timestamp',
    'get_logger',
    
    # Advanced logging
    'LoggingContext',
    'PerformanceLogger',
    'log_function_call',
    'log_pipeline_stage',
    'create_job_logger',
    
    # Constants
    'FRAMEWORK_NAME',
    'FRAMEWORK_VERSION',
    'DEFAULT_CONFIG',
    'TIME_SCOPES',
    'STATUS_CODES',
    'ERROR_MESSAGES',
    'SUCCESS_MESSAGES',
    
    # Exceptions
    'FrameworkError',
    'ConfigurationError',
    'DatabaseError',
    'PipelineError',
    'ExtractionError',
    'TransformationError',
    'LoadingError',
    'MigrationError',
    'ValidationError',
    
    # Legacy Validators
    'validate_config_legacy',
    'validate_log_level',
    'validate_time_scope',
    'validate_url',
    'validate_file_path',
    'validate_dataframe_legacy',
    
    # Pydantic Validators
    'PydanticValidator',
    'validate_config',
    'validate_pipeline_config',
    'validate_extractor_config',
    'validate_transformer_config',
    'validate_loader_config',
    'validate_database_config',
    'validate_api_config',
    'validate_dataframe',
    'validate_job_execution',
    'safe_validate',
    
    # Pydantic Models
    'FrameworkSettings',
    'PipelineConfig',
    'PipelineData',
    'ExtractorConfig',
    'TransformerConfig',
    'LoaderConfig',
    'JobExecution',
    'DatabaseConfig',
    'APIConfig',
    'ValidationResult',
    'DataFrameInfo',
    
    # Metadata
    '__version__'
]

# Initialize logging when core is imported
try:
    log_level = config.get('LOG_LEVEL', 'INFO')
    setup_logging(log_level)
    log_with_timestamp(f"Core package v{__version__} initialized", "Core", "info")
except Exception as e:
    # Fallback to basic logging if setup fails
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Core package initialized with fallback logging: {e}")