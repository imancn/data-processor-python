# src/core/exceptions.py
"""
Custom exception classes for the data processing framework.

This module defines a hierarchy of custom exceptions that provide
better error handling and debugging capabilities throughout the
framework.
"""

from typing import Optional, Dict, Any


class FrameworkError(Exception):
    """Base exception class for all framework-related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Initialize framework error.
        
        Args:
            message: Error message
            details: Additional error details
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
    
    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(FrameworkError):
    """Raised when there's an issue with configuration."""
    pass


class DatabaseError(FrameworkError):
    """Base class for database-related errors."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass


class DatabaseQueryError(DatabaseError):
    """Raised when database query fails."""
    pass


class PipelineError(FrameworkError):
    """Base class for pipeline-related errors."""
    pass


class PipelineNotFoundError(PipelineError):
    """Raised when a requested pipeline is not found."""
    pass


class PipelineExecutionError(PipelineError):
    """Raised when pipeline execution fails."""
    pass


class PipelineConfigurationError(PipelineError):
    """Raised when pipeline configuration is invalid."""
    pass


class ExtractionError(FrameworkError):
    """Raised when data extraction fails."""
    pass


class HTTPExtractionError(ExtractionError):
    """Raised when HTTP data extraction fails."""
    pass


class DatabaseExtractionError(ExtractionError):
    """Raised when database extraction fails."""
    pass


class TransformationError(FrameworkError):
    """Raised when data transformation fails."""
    pass


class ValidationError(TransformationError):
    """Raised when data validation fails."""
    pass


class LoadingError(FrameworkError):
    """Raised when data loading fails."""
    pass


class DatabaseLoadingError(LoadingError):
    """Raised when database loading fails."""
    pass


class MigrationError(FrameworkError):
    """Base class for migration-related errors."""
    pass


class MigrationNotFoundError(MigrationError):
    """Raised when a migration file is not found."""
    pass


class MigrationExecutionError(MigrationError):
    """Raised when migration execution fails."""
    pass


class CronError(FrameworkError):
    """Base class for cron-related errors."""
    pass


class CronJobNotFoundError(CronError):
    """Raised when a cron job is not found."""
    pass


class CronScheduleError(CronError):
    """Raised when cron schedule is invalid."""
    pass


class ValidationError(FrameworkError):
    """Raised when validation fails."""
    pass


class TimeoutError(FrameworkError):
    """Raised when an operation times out."""
    pass


class RetryError(FrameworkError):
    """Raised when all retry attempts fail."""
    
    def __init__(self, message: str, attempts: int, last_error: Exception, details: Optional[Dict[str, Any]] = None):
        """
        Initialize retry error.
        
        Args:
            message: Error message
            attempts: Number of attempts made
            last_error: The last error that occurred
            details: Additional error details
        """
        super().__init__(message, details)
        self.attempts = attempts
        self.last_error = last_error
    
    def __str__(self) -> str:
        base_msg = f"{self.message} (after {self.attempts} attempts)"
        if self.last_error:
            base_msg += f" | Last error: {self.last_error}"
        if self.details:
            base_msg += f" | Details: {self.details}"
        return base_msg


# Exception mapping for better error categorization
EXCEPTION_MAPPING = {
    'config': ConfigurationError,
    'database': DatabaseError,
    'database_connection': DatabaseConnectionError,
    'database_query': DatabaseQueryError,
    'pipeline': PipelineError,
    'pipeline_not_found': PipelineNotFoundError,
    'pipeline_execution': PipelineExecutionError,
    'pipeline_config': PipelineConfigurationError,
    'extraction': ExtractionError,
    'http_extraction': HTTPExtractionError,
    'database_extraction': DatabaseExtractionError,
    'transformation': TransformationError,
    'validation': ValidationError,
    'loading': LoadingError,
    'database_loading': DatabaseLoadingError,
    'migration': MigrationError,
    'migration_not_found': MigrationNotFoundError,
    'migration_execution': MigrationExecutionError,
    'cron': CronError,
    'cron_job_not_found': CronJobNotFoundError,
    'cron_schedule': CronScheduleError,
    'timeout': TimeoutError,
    'retry': RetryError,
}


def get_exception_class(error_type: str) -> type:
    """
    Get exception class by error type.
    
    Args:
        error_type: Type of error
        
    Returns:
        Exception class
        
    Raises:
        ValueError: If error type is not found
    """
    if error_type not in EXCEPTION_MAPPING:
        raise ValueError(f"Unknown error type: {error_type}")
    return EXCEPTION_MAPPING[error_type]


def create_exception(error_type: str, message: str, details: Optional[Dict[str, Any]] = None) -> FrameworkError:
    """
    Create exception instance by error type.
    
    Args:
        error_type: Type of error
        message: Error message
        details: Additional error details
        
    Returns:
        Exception instance
        
    Example:
        >>> exc = create_exception('database_connection', 'Failed to connect', {'host': 'localhost'})
        >>> raise exc
    """
    try:
        exception_class = get_exception_class(error_type)
        return exception_class(message, details)
    except ValueError:
        # Fall back to FrameworkError for unknown types
        return FrameworkError(message, details)
