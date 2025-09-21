# src/core/models.py
"""
Pydantic models for data validation throughout the framework.

This module provides Pydantic models for configuration, pipeline data,
and other framework components to ensure type safety and data validation.
"""

from typing import Dict, Any, List, Optional, Union, Literal
from datetime import datetime
from pathlib import Path
import os

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from pydantic_settings import BaseSettings

from .constants import TIME_SCOPES, LOG_LEVELS, STATUS_CODES


class FrameworkSettings(BaseSettings):
    """
    Framework configuration using Pydantic BaseSettings.
    
    This automatically loads from environment variables and validates types.
    """
    
    # Logging configuration
    log_level: str = Field(default='INFO', description="Logging level")
    log_dir: str = Field(default='logs', description="Directory for log files")
    log_file: Optional[str] = Field(default=None, description="Main log file path")
    
    # Application configuration
    timeout: int = Field(default=30, ge=1, le=300, description="Request timeout in seconds")
    batch_size: int = Field(default=1000, ge=1, description="Batch size for processing")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")
    retry_delay: float = Field(default=1.0, ge=0.1, description="Delay between retries")
    retry_backoff: float = Field(default=2.0, ge=1.0, description="Retry backoff multiplier")
    
    # ClickHouse configuration
    clickhouse_host: str = Field(default='localhost', description="ClickHouse host")
    clickhouse_port: int = Field(default=8123, ge=1, le=65535, description="ClickHouse port")
    clickhouse_user: str = Field(default='default', description="ClickHouse username")
    clickhouse_password: str = Field(default='', description="ClickHouse password")
    clickhouse_database: str = Field(default='data_warehouse', description="ClickHouse database")
    clickhouse_timeout: int = Field(default=30, ge=1, description="ClickHouse connection timeout")
    
    # Generic API configuration
    api_key: Optional[str] = Field(default=None, description="Generic API key")
    api_base_url: Optional[str] = Field(default=None, description="Generic API base URL")
    
    @field_validator('log_level')
    @classmethod
    def validate_log_level(cls, v):
        """Validate log level is supported."""
        if v.upper() not in LOG_LEVELS:
            raise ValueError(f"Log level must be one of: {LOG_LEVELS}")
        return v.upper()
    
    @field_validator('log_dir')
    @classmethod
    def validate_log_dir(cls, v):
        """Ensure log directory exists."""
        if v:
            os.makedirs(v, exist_ok=True)
        return v
    
    @field_validator('api_base_url')
    @classmethod
    def validate_api_base_url(cls, v):
        """Validate API base URL format."""
        if v and not v.startswith(('http://', 'https://')):
            raise ValueError("API base URL must start with http:// or https://")
        return v
    
    model_config = ConfigDict(
        env_file='.env',
        case_sensitive=False,
        # Environment variable mapping is handled automatically by field names
    )


class PipelineConfig(BaseModel):
    """Configuration model for pipeline definitions."""
    
    name: str = Field(..., min_length=1, description="Pipeline name")
    description: Optional[str] = Field(default=None, description="Pipeline description")
    schedule: str = Field(default="0 * * * *", description="Cron schedule expression")
    enabled: bool = Field(default=True, description="Whether pipeline is enabled")
    timeout: Optional[int] = Field(default=None, ge=1, description="Pipeline timeout in seconds")
    retry_count: int = Field(default=3, ge=0, description="Number of retries on failure")
    time_scope: Optional[str] = Field(default=None, description="Time scope for data processing")
    
    @field_validator('schedule')
    @classmethod
    def validate_cron_schedule(cls, v):
        """Validate cron schedule format."""
        parts = v.strip().split()
        if len(parts) != 5:
            raise ValueError("Cron schedule must have 5 parts (minute hour day month day_of_week)")
        return v
    
    @field_validator('time_scope')
    @classmethod
    def validate_time_scope(cls, v):
        """Validate time scope is supported."""
        if v and v not in TIME_SCOPES:
            raise ValueError(f"Time scope must be one of: {TIME_SCOPES}")
        return v


class PipelineData(BaseModel):
    """Model for pipeline data and metadata."""
    
    pipeline: Any = Field(..., description="Pipeline function or callable")
    config: PipelineConfig = Field(..., description="Pipeline configuration")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")
    
    model_config = ConfigDict(arbitrary_types_allowed=True)  # Allow callable types


class ExtractorConfig(BaseModel):
    """Configuration model for data extractors."""
    
    type: Literal['http', 'clickhouse', 'file'] = Field(..., description="Extractor type")
    url: Optional[str] = Field(default=None, description="URL for HTTP extractor")
    headers: Optional[Dict[str, str]] = Field(default=None, description="HTTP headers")
    query: Optional[str] = Field(default=None, description="SQL query for database extractor")
    file_path: Optional[str] = Field(default=None, description="File path for file extractor")
    timeout: Optional[int] = Field(default=30, ge=1, description="Request timeout")
    
    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        """Validate URL for HTTP extractors."""
        if v and not v.startswith(('http://', 'https://')):
            raise ValueError("URL must start with http:// or https://")
        return v
    
    @field_validator('query')
    @classmethod
    def validate_query(cls, v):
        """Validate query for database extractors."""
        return v
    
    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, v):
        """Validate file path for file extractors."""
        return v
    
    @model_validator(mode='after')
    def validate_extractor_requirements(self):
        """Validate extractor-specific requirements."""
        if self.type == 'http' and not self.url:
            raise ValueError("URL is required for HTTP extractor")
        if self.type == 'clickhouse' and not self.query:
            raise ValueError("Query is required for ClickHouse extractor")
        if self.type == 'file' and not self.file_path:
            raise ValueError("File path is required for file extractor")
        return self


class TransformerConfig(BaseModel):
    """Configuration model for data transformers."""
    
    type: Literal['lambda', 'pandas', 'custom'] = Field(..., description="Transformer type")
    function: Optional[Any] = Field(default=None, description="Transform function")
    operations: Optional[List[str]] = Field(default=None, description="List of operations")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Transform parameters")
    
    model_config = ConfigDict(arbitrary_types_allowed=True)


class LoaderConfig(BaseModel):
    """Configuration model for data loaders."""
    
    type: Literal['clickhouse', 'console', 'file'] = Field(..., description="Loader type")
    table: Optional[str] = Field(default=None, description="Table name for database loader")
    columns: Optional[List[str]] = Field(default=None, description="Column names")
    file_path: Optional[str] = Field(default=None, description="File path for file loader")
    mode: Optional[str] = Field(default='insert', description="Loading mode (insert, upsert)")
    batch_size: Optional[int] = Field(default=1000, ge=1, description="Batch size")
    
    @field_validator('table')
    @classmethod
    def validate_table(cls, v):
        """Validate table for database loaders."""
        return v
    
    @model_validator(mode='after')
    def validate_loader_requirements(self):
        """Validate loader-specific requirements."""
        if self.type == 'clickhouse' and not self.table:
            raise ValueError("Table name is required for ClickHouse loader")
        return self


class JobExecution(BaseModel):
    """Model for job execution tracking."""
    
    job_name: str = Field(..., description="Job name")
    pipeline_name: str = Field(..., description="Pipeline name")
    started_at: datetime = Field(default_factory=datetime.utcnow, description="Start time")
    completed_at: Optional[datetime] = Field(default=None, description="Completion time")
    status: str = Field(default='running', description="Execution status")
    records_processed: Optional[int] = Field(default=None, ge=0, description="Number of records processed")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    execution_time: Optional[float] = Field(default=None, ge=0, description="Execution time in seconds")
    
    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        """Validate execution status."""
        valid_statuses = ['running', 'completed', 'failed', 'cancelled']
        if v not in valid_statuses:
            raise ValueError(f"Status must be one of: {valid_statuses}")
        return v


class DatabaseConfig(BaseModel):
    """Database connection configuration."""
    
    host: str = Field(..., description="Database host")
    port: int = Field(..., ge=1, le=65535, description="Database port")
    user: str = Field(..., description="Database user")
    password: str = Field(..., description="Database password", repr=False)
    database: str = Field(..., description="Database name")
    timeout: int = Field(default=30, ge=1, description="Connection timeout")
    
    model_config = ConfigDict()


class APIConfig(BaseModel):
    """API configuration model."""
    
    base_url: str = Field(..., description="API base URL")
    api_key: Optional[str] = Field(default=None, description="API key")
    timeout: int = Field(default=30, ge=1, description="Request timeout")
    max_retries: int = Field(default=3, ge=0, description="Maximum retries")
    headers: Optional[Dict[str, str]] = Field(default=None, description="Default headers")
    
    @field_validator('base_url')
    @classmethod
    def validate_base_url(cls, v):
        """Validate base URL format."""
        if not v.startswith(('http://', 'https://')):
            raise ValueError("Base URL must start with http:// or https://")
        return v.rstrip('/')  # Remove trailing slash


class ValidationResult(BaseModel):
    """Result of validation operations."""
    
    is_valid: bool = Field(..., description="Whether validation passed")
    errors: List[str] = Field(default_factory=list, description="List of validation errors")
    warnings: List[str] = Field(default_factory=list, description="List of validation warnings")
    data: Optional[Dict[str, Any]] = Field(default=None, description="Validated data")
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
    
    @property
    def has_errors(self) -> bool:
        """Check if there are any errors."""
        return len(self.errors) > 0
    
    @property
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0


class DataFrameInfo(BaseModel):
    """Information about a pandas DataFrame."""
    
    shape: tuple = Field(..., description="DataFrame shape (rows, columns)")
    columns: List[str] = Field(..., description="Column names")
    dtypes: Dict[str, str] = Field(..., description="Column data types")
    memory_usage: int = Field(..., ge=0, description="Memory usage in bytes")
    has_nulls: bool = Field(..., description="Whether DataFrame has null values")
    null_counts: Dict[str, int] = Field(default_factory=dict, description="Null count per column")
    
    @classmethod
    def from_dataframe(cls, df):
        """Create DataFrameInfo from pandas DataFrame."""
        try:
            import pandas as pd
            
            if not isinstance(df, pd.DataFrame):
                raise ValueError("Input must be a pandas DataFrame")
            
            return cls(
                shape=df.shape,
                columns=list(df.columns),
                dtypes={col: str(dtype) for col, dtype in df.dtypes.items()},
                memory_usage=df.memory_usage(deep=True).sum(),
                has_nulls=df.isnull().any().any(),
                null_counts=df.isnull().sum().to_dict()
            )
        except ImportError:
            raise ValueError("pandas is required to create DataFrameInfo")


# Model registry for dynamic model creation
MODEL_REGISTRY = {
    'framework_settings': FrameworkSettings,
    'pipeline_config': PipelineConfig,
    'pipeline_data': PipelineData,
    'extractor_config': ExtractorConfig,
    'transformer_config': TransformerConfig,
    'loader_config': LoaderConfig,
    'job_execution': JobExecution,
    'database_config': DatabaseConfig,
    'api_config': APIConfig,
    'validation_result': ValidationResult,
    'dataframe_info': DataFrameInfo,
}


def get_model(model_name: str) -> BaseModel:
    """
    Get a Pydantic model by name.
    
    Args:
        model_name: Name of the model
        
    Returns:
        Pydantic model class
        
    Raises:
        ValueError: If model name is not found
    """
    if model_name not in MODEL_REGISTRY:
        raise ValueError(f"Unknown model: {model_name}. Available models: {list(MODEL_REGISTRY.keys())}")
    return MODEL_REGISTRY[model_name]


def validate_data(data: Dict[str, Any], model_name: str) -> ValidationResult:
    """
    Validate data using a Pydantic model.
    
    Args:
        data: Data to validate
        model_name: Name of the model to use for validation
        
    Returns:
        ValidationResult with validation details
    """
    try:
        model_class = get_model(model_name)
        validated_data = model_class(**data)
        
        return ValidationResult(
            is_valid=True,
            data=validated_data.model_dump()
        )
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            errors=[str(e)]
        )
