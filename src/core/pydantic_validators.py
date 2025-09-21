# src/core/pydantic_validators.py
"""
Pydantic-based validation utilities for the data processing framework.

This module replaces the custom validation system with Pydantic models
for better type safety, automatic validation, and improved developer experience.
"""

from typing import Dict, Any, List, Optional, Union, Type, TypeVar
import pandas as pd
from pathlib import Path

from pydantic import BaseModel, ValidationError as PydanticValidationError

from .models import (
    FrameworkSettings, PipelineConfig, PipelineData,
    ExtractorConfig, TransformerConfig, LoaderConfig,
    JobExecution, DatabaseConfig, APIConfig,
    ValidationResult, DataFrameInfo, get_model, validate_data
)
from .exceptions import ValidationError

T = TypeVar('T', bound=BaseModel)


class PydanticValidator:
    """
    Pydantic-based validator for framework components.
    
    This class provides a unified interface for validating different
    types of data using Pydantic models.
    """
    
    @staticmethod
    def validate_config(config_data: Dict[str, Any]) -> FrameworkSettings:
        """
        Validate framework configuration using Pydantic.
        
        Args:
            config_data: Configuration dictionary
            
        Returns:
            Validated FrameworkSettings instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return FrameworkSettings(**config_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Configuration validation failed: {e}",
                {'errors': e.errors(), 'config_data': config_data}
            )
    
    @staticmethod
    def validate_pipeline_config(pipeline_data: Dict[str, Any]) -> PipelineConfig:
        """
        Validate pipeline configuration.
        
        Args:
            pipeline_data: Pipeline configuration dictionary
            
        Returns:
            Validated PipelineConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return PipelineConfig(**pipeline_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Pipeline configuration validation failed: {e}",
                {'errors': e.errors(), 'pipeline_data': pipeline_data}
            )
    
    @staticmethod
    def validate_extractor_config(extractor_data: Dict[str, Any]) -> ExtractorConfig:
        """
        Validate extractor configuration.
        
        Args:
            extractor_data: Extractor configuration dictionary
            
        Returns:
            Validated ExtractorConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return ExtractorConfig(**extractor_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Extractor configuration validation failed: {e}",
                {'errors': e.errors(), 'extractor_data': extractor_data}
            )
    
    @staticmethod
    def validate_transformer_config(transformer_data: Dict[str, Any]) -> TransformerConfig:
        """
        Validate transformer configuration.
        
        Args:
            transformer_data: Transformer configuration dictionary
            
        Returns:
            Validated TransformerConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return TransformerConfig(**transformer_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Transformer configuration validation failed: {e}",
                {'errors': e.errors(), 'transformer_data': transformer_data}
            )
    
    @staticmethod
    def validate_loader_config(loader_data: Dict[str, Any]) -> LoaderConfig:
        """
        Validate loader configuration.
        
        Args:
            loader_data: Loader configuration dictionary
            
        Returns:
            Validated LoaderConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return LoaderConfig(**loader_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Loader configuration validation failed: {e}",
                {'errors': e.errors(), 'loader_data': loader_data}
            )
    
    @staticmethod
    def validate_database_config(db_data: Dict[str, Any]) -> DatabaseConfig:
        """
        Validate database configuration.
        
        Args:
            db_data: Database configuration dictionary
            
        Returns:
            Validated DatabaseConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return DatabaseConfig(**db_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Database configuration validation failed: {e}",
                {'errors': e.errors(), 'db_data': db_data}
            )
    
    @staticmethod
    def validate_api_config(api_data: Dict[str, Any]) -> APIConfig:
        """
        Validate API configuration.
        
        Args:
            api_data: API configuration dictionary
            
        Returns:
            Validated APIConfig instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return APIConfig(**api_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"API configuration validation failed: {e}",
                {'errors': e.errors(), 'api_data': api_data}
            )
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> DataFrameInfo:
        """
        Validate pandas DataFrame and return information.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            DataFrameInfo with validation details
            
        Raises:
            ValidationError: If validation fails
        """
        try:
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
            
            return DataFrameInfo.from_dataframe(df)
            
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            raise ValidationError(
                f"DataFrame validation failed: {e}",
                {'error': str(e)}
            )
    
    @staticmethod
    def validate_job_execution(job_data: Dict[str, Any]) -> JobExecution:
        """
        Validate job execution data.
        
        Args:
            job_data: Job execution data dictionary
            
        Returns:
            Validated JobExecution instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return JobExecution(**job_data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Job execution validation failed: {e}",
                {'errors': e.errors(), 'job_data': job_data}
            )
    
    @staticmethod
    def validate_with_model(data: Dict[str, Any], model_class: Type[T]) -> T:
        """
        Validate data using a specific Pydantic model.
        
        Args:
            data: Data to validate
            model_class: Pydantic model class
            
        Returns:
            Validated model instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return model_class(**data)
        except PydanticValidationError as e:
            raise ValidationError(
                f"Validation failed for {model_class.__name__}: {e}",
                {'errors': e.errors(), 'data': data, 'model': model_class.__name__}
            )
    
    @staticmethod
    def validate_with_model_name(data: Dict[str, Any], model_name: str) -> BaseModel:
        """
        Validate data using a model name from the registry.
        
        Args:
            data: Data to validate
            model_name: Name of the model in the registry
            
        Returns:
            Validated model instance
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            model_class = get_model(model_name)
            return PydanticValidator.validate_with_model(data, model_class)
        except ValueError as e:
            raise ValidationError(
                f"Model validation failed: {e}",
                {'model_name': model_name, 'available_models': list(get_model.__globals__['MODEL_REGISTRY'].keys())}
            )
    
    @staticmethod
    def safe_validate(data: Dict[str, Any], model_name: str) -> ValidationResult:
        """
        Safely validate data and return detailed results.
        
        Args:
            data: Data to validate
            model_name: Name of the model to use
            
        Returns:
            ValidationResult with validation details
        """
        return validate_data(data, model_name)
    
    @staticmethod
    def validate_file_path(file_path: Union[str, Path], must_exist: bool = True, 
                          must_be_file: bool = True) -> Path:
        """
        Validate file path using Pydantic.
        
        Args:
            file_path: File path to validate
            must_exist: Whether the file must exist
            must_be_file: Whether the path must be a file (not directory)
            
        Returns:
            Validated Path object
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            path = Path(file_path)
            
            if must_exist and not path.exists():
                raise ValidationError(
                    f"File does not exist: {file_path}",
                    {'path': str(path), 'exists': False}
                )
            
            if must_exist and must_be_file and not path.is_file():
                raise ValidationError(
                    f"Path is not a file: {file_path}",
                    {'path': str(path), 'is_file': path.is_file(), 'is_dir': path.is_dir()}
                )
            
            return path
            
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            raise ValidationError(
                f"File path validation failed: {e}",
                {'file_path': str(file_path), 'error': str(e)}
            )
    
    @staticmethod
    def validate_directory_path(dir_path: Union[str, Path], must_exist: bool = True,
                               create_if_missing: bool = False) -> Path:
        """
        Validate directory path using Pydantic.
        
        Args:
            dir_path: Directory path to validate
            must_exist: Whether the directory must exist
            create_if_missing: Whether to create directory if missing
            
        Returns:
            Validated Path object
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            path = Path(dir_path)
            
            if not path.exists():
                if create_if_missing:
                    path.mkdir(parents=True, exist_ok=True)
                elif must_exist:
                    raise ValidationError(
                        f"Directory does not exist: {dir_path}",
                        {'path': str(path), 'exists': False}
                    )
            elif path.exists() and not path.is_dir():
                raise ValidationError(
                    f"Path exists but is not a directory: {dir_path}",
                    {'path': str(path), 'is_dir': path.is_dir(), 'is_file': path.is_file()}
                )
            
            return path
            
        except Exception as e:
            if isinstance(e, ValidationError):
                raise
            raise ValidationError(
                f"Directory path validation failed: {e}",
                {'dir_path': str(dir_path), 'error': str(e)}
            )


# Convenience functions for common validations
def validate_config(config_data: Dict[str, Any]) -> FrameworkSettings:
    """Validate framework configuration."""
    return PydanticValidator.validate_config(config_data)


def validate_pipeline_config(pipeline_data: Dict[str, Any]) -> PipelineConfig:
    """Validate pipeline configuration."""
    return PydanticValidator.validate_pipeline_config(pipeline_data)


def validate_extractor_config(extractor_data: Dict[str, Any]) -> ExtractorConfig:
    """Validate extractor configuration."""
    return PydanticValidator.validate_extractor_config(extractor_data)


def validate_transformer_config(transformer_data: Dict[str, Any]) -> TransformerConfig:
    """Validate transformer configuration."""
    return PydanticValidator.validate_transformer_config(transformer_data)


def validate_loader_config(loader_data: Dict[str, Any]) -> LoaderConfig:
    """Validate loader configuration."""
    return PydanticValidator.validate_loader_config(loader_data)


def validate_database_config(db_data: Dict[str, Any]) -> DatabaseConfig:
    """Validate database configuration."""
    return PydanticValidator.validate_database_config(db_data)


def validate_api_config(api_data: Dict[str, Any]) -> APIConfig:
    """Validate API configuration."""
    return PydanticValidator.validate_api_config(api_data)


def validate_dataframe(df: pd.DataFrame, required_columns: Optional[List[str]] = None) -> DataFrameInfo:
    """Validate pandas DataFrame."""
    return PydanticValidator.validate_dataframe(df, required_columns)


def validate_job_execution(job_data: Dict[str, Any]) -> JobExecution:
    """Validate job execution data."""
    return PydanticValidator.validate_job_execution(job_data)


def safe_validate(data: Dict[str, Any], model_name: str) -> ValidationResult:
    """Safely validate data and return results."""
    return PydanticValidator.safe_validate(data, model_name)
