# src/pipelines/__init__.py
"""
Generic pipeline framework for data processing.

This package provides a comprehensive framework for building, registering,
and executing data processing pipelines. It includes auto-discovery of
pipeline modules, factory patterns for creating pipeline components,
and a registry system for managing pipeline lifecycles.

Example:
    >>> from pipelines import register_pipeline, get_all_pipelines
    >>> from pipelines.pipeline_factory import create_etl_pipeline
    >>> 
    >>> # Create a simple pipeline
    >>> pipeline = create_etl_pipeline(
    ...     name="example_pipeline",
    ...     extractor=my_extractor,
    ...     transformer=my_transformer,
    ...     loader=my_loader
    ... )
    >>> 
    >>> # Register the pipeline
    >>> register_pipeline("example", {
    ...     "pipeline": pipeline,
    ...     "schedule": "0 * * * *",
    ...     "description": "Example pipeline"
    ... })
"""

import os
import importlib
from pathlib import Path
from typing import Dict, Any, Optional, List

from core.logging import log_with_timestamp, get_logger
from core.exceptions import PipelineError, PipelineNotFoundError
from core.constants import PIPELINE_PATTERNS

# Get logger for this module
logger = get_logger(__name__)

# Global pipeline registry
_pipeline_registry: Dict[str, Dict[str, Any]] = {}

# Import core pipeline components
from .pipeline_factory import (
    create_etl_pipeline, create_el_pipeline,
    create_parallel_pipeline, create_sequential_pipeline,
    create_conditional_pipeline, create_retry_pipeline
)
from .pipeline_registry import (
    register_pipeline as register_cron_job,
    list_cron_jobs,
    get_cron_job,
    unregister_pipeline as remove_cron_job,
    register_pipeline_with_timescope
)

def register_pipeline(name: str, pipeline_data: Dict[str, Any]) -> None:
    """
    Register a pipeline in the global registry.
    
    Args:
        name: Pipeline name
        pipeline_data: Pipeline configuration including 'pipeline', 'schedule', 'description'
        
    Example:
        >>> register_pipeline("my_pipeline", {
        ...     "pipeline": my_pipeline_function,
        ...     "schedule": "0 * * * *",
        ...     "description": "My data pipeline"
        ... })
    """
    _pipeline_registry[name] = pipeline_data
    log_with_timestamp(f"Registered pipeline: {name}", "Pipeline Registry")
    logger.info(f"Pipeline registered: {name} with schedule: {pipeline_data.get('schedule', 'N/A')}")


def get_all_pipelines() -> Dict[str, Dict[str, Any]]:
    """
    Get all registered pipelines.
    
    Returns:
        Dictionary of pipeline name to pipeline data
    """
    return _pipeline_registry.copy()


def get_pipeline(name: str) -> Optional[Dict[str, Any]]:
    """
    Get a specific pipeline by name.
    
    Args:
        name: Pipeline name
        
    Returns:
        Pipeline data or None if not found
    """
    return _pipeline_registry.get(name)


def remove_pipeline(name: str) -> bool:
    """
    Remove a pipeline from the registry.
    
    Args:
        name: Pipeline name
        
    Returns:
        True if pipeline was removed, False if not found
    """
    if name in _pipeline_registry:
        del _pipeline_registry[name]
        log_with_timestamp(f"Removed pipeline: {name}", "Pipeline Registry")
        return True
    return False


def list_pipeline_names() -> List[str]:
    """
    Get list of all registered pipeline names.
    
    Returns:
        List of pipeline names
    """
    return list(_pipeline_registry.keys())


def discover_and_load_pipelines() -> int:
    """
    Auto-discover and load all pipeline modules.
    
    Returns:
        Number of pipeline modules loaded
        
    Raises:
        PipelineError: If critical error occurs during discovery
    """
    current_dir = Path(__file__).parent
    pipeline_pattern = PIPELINE_PATTERNS['pipeline_file']
    
    pipeline_modules = []
    for file_path in current_dir.glob(pipeline_pattern):
        if file_path.name != '__init__.py':
            module_name = file_path.stem
            pipeline_modules.append(module_name)
    
    loaded_count = 0
    errors = []
    
    for module_name in pipeline_modules:
        try:
            module = importlib.import_module(f'pipelines.{module_name}')
            
            # Check if module has register_pipelines function
            if hasattr(module, 'register_pipelines'):
                module.register_pipelines()
                log_with_timestamp(f"Registered pipelines from {module_name}", "Pipeline Discovery")
            
            # Check if module has get_pipeline_registry function
            if hasattr(module, 'get_pipeline_registry'):
                registry = module.get_pipeline_registry()
                for pipeline_name, pipeline_data in registry.items():
                    register_pipeline(pipeline_name, pipeline_data)
            
            loaded_count += 1
            logger.info(f"Successfully loaded pipeline module: {module_name}")
            
        except ImportError as e:
            error_msg = f"Failed to import pipeline module {module_name}: {e}"
            errors.append(error_msg)
            log_with_timestamp(error_msg, "Pipeline Discovery", "error")
            logger.error(error_msg)
        except Exception as e:
            error_msg = f"Error loading pipeline module {module_name}: {e}"
            errors.append(error_msg)
            log_with_timestamp(error_msg, "Pipeline Discovery", "error")
            logger.error(error_msg)
    
    if pipeline_modules:
        log_with_timestamp(
            f"Pipeline discovery completed: {loaded_count}/{len(pipeline_modules)} modules loaded",
            "Pipeline Discovery"
        )
    else:
        log_with_timestamp(
            "No pipeline modules found. Add *_pipeline.py files to src/pipelines/ directory",
            "Pipeline Discovery",
            "warning"
        )
    
    return loaded_count


def validate_pipeline_data(pipeline_data: Dict[str, Any]) -> None:
    """
    Validate pipeline data structure.
    
    Args:
        pipeline_data: Pipeline data to validate
        
    Raises:
        PipelineError: If pipeline data is invalid
    """
    required_keys = ['pipeline']
    missing_keys = [key for key in required_keys if key not in pipeline_data]
    
    if missing_keys:
        raise PipelineError(
            f"Pipeline data missing required keys: {missing_keys}",
            {'missing_keys': missing_keys, 'provided_keys': list(pipeline_data.keys())}
        )
    
    if not callable(pipeline_data['pipeline']):
        raise PipelineError(
            "Pipeline must be callable",
            {'pipeline_type': type(pipeline_data['pipeline']).__name__}
        )


# Auto-discover pipelines when package is imported
try:
    discovered_count = discover_and_load_pipelines()
    log_with_timestamp(f"Pipeline package initialized with {discovered_count} modules", "Pipelines")
except Exception as e:
    log_with_timestamp(f"Error during pipeline package initialization: {e}", "Pipelines", "error")
    logger.error(f"Pipeline package initialization failed: {e}")

# Public API
__all__ = [
    # Pipeline management
    'register_pipeline',
    'get_all_pipelines', 
    'get_pipeline',
    'remove_pipeline',
    'list_pipeline_names',
    
    # Discovery
    'discover_and_load_pipelines',
    
    # Factory functions
    'create_etl_pipeline',
    'create_el_pipeline',
    'create_parallel_pipeline',
    'create_sequential_pipeline',
    'create_conditional_pipeline',
    'create_retry_pipeline',
    
    # Cron job management
    'register_cron_job',
    'list_cron_jobs',
    'get_cron_job',
    'remove_cron_job',
    
    # Validation
    'validate_pipeline_data',
]
