# src/pipelines/pipeline_registry.py
"""
Pipeline registry for managing and discovering pipelines.

This module provides a centralized registry for all pipelines,
enabling easy discovery, registration, and management.
"""

from typing import Dict, Any, List, Optional
from core.logging import log_with_timestamp


class PipelineRegistry:
    """
    Centralized registry for all pipelines.
    
    This class manages pipeline registration, discovery, and provides
    a single source of truth for all available pipelines.
    """
    
    def __init__(self):
        self._pipelines: Dict[str, Dict[str, Any]] = {}
        self._pipeline_classes: Dict[str, Any] = {}
    
    def register_pipeline(self, pipeline_instance: Any) -> bool:
        """
        Register a pipeline instance.
        
        Args:
            pipeline_instance: Pipeline instance to register
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not hasattr(pipeline_instance, 'get_pipeline_info'):
                log_with_timestamp(f"Pipeline instance must have get_pipeline_info method", "Pipeline Registry", "error")
                return False
            
            pipeline_info = pipeline_instance.get_pipeline_info()
            pipeline_name = pipeline_info['name']
            
            if pipeline_name in self._pipelines:
                log_with_timestamp(f"Pipeline {pipeline_name} is already registered, updating", "Pipeline Registry", "warning")
            
            self._pipelines[pipeline_name] = pipeline_info
            self._pipeline_classes[pipeline_name] = pipeline_instance
            
            log_with_timestamp(f"Registered pipeline: {pipeline_name}", "Pipeline Registry")
            return True
            
        except Exception as e:
            log_with_timestamp(f"Error registering pipeline: {e}", "Pipeline Registry", "error")
            return False
    
    def register_pipeline_class(self, pipeline_class: Any, name: str, description: str, schedule: str = "* * * * *") -> bool:
        """
        Register a pipeline class by instantiating it.
        
        Args:
            pipeline_class: Pipeline class to instantiate and register
            name: Pipeline name
            description: Pipeline description
            schedule: Cron schedule
            
        Returns:
            True if successful, False otherwise
        """
        try:
            pipeline_instance = pipeline_class(name, description, schedule)
            return self.register_pipeline(pipeline_instance)
        except Exception as e:
            log_with_timestamp(f"Error registering pipeline class {name}: {e}", "Pipeline Registry", "error")
            return False
    
    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get a pipeline by name.
        
        Args:
            name: Pipeline name
            
        Returns:
            Pipeline info dictionary or None if not found
        """
        return self._pipelines.get(name)
    
    def get_pipeline_instance(self, name: str) -> Optional[Any]:
        """
        Get a pipeline instance by name.
        
        Args:
            name: Pipeline name
            
        Returns:
            Pipeline instance or None if not found
        """
        return self._pipeline_classes.get(name)
    
    def get_all_pipelines(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all registered pipelines.
        
        Returns:
            Dictionary of all pipeline info
        """
        return self._pipelines.copy()
    
    def list_pipeline_names(self) -> List[str]:
        """
        Get list of all pipeline names.
        
        Returns:
            List of pipeline names
        """
        return list(self._pipelines.keys())
    
    def remove_pipeline(self, name: str) -> bool:
        """
        Remove a pipeline from the registry.
        
        Args:
            name: Pipeline name to remove
            
        Returns:
            True if removed, False if not found
        """
        if name in self._pipelines:
            del self._pipelines[name]
            if name in self._pipeline_classes:
                del self._pipeline_classes[name]
            log_with_timestamp(f"Removed pipeline: {name}", "Pipeline Registry")
            return True
        return False
    
    def get_pipelines_by_schedule(self, schedule: str) -> List[Dict[str, Any]]:
        """
        Get all pipelines with a specific schedule.
        
        Args:
            schedule: Cron schedule to filter by
            
        Returns:
            List of pipeline info dictionaries
        """
        return [pipeline for pipeline in self._pipelines.values() if pipeline.get('schedule') == schedule]
    
    def get_scheduled_pipelines(self) -> List[Dict[str, Any]]:
        """
        Get all pipelines that have a schedule (not manual only).
        
        Returns:
            List of scheduled pipeline info dictionaries
        """
        return [pipeline for pipeline in self._pipelines.values() if pipeline.get('schedule') != 'manual']
    
    def validate_pipeline(self, pipeline_info: Dict[str, Any]) -> bool:
        """
        Validate pipeline info structure.
        
        Args:
            pipeline_info: Pipeline info to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_keys = ['name', 'description', 'execute']
        missing_keys = [key for key in required_keys if key not in pipeline_info]
        
        if missing_keys:
            log_with_timestamp(f"Pipeline validation failed - missing keys: {missing_keys}", "Pipeline Registry", "error")
            return False
        
        if not callable(pipeline_info['execute']):
            log_with_timestamp(f"Pipeline validation failed - execute must be callable", "Pipeline Registry", "error")
            return False
        
        return True


# Global pipeline registry instance
pipeline_registry = PipelineRegistry()


def register_pipeline(pipeline_instance: Any) -> bool:
    """
    Register a pipeline instance in the global registry.
    
    Args:
        pipeline_instance: Pipeline instance to register
        
    Returns:
        True if successful, False otherwise
    """
    return pipeline_registry.register_pipeline(pipeline_instance)


def register_pipeline_class(pipeline_class: Any, name: str, description: str, schedule: str = "* * * * *") -> bool:
    """
    Register a pipeline class in the global registry.
    
    Args:
        pipeline_class: Pipeline class to instantiate and register
        name: Pipeline name
        description: Pipeline description
        schedule: Cron schedule
        
    Returns:
        True if successful, False otherwise
    """
    return pipeline_registry.register_pipeline_class(pipeline_class, name, description, schedule)


def get_pipeline(name: str) -> Optional[Dict[str, Any]]:
    """
    Get a pipeline by name from the global registry.
    
    Args:
        name: Pipeline name
        
    Returns:
        Pipeline info dictionary or None if not found
    """
    return pipeline_registry.get_pipeline(name)


def get_all_pipelines() -> Dict[str, Dict[str, Any]]:
    """
    Get all pipelines from the global registry.
    
    Returns:
        Dictionary of all pipeline info
    """
    return pipeline_registry.get_all_pipelines()


def list_pipeline_names() -> List[str]:
    """
    Get list of all pipeline names from the global registry.
    
    Returns:
        List of pipeline names
    """
    return pipeline_registry.list_pipeline_names()


# Public API
__all__ = [
    'PipelineRegistry',
    'pipeline_registry',
    'register_pipeline',
    'register_pipeline_class',
    'get_pipeline',
    'get_all_pipelines',
    'list_pipeline_names',
]
