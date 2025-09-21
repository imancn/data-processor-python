# src/pipelines/pipeline_registry.py
"""
Generic Pipeline Registration System
Supports dynamic pipeline registration with cron scheduling and table naming.
"""
from typing import Dict, Any, List, Optional
from core.logging import log_with_timestamp

# Global pipeline registry
_pipeline_registry = {}
_cron_registry = {}

def register_pipeline(
    pipeline_name: str,
    pipeline_func: callable,
    schedule: str = "0 * * * *",
    description: str = "",
    table_name: Optional[str] = None
) -> bool:
    """
    Register a pipeline with cron scheduling and table naming.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., 'data_hourly')
        pipeline_func: Pipeline function to execute
        schedule: Cron schedule expression
        description: Human-readable description
        table_name: Destination table name (defaults to pipeline_name)
    """
    try:
        # Use pipeline_name as table_name if not specified
        if table_name is None:
            table_name = pipeline_name
        
        # Register pipeline
        _pipeline_registry[pipeline_name] = {
            'pipeline': pipeline_func,
            'table_name': table_name,
            'description': description
        }
        
        # Register cron job
        _cron_registry[pipeline_name] = {
            'pipeline': pipeline_func,
            'schedule': schedule,
            'description': description,
            'table_name': table_name
        }
        
        log_with_timestamp(f"Registered pipeline: {pipeline_name} -> {table_name} (schedule: {schedule})", "Pipeline Registry")
        return True
        
    except Exception as e:
        log_with_timestamp(f"Failed to register pipeline {pipeline_name}: {e}", "Pipeline Registry", "error")
        return False

def unregister_pipeline(pipeline_name: str) -> bool:
    """Unregister a pipeline."""
    try:
        if pipeline_name in _pipeline_registry:
            del _pipeline_registry[pipeline_name]
        if pipeline_name in _cron_registry:
            del _cron_registry[pipeline_name]
        
        log_with_timestamp(f"Unregistered pipeline: {pipeline_name}", "Pipeline Registry")
        return True
    except Exception as e:
        log_with_timestamp(f"Failed to unregister pipeline {pipeline_name}: {e}", "Pipeline Registry", "error")
        return False

def get_pipeline(pipeline_name: str) -> Optional[Dict[str, Any]]:
    """Get a pipeline by name."""
    return _pipeline_registry.get(pipeline_name)

def get_cron_job(job_name: str) -> Optional[Dict[str, Any]]:
    """Get a cron job by name."""
    return _cron_registry.get(job_name)

def list_pipelines() -> List[str]:
    """List all registered pipelines."""
    return list(_pipeline_registry.keys())

def list_cron_jobs() -> Dict[str, Dict[str, Any]]:
    """List all registered cron jobs."""
    return _cron_registry.copy()

def get_table_name(pipeline_name: str) -> Optional[str]:
    """Get the table name for a pipeline."""
    pipeline = _pipeline_registry.get(pipeline_name)
    return pipeline.get('table_name') if pipeline else None

def register_pipeline_with_timescope(
    base_name: str,
    pipeline_func: callable,
    time_scopes: List[str],
    schedule_template: str = "0 * * * *",
    description_template: str = "{base_name} {time_scope} data"
) -> int:
    """
    Register multiple pipelines for different time scopes.
    
    Args:
        base_name: Base pipeline name (e.g., 'data')
        pipeline_func: Pipeline function factory
        time_scopes: List of time scopes (e.g., ['hourly', 'daily', 'weekly'])
        schedule_template: Cron schedule template
        description_template: Description template with {base_name} and {time_scope} placeholders
    
    Returns:
        Number of pipelines registered
    """
    registered_count = 0
    
    for time_scope in time_scopes:
        pipeline_name = f"{base_name}_{time_scope}"
        table_name = f"{base_name}_{time_scope}"
        schedule = schedule_template
        description = description_template.format(base_name=base_name, time_scope=time_scope)
        
        if register_pipeline(pipeline_name, pipeline_func, schedule, description, table_name):
            registered_count += 1
    
    return registered_count

def clear_registry():
    """Clear all registered pipelines and cron jobs."""
    global _pipeline_registry, _cron_registry
    _pipeline_registry.clear()
    _cron_registry.clear()
    log_with_timestamp("Cleared all pipeline registrations", "Pipeline Registry")
