# src/main.py
"""
Main application entry point for the data processing framework.
"""
import asyncio
import sys
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from core.logging import setup_logging, log_with_timestamp
from core.config import config

# Import pipeline factory
from pipelines.pipeline_factory import run_pipeline

# Global cron registry
_cron_registry = {}

def register_cron_job(
    job_name: str,
    pipeline: Callable[..., bool],
    schedule: str,
    description: str = ""
):
    """
    Register a pipeline as a cron job.
    
    Args:
        job_name: Unique name for the cron job
        pipeline: The pipeline function to run
        schedule: Cron schedule expression (e.g., "0 */6 * * *" for every 6 hours)
        description: Optional description of the job
    """
    _cron_registry[job_name] = {
        'pipeline': pipeline,
        'schedule': schedule,
        'description': description,
        'last_run': None,
        'status': 'registered'
    }
    log_with_timestamp(f"Registered cron job: {job_name} (schedule: {schedule})", "Main")

def run_cron_job(job_name: str, *args, **kwargs) -> bool:
    """
    Run a specific cron job.
    
    Args:
        job_name: Name of the cron job to run
        *args, **kwargs: Arguments to pass to the pipeline
        
    Returns:
        bool: True if successful
    """
    if job_name not in _cron_registry:
        log_with_timestamp(f"Cron job '{job_name}' not found", "Main", "error")
        return False
    
    job = _cron_registry[job_name]
    job['last_run'] = datetime.now()
    job['status'] = 'running'
    
    log_with_timestamp(f"Running cron job: {job_name}", "Main")
    
    try:
        # Run the pipeline (handle both sync and async functions)
        pipeline_func = job['pipeline']
        if asyncio.iscoroutinefunction(pipeline_func):
            success = asyncio.run(pipeline_func(*args, **kwargs))
        else:
            success = pipeline_func(*args, **kwargs)
        
        if success:
            job['status'] = 'completed'
            log_with_timestamp(f"Successfully completed cron job: {job_name}", "Main")
        else:
            job['status'] = 'failed'
            log_with_timestamp(f"Cron job failed: {job_name}", "Main", "error")
        
        return success
    except Exception as e:
        job['status'] = 'error'
        log_with_timestamp(f"Error running cron job '{job_name}': {e}", "Main", "error")
        return False

def list_cron_jobs() -> Dict[str, Any]:
    """List all registered cron jobs."""
    return _cron_registry.copy()

def get_cron_job_status(job_name: str) -> Dict[str, Any]:
    """Get the status of a specific cron job."""
    return _cron_registry.get(job_name, {})

def unregister_cron_job(job_name: str) -> bool:
    """Unregister a cron job."""
    if job_name in _cron_registry:
        del _cron_registry[job_name]
        log_with_timestamp(f"Unregistered cron job: {job_name}", "Main")
        return True
    return False

def register_all_pipelines():
    """Register all available pipelines."""
    log_with_timestamp("Registering all pipelines...", "Main")
    
    # This function can be extended to register any pipeline type
    # Specific pipeline implementations should register themselves
    log_with_timestamp("Pipeline registration completed", "Main")

def run_pipeline_by_name(pipeline_name: str) -> bool:
    """Run a pipeline by name."""
    try:
        # Try to run as cron job first
        if pipeline_name in _cron_registry:
            return run_cron_job(pipeline_name)
        else:
            log_with_timestamp(f"Pipeline not found: {pipeline_name}", "Main", "error")
            return False
    except Exception as e:
        log_with_timestamp(f"Error running pipeline {pipeline_name}: {e}", "Main", "error")
        return False

def main():
    """Main application entry point."""
    # Setup logging
    setup_logging(config.log_level, config.log_file)
    
    log_with_timestamp("Starting Data Processing Framework", "Main")
    
    # Register all pipelines
    register_all_pipelines()
    
    # Run specific pipeline if provided
    if len(sys.argv) > 1:
        pipeline_name = sys.argv[1]
        success = run_pipeline_by_name(pipeline_name)
        if success:
            log_with_timestamp(f"Pipeline {pipeline_name} completed successfully", "Main")
        else:
            log_with_timestamp(f"Pipeline {pipeline_name} failed", "Main", "error")
            sys.exit(1)
    else:
        log_with_timestamp("No pipeline specified. Use: python main.py <pipeline_name>", "Main")
        
        # List available pipelines
        cron_jobs = list(_cron_registry.keys())
        
        if cron_jobs:
            log_with_timestamp(f"Available pipelines: {', '.join(cron_jobs)}", "Main")
        else:
            log_with_timestamp("No pipelines available. Register pipelines using register_cron_job()", "Main", "warning")
    
    log_with_timestamp("Application completed", "Main")

if __name__ == "__main__":
    main()