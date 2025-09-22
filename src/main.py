# src/main.py
"""
Main application entry point for the data processing framework.

This module provides the main application interface with pipeline discovery,
cron job management, and command-line interface functionality.
"""
import asyncio
import sys
import os
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from pathlib import Path

from core.logging import setup_logging, log_with_timestamp
from core.config import config
from core import (
    validate_pipeline_config, PipelineConfig, ValidationError,
    log_pipeline_stage, PerformanceLogger
)
from pipelines.pipeline_registry import pipeline_registry, get_all_pipelines

# Import migration manager
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'migrations'))
from migration_manager import ClickHouseMigrationManager

# Global cron registry
_cron_registry = {}

def ensure_database_schema():
    """Ensure database schema is up to date by running migrations."""
    log_with_timestamp("Checking database schema...", "Main")
    try:
        migration_manager = ClickHouseMigrationManager()
        if migration_manager.run_migrations():
            log_with_timestamp("Database schema is up to date", "Main")
            return True
        else:
            log_with_timestamp("Database schema migration failed", "Main", "error")
            return False
    except Exception as e:
        log_with_timestamp(f"Error checking database schema: {e}", "Main", "error")
        return False

def register_cron_job(
    job_name: str,
    pipeline: Callable[..., bool],
    schedule: str,
    description: str = "",
    **kwargs
):
    """
    Register a pipeline as a cron job with Pydantic validation.
    
    Args:
        job_name: Unique name for the cron job
        pipeline: The pipeline function to run
        schedule: Cron schedule expression (e.g., "0 */6 * * *" for every 6 hours)
        description: Optional description of the job
        **kwargs: Additional pipeline configuration options
    """
    try:
        # Create pipeline configuration data
        pipeline_data = {
            'name': job_name,
            'description': description,
            'schedule': schedule,
            **kwargs
        }
        
        # Validate pipeline configuration using Pydantic
        pipeline_config = validate_pipeline_config(pipeline_data)
        
        # Register the validated pipeline
        _cron_registry[job_name] = {
            'pipeline': pipeline,
            'config': pipeline_config,
            'last_run': None,
            'status': 'registered'
        }
        
        log_with_timestamp(
            f"Registered cron job: {job_name} (schedule: {schedule}, validated)", 
            "Main"
        )
        
    except ValidationError as e:
        log_with_timestamp(
            f"Failed to register cron job '{job_name}': {e}", 
            "Main", 
            "error"
        )
        raise

@log_pipeline_stage('JobExecution', 'info')
def run_cron_job(job_name: str, *args, **kwargs) -> bool:
    """
    Run a specific cron job with performance monitoring and validation.
    
    Args:
        job_name: Name of the cron job to run
        *args, **kwargs: Arguments to pass to the pipeline
        
    Returns:
        bool: True if successful
    """
    if job_name not in _cron_registry:
        log_with_timestamp(f"Cron job '{job_name}' not found", "Main", "error")
        return False
    
    # Always ensure database schema is up to date before running any job
    if not ensure_database_schema():
        log_with_timestamp(f"Database schema check failed, skipping job: {job_name}", "Main", "error")
        return False
    
    job = _cron_registry[job_name]
    job['last_run'] = datetime.now()
    job['status'] = 'running'
    
    # Get pipeline configuration for validation and monitoring
    pipeline_config = job.get('config')
    timeout = pipeline_config.timeout if pipeline_config and pipeline_config.timeout else 300
    
    log_with_timestamp(f"Running cron job: {job_name} (timeout: {timeout}s)", "Main")
    
    # Use performance logger for detailed timing
    with PerformanceLogger(f"CronJob-{job_name}", "JobExecution"):
        try:
            # Run the pipeline (handle both sync and async functions)
            pipeline_func = job['pipeline']
            if asyncio.iscoroutinefunction(pipeline_func):
                success = asyncio.run(pipeline_func(*args, **kwargs))
            else:
                success = pipeline_func(*args, **kwargs)
        except Exception as e:
            log_with_timestamp(f"Pipeline execution failed: {str(e)}", "Main", "error")
            success = False
        
        if success:
            job['status'] = 'completed'
            log_with_timestamp(f"Successfully completed cron job: {job_name}", "Main")
        else:
            job['status'] = 'failed'
            log_with_timestamp(f"Cron job failed: {job_name}", "Main", "error")
        
        return success

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

def _discover_pipeline_modules() -> list:
    """Discover pipeline modules in the pipelines directory."""
    pipelines_dir = Path(__file__).parent / 'pipelines'
    pipeline_modules = []
    
    # Look for pipeline files in the main pipelines directory
    for file_path in pipelines_dir.glob('*_pipeline.py'):
        if file_path.name != '__init__.py':
            pipeline_modules.append(file_path.stem)
    
    return pipeline_modules


def _load_pipeline_module(module_name: str) -> Optional[Any]:
    """Load a pipeline module and return it."""
    try:
        import importlib
        return importlib.import_module(f'pipelines.{module_name}')
    except Exception as e:
        log_with_timestamp(f"Error loading pipeline module {module_name}: {e}", "Main", "warning")
        return None


def _register_pipeline_from_module(module: Any, module_name: str) -> int:
    """Register pipelines from a loaded module."""
    registered_count = 0
    
    # Look for register function
    if hasattr(module, 'register_pipelines'):
        module.register_pipelines()
        log_with_timestamp(f"Registered pipelines from {module_name}", "Main")
        registered_count += 1
    
    return registered_count

def get_registered_pipelines() -> Dict[str, Dict[str, Any]]:
    """Get all registered pipelines from the pipeline registry."""
    return get_all_pipelines()

def get_scheduled_pipelines() -> Dict[str, Dict[str, Any]]:
    """Get all scheduled pipelines (non-manual)."""
    return {name: info for name, info in get_all_pipelines().items() 
            if info.get('schedule') != 'manual'}


def register_all_pipelines():
    """Register all available pipelines."""
    log_with_timestamp("Registering all pipelines...", "Main")
    
    pipeline_modules = _discover_pipeline_modules()
    total_registered = 0
    
    for module_name in pipeline_modules:
        module = _load_pipeline_module(module_name)
        if module:
            registered_count = _register_pipeline_from_module(module, module_name)
            total_registered += registered_count
    
    if total_registered == 0:
        log_with_timestamp("No pipelines found. Add pipeline modules to src/pipelines/ directory", "Main", "warning")
    else:
        log_with_timestamp(f"Registered {total_registered} cron jobs from {len(pipeline_modules)} pipeline modules", "Main")

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

def _handle_migrate_command():
    """Handle the migrate command."""
    log_with_timestamp("Running database migrations...", "Main")
    if ensure_database_schema():
        log_with_timestamp("Migrations completed successfully", "Main")
    else:
        log_with_timestamp("Migrations failed", "Main", "error")
        sys.exit(1)


def _handle_migrate_status_command():
    """Handle the migrate_status command."""
    log_with_timestamp("Checking migration status...", "Main")
    try:
        migration_manager = ClickHouseMigrationManager()
        migration_manager.show_status()
    except Exception as e:
        log_with_timestamp(f"Error checking migration status: {e}", "Main", "error")
        sys.exit(1)


def _handle_list_command():
    """Handle the list command."""
    register_all_pipelines()
    cron_jobs = list(_cron_registry.keys())
    
    if cron_jobs:
        log_with_timestamp(f"Available pipelines: {', '.join(cron_jobs)}", "Main")
    else:
        log_with_timestamp("No pipelines available. Register pipelines using register_cron_job()", "Main", "warning")


def _handle_pipeline_command(pipeline_name: str):
    """Handle running a specific pipeline."""
    register_all_pipelines()
    success = run_pipeline_by_name(pipeline_name)
    if success:
        log_with_timestamp(f"Pipeline {pipeline_name} completed successfully", "Main")
    else:
        log_with_timestamp(f"Pipeline {pipeline_name} failed", "Main", "error")
        sys.exit(1)


def _show_help():
    """Show help information."""
    log_with_timestamp("No command specified. Available commands:", "Main")
    log_with_timestamp("  migrate        - Run database migrations", "Main")
    log_with_timestamp("  migrate_status - Show migration status", "Main")
    log_with_timestamp("  list           - List available pipelines", "Main")
    log_with_timestamp("  <pipeline_name> - Run specific pipeline", "Main")
    
    # Register all pipelines and show them
    register_all_pipelines()
    cron_jobs = list(_cron_registry.keys())
    
    if cron_jobs:
        log_with_timestamp(f"Available pipelines: {', '.join(cron_jobs)}", "Main")
    else:
        log_with_timestamp("No pipelines available. Register pipelines using register_cron_job()", "Main", "warning")


def main():
    """Main application entry point."""
    # Setup logging
    setup_logging(config.log_level, config.log_file)
    
    log_with_timestamp("Starting Data Processing Framework", "Main")
    
    # Handle commands
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "migrate":
            _handle_migrate_command()
        elif command == "migrate_status":
            _handle_migrate_status_command()
        elif command == "list":
            _handle_list_command()
        else:
            _handle_pipeline_command(command)
    else:
        _show_help()
    
    log_with_timestamp("Application completed", "Main")

if __name__ == "__main__":
    main()