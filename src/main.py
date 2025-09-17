# src/main.py
"""
Main application entry point for the data processing framework.
"""
import asyncio
import sys
import os
from typing import Dict, Any, Optional, Callable
from datetime import datetime
from core.logging import setup_logging, log_with_timestamp
from core.config import config

# Import pipeline factory
# from pipelines.pipeline_factory import run_pipeline  # Not needed in new implementation

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
    
    # Always ensure database schema is up to date before running any job
    if not ensure_database_schema():
        log_with_timestamp(f"Database schema check failed, skipping job: {job_name}", "Main", "error")
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
    
    # Register CMC pipelines
    from pipelines.cmc_pipeline import register_cmc_pipelines
    register_cmc_pipelines()
    
    # Register CMC pipelines as cron jobs using the existing system
    from pipelines.cmc_pipeline import get_pipeline_registry
    
    # Get CMC pipeline registry
    cmc_pipelines = get_pipeline_registry()
    
    # Time scope configurations - aligned with actual time scope starts
    time_scope_schedules = {
        'cmc_latest_quotes': '0 */6 * * *',  # Every 6 hours (00:00, 06:00, 12:00, 18:00)
        'cmc_hourly': '0 * * * *',           # Every hour at minute 0 (start of hour)
        'cmc_daily': '0 0 * * *',            # Daily at 00:00 (start of day)
        'cmc_weekly': '0 0 * * 1',           # Weekly on Monday at 00:00 (start of week)
        'cmc_monthly': '0 0 1 * *',          # Monthly on 1st at 00:00 (start of month)
        'cmc_yearly': '0 0 1 1 *'            # Yearly on Jan 1st at 00:00 (start of year)
    }
    
    # Register each CMC pipeline as cron job
    for pipeline_name, pipeline_data in cmc_pipelines.items():
        schedule = time_scope_schedules.get(pipeline_name, '0 * * * *')
        description = pipeline_data.get('description', f'CMC {pipeline_name} pipeline')
        
        register_cron_job(
            job_name=pipeline_name,
            pipeline=pipeline_data['pipeline'],
            schedule=schedule,
            description=description
        )
    
    log_with_timestamp(f"Registered {len(list_cron_jobs())} cron jobs", "Main")

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
    
    # Handle migration commands
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "migrate":
            log_with_timestamp("Running database migrations...", "Main")
            if ensure_database_schema():
                log_with_timestamp("Migrations completed successfully", "Main")
            else:
                log_with_timestamp("Migrations failed", "Main", "error")
                sys.exit(1)
            return
        
        elif command == "migrate_status":
            log_with_timestamp("Checking migration status...", "Main")
            try:
                migration_manager = ClickHouseMigrationManager()
                migration_manager.show_status()
            except Exception as e:
                log_with_timestamp(f"Error checking migration status: {e}", "Main", "error")
                sys.exit(1)
            return
        
        elif command == "list":
            # Register all pipelines first
            register_all_pipelines()
            
            # List available pipelines
            cron_jobs = list(_cron_registry.keys())
            
            if cron_jobs:
                log_with_timestamp(f"Available pipelines: {', '.join(cron_jobs)}", "Main")
            else:
                log_with_timestamp("No pipelines available. Register pipelines using register_cron_job()", "Main", "warning")
            return
        
        else:
            # Run specific pipeline
            register_all_pipelines()
            success = run_pipeline_by_name(command)
            if success:
                log_with_timestamp(f"Pipeline {command} completed successfully", "Main")
            else:
                log_with_timestamp(f"Pipeline {command} failed", "Main", "error")
                sys.exit(1)
    else:
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
    
    log_with_timestamp("Application completed", "Main")

if __name__ == "__main__":
    main()