# scripts/run.py
"""
Script to run pipelines and cron jobs.
"""
import sys
import os
import asyncio
from datetime import datetime

# Add src and samples to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'samples'))

from core.logging import setup_logging, log_with_timestamp, get_job_log_path
from core.config import config
from main import run_cron_job, list_cron_jobs, register_cron_job

# Import pipelines - domain-specific pipelines should be added here
# from pipelines.example_pipeline import register_example_pipelines

def register_all_pipelines():
    """Register all available pipelines as cron jobs."""
    log_with_timestamp("Registering all pipelines as cron jobs...", "Run Script")
    
    # Use the main.py registration which has proper time scope schedules
    from main import register_all_pipelines as main_register_all_pipelines
    main_register_all_pipelines()
    
    log_with_timestamp(f"Registered {len(list_cron_jobs())} cron jobs", "Run Script")

def run_pipeline(pipeline_name: str):
    """Run a specific pipeline."""
    log_with_timestamp(f"Running pipeline: {pipeline_name}", "Run Script")
    
    # Try to run as cron job
    if pipeline_name in list_cron_jobs():
        return run_cron_job(pipeline_name)
    
    log_with_timestamp(f"Pipeline not found: {pipeline_name}", "Run Script", "error")
    return False

def list_available_pipelines():
    """List all available pipelines."""
    log_with_timestamp("Available pipelines:", "Run Script")
    
    # List cron jobs
    cron_jobs = list_cron_jobs()
    for job_name, job_info in cron_jobs.items():
        config = job_info.get('config')
        if config and hasattr(config, 'schedule'):
            schedule = config.schedule
        else:
            schedule = 'Unknown'
        log_with_timestamp(f"  - {job_name} (Cron: {schedule})", "Run Script")

def main():
    """Main entry point."""
    # Setup logging (high-level app log)
    setup_logging(config.log_level, config.log_file)
    
    log_with_timestamp("Starting Data Processing Framework", "Run Script")
    
    # Register all pipelines
    register_all_pipelines()
    
    if len(sys.argv) < 2:
        log_with_timestamp("Usage: python run.py <command> [pipeline_name]", "Run Script")
        log_with_timestamp("Commands: run, list", "Run Script")
        return
    
    command = sys.argv[1]
    
    if command == "run":
        if len(sys.argv) < 3:
            log_with_timestamp("Please specify a pipeline name", "Run Script", "error")
            return
        pipeline_name = sys.argv[2]
        # Switch to per-job log file temporarily
        from logging import getLogger, FileHandler
        import logging as _logging
        job_log_path = get_job_log_path(pipeline_name)
        fh = FileHandler(job_log_path)
        fh.setLevel(_logging.DEBUG)
        fh.setFormatter(_logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
        root_logger = getLogger()
        root_logger.addHandler(fh)
        run_pipeline(pipeline_name)
        root_logger.removeHandler(fh)
    elif command == "list":
        list_available_pipelines()
    else:
        log_with_timestamp(f"Unknown command: {command}", "Run Script", "error")

if __name__ == "__main__":
    main()