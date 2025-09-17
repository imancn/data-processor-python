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

from core.logging import setup_logging, log_with_timestamp
from core.config import config
from main import run_cron_job, list_cron_jobs, register_cron_job

# Import pipelines
from pipelines.cmc_pipeline import register_cmc_pipelines, get_cmc_pipeline
from examples.weather_pipeline import create_weather_pipeline

def register_all_pipelines():
    """Register all available pipelines as cron jobs."""
    log_with_timestamp("Registering all pipelines as cron jobs...", "Run Script")
    
    # Register CMC pipelines
    register_cmc_pipelines()
    
    # Register CMC pipelines as cron jobs
    from pipelines.cmc_pipeline import _pipeline_registry
    for name, pipeline in _pipeline_registry.items():
        register_cron_job(
            job_name=name,
            pipeline=pipeline,
            schedule="0 */6 * * *",  # Every 6 hours
            description=f"CMC {name} pipeline"
        )
    
    # Register weather pipeline as cron job
    weather_pipeline = create_weather_pipeline()
    register_cron_job(
        job_name="weather_data",
        pipeline=weather_pipeline,
        schedule="0 */2 * * *",  # Every 2 hours
        description="Weather data pipeline"
    )
    
    log_with_timestamp(f"Registered {len(list_cron_jobs())} cron jobs", "Run Script")

def run_pipeline(pipeline_name: str):
    """Run a specific pipeline."""
    log_with_timestamp(f"Running pipeline: {pipeline_name}", "Run Script")
    
    # Try to get from CMC pipelines first
    cmc_pipeline = get_cmc_pipeline(pipeline_name)
    if cmc_pipeline:
        success = asyncio.run(cmc_pipeline())
        if success:
            log_with_timestamp(f"Successfully completed pipeline: {pipeline_name}", "Run Script")
        else:
            log_with_timestamp(f"Pipeline failed: {pipeline_name}", "Run Script", "error")
        return success
    
    # Try to run as cron job
    if pipeline_name in list_cron_jobs():
        return run_cron_job(pipeline_name)
    
    log_with_timestamp(f"Pipeline not found: {pipeline_name}", "Run Script", "error")
    return False

def list_available_pipelines():
    """List all available pipelines."""
    log_with_timestamp("Available pipelines:", "Run Script")
    
    # List CMC pipelines
    from pipelines.cmc_pipeline import list_cmc_pipelines
    cmc_pipelines = list_cmc_pipelines()
    for pipeline in cmc_pipelines:
        log_with_timestamp(f"  - {pipeline} (CMC)", "Run Script")
    
    # List cron jobs
    cron_jobs = list_cron_jobs()
    for job_name, job_info in cron_jobs.items():
        log_with_timestamp(f"  - {job_name} (Cron: {job_info['schedule']})", "Run Script")

def main():
    """Main entry point."""
    # Setup logging
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
        run_pipeline(pipeline_name)
    elif command == "list":
        list_available_pipelines()
    else:
        log_with_timestamp(f"Unknown command: {command}", "Run Script", "error")

if __name__ == "__main__":
    main()