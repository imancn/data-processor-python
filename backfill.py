#!/usr/bin/env python3
"""
Backfill script for running historical data collection jobs.
This script is independent from pipelines and just triggers jobs directly.
"""
import sys
import os
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from core.logging import setup_logging, log_with_timestamp
from core.config import config
from main import run_cron_job, list_cron_jobs, register_all_pipelines

def get_available_jobs() -> List[str]:
    """Get list of available jobs for backfill."""
    register_all_pipelines()
    return list(list_cron_jobs().keys())

def run_job_for_date_range(job_name: str, start_date: datetime, end_date: datetime) -> bool:
    """
    Run a specific job for a date range.
    Each job has its own time scope logic, so we just trigger it.
    """
    log_with_timestamp(f"Running {job_name} for date range {start_date.date()} to {end_date.date()}", "Backfill")
    
    try:
        # Each job handles its own time scope internally
        success = run_cron_job(job_name)
        if success:
            log_with_timestamp(f"Successfully completed {job_name}", "Backfill")
        else:
            log_with_timestamp(f"Failed to complete {job_name}", "Backfill", "error")
        return success
    except Exception as e:
        log_with_timestamp(f"Error running {job_name}: {e}", "Backfill", "error")
        return False

def backfill_job(job_name: str, days: int) -> bool:
    """
    Backfill a specific job for the specified number of days.
    """
    if job_name not in get_available_jobs():
        log_with_timestamp(f"Job '{job_name}' not available. Available jobs: {', '.join(get_available_jobs())}", "Backfill", "error")
        return False
    
    log_with_timestamp(f"Starting backfill for {job_name} for {days} days", "Backfill")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    log_with_timestamp(f"Backfill period: {start_date.date()} to {end_date.date()}", "Backfill")
    
    # Run the job (it will handle its own time scope logic)
    success = run_job_for_date_range(job_name, start_date, end_date)
    
    if success:
        log_with_timestamp(f"Backfill completed successfully for {job_name}", "Backfill")
    else:
        log_with_timestamp(f"Backfill failed for {job_name}", "Backfill", "error")
    
    return success

def backfill_all_jobs(days: int) -> bool:
    """
    Backfill all available jobs for the specified number of days.
    """
    available_jobs = get_available_jobs()
    log_with_timestamp(f"Starting backfill for all jobs: {', '.join(available_jobs)}", "Backfill")
    
    results = {}
    for job_name in available_jobs:
        log_with_timestamp(f"Backfilling {job_name}...", "Backfill")
        success = backfill_job(job_name, days)
        results[job_name] = success
    
    # Summary
    successful_jobs = [job for job, success in results.items() if success]
    failed_jobs = [job for job, success in results.items() if not success]
    
    log_with_timestamp(f"Backfill summary: {len(successful_jobs)} successful, {len(failed_jobs)} failed", "Backfill")
    
    if successful_jobs:
        log_with_timestamp(f"Successful jobs: {', '.join(successful_jobs)}", "Backfill")
    
    if failed_jobs:
        log_with_timestamp(f"Failed jobs: {', '.join(failed_jobs)}", "Backfill", "error")
    
    return len(failed_jobs) == 0

def show_data_counts():
    """Show data counts in the database."""
    try:
        from clickhouse_driver import Client
        
        clickhouse_config = config.get_clickhouse_config()
        client = Client(
            host=clickhouse_config['host'],
            port=clickhouse_config['port'],
            user=clickhouse_config['user'],
            password=clickhouse_config['password'],
            database=clickhouse_config['database']
        )
        
        log_with_timestamp("Database data counts:", "Backfill")
        
        # Check each table
        tables = [
            'cmc_latest_quotes',
            'cmc_hourly', 
            'cmc_daily',
            'cmc_weekly',
            'cmc_monthly',
            'cmc_yearly'
        ]
        
        for table in tables:
            try:
                result = client.execute(f"SELECT COUNT(*) FROM {table}")
                count = result[0][0] if result else 0
                log_with_timestamp(f"  {table}: {count} records", "Backfill")
            except Exception as e:
                log_with_timestamp(f"  {table}: Error - {e}", "Backfill", "warning")
                
    except Exception as e:
        log_with_timestamp(f"Error checking data counts: {e}", "Backfill", "error")

def main():
    """Main entry point."""
    # Setup logging
    setup_logging(config.log_level, config.log_file)
    
    parser = argparse.ArgumentParser(description='Backfill historical data')
    parser.add_argument('command', choices=['backfill', 'backfill_all', 'list_jobs', 'counts'], 
                       help='Command to run')
    parser.add_argument('--days', type=int, default=30, 
                       help='Number of days to backfill (default: 30)')
    parser.add_argument('--jobs', nargs='+', 
                       help='Specific jobs to backfill (for backfill command)')
    
    args = parser.parse_args()
    
    if args.command == 'list_jobs':
        available_jobs = get_available_jobs()
        log_with_timestamp("Available jobs for backfill:", "Backfill")
        for job in available_jobs:
            log_with_timestamp(f"  - {job}", "Backfill")
    
    elif args.command == 'counts':
        show_data_counts()
    
    elif args.command == 'backfill':
        if not args.jobs:
            log_with_timestamp("Please specify jobs to backfill using --jobs", "Backfill", "error")
            sys.exit(1)
        
        success = True
        for job in args.jobs:
            if not backfill_job(job, args.days):
                success = False
        
        sys.exit(0 if success else 1)
    
    elif args.command == 'backfill_all':
        success = backfill_all_jobs(args.days)
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
