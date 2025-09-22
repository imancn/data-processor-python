# src/pipelines/tools/backfill_utils.py
"""
Backfill utilities for historical data processing.

This module provides a centralized backfill manager that handles
time range management and backfill execution for all pipelines.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Callable, Any
from core.logging import log_with_timestamp


class BackfillManager:
    """
    Centralized backfill manager for all pipelines.
    
    This class manages time ranges and backfill state across all pipelines,
    ensuring consistent behavior and avoiding duplicate implementations.
    """
    
    def __init__(self):
        self._last_processed_time: Optional[datetime] = None
        self._backfill_start_time: Optional[datetime] = None
        self._backfill_end_time: Optional[datetime] = None
        self._active_pipeline: Optional[str] = None
    
    def set_backfill_time_range(self, start_date: datetime, end_date: datetime, pipeline_name: str = "Unknown"):
        """
        Set the time range for backfill operations.
        
        Args:
            start_date: Start date for backfill
            end_date: End date for backfill
            pipeline_name: Name of the pipeline for logging
        """
        self._last_processed_time = start_date
        self._backfill_start_time = start_date
        self._backfill_end_time = end_date
        self._active_pipeline = pipeline_name
        log_with_timestamp(f"Set backfill time range for {pipeline_name}: {start_date} to {end_date}", "Backfill Manager")
    
    def get_last_processed_time(self, default_hours_ago: int = 1) -> datetime:
        """
        Get the last processed time, defaulting to specified hours ago if not set.
        
        Args:
            default_hours_ago: Default hours to go back if no last processed time is set
            
        Returns:
            Last processed time
        """
        if self._last_processed_time is None:
            self._last_processed_time = datetime.now() - timedelta(hours=default_hours_ago)
        return self._last_processed_time
    
    def update_last_processed_time(self, new_time: datetime):
        """
        Update the last processed time.
        
        Args:
            new_time: New last processed time
        """
        self._last_processed_time = new_time
        log_with_timestamp(f"Updated last processed time to {new_time}", "Backfill Manager")
    
    def get_time_range(self):
        """
        Get the current time range (backfill or incremental).
        
        Returns:
            Tuple of (start_time, end_time, is_backfill)
        """
        if self._backfill_start_time and self._backfill_end_time:
            return self._backfill_start_time, self._backfill_end_time, True
        else:
            start_time = self.get_last_processed_time()
            end_time = datetime.now()
            return start_time, end_time, False
    
    def is_backfill_mode(self) -> bool:
        """
        Check if currently in backfill mode.
        
        Returns:
            True if in backfill mode, False otherwise
        """
        return self._backfill_start_time is not None and self._backfill_end_time is not None
    
    def clear_backfill_mode(self):
        """Clear backfill mode and reset to incremental mode."""
        self._backfill_start_time = None
        self._backfill_end_time = None
        self._active_pipeline = None
        log_with_timestamp("Cleared backfill mode, switched to incremental mode", "Backfill Manager")
    
    def get_active_pipeline(self) -> Optional[str]:
        """Get the currently active pipeline name."""
        return self._active_pipeline


# Global backfill manager instance
backfill_manager = BackfillManager()


def run_backfill(
    pipeline_func: Callable,
    days: int,
    pipeline_name: str = "Pipeline"
) -> bool:
    """
    Run backfill for specified number of days.
    
    This is the centralized backfill function that all pipelines should use.
    It manages the backfill state and executes the pipeline function.
    
    Args:
        pipeline_func: Pipeline function to execute (should be async)
        days: Number of days to backfill
        pipeline_name: Name of the pipeline for logging
        
    Returns:
        True if backfill succeeded, False otherwise
    """
    try:
        log_with_timestamp(f"Starting backfill for {pipeline_name} - {days} days", "Backfill")
        
        # Set time range for backfill
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        backfill_manager.set_backfill_time_range(start_time, end_time, pipeline_name)
        
        log_with_timestamp(f"Backfill time range: {start_time} to {end_time}", "Backfill")
        
        # Run the pipeline
        if asyncio.iscoroutinefunction(pipeline_func):
            result = asyncio.run(pipeline_func())
        else:
            result = pipeline_func()
        
        if result:
            log_with_timestamp(f"Backfill completed successfully for {pipeline_name} - {days} days", "Backfill")
        else:
            log_with_timestamp(f"Backfill failed for {pipeline_name} - {days} days", "Backfill", "error")
        
        # Clear backfill mode after completion
        backfill_manager.clear_backfill_mode()
        
        return result
        
    except Exception as e:
        log_with_timestamp(f"Backfill error for {pipeline_name}: {e}", "Backfill", "error")
        import traceback
        log_with_timestamp(f"Backfill traceback: {traceback.format_exc()}", "Backfill", "error")
        
        # Clear backfill mode even on error
        backfill_manager.clear_backfill_mode()
        
        return False


# Public API
__all__ = [
    'BackfillManager',
    'backfill_manager',
    'run_backfill',
]
