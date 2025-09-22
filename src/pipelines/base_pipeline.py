# src/pipelines/base_pipeline.py
"""
Base pipeline class for creating ETL pipelines.

This module provides a base class that all pipelines should inherit from,
ensuring consistent structure and common functionality.
"""

import asyncio
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable

from core.logging import log_with_timestamp
from pipelines.tools.backfill_utils import backfill_manager, run_backfill
from pipelines.tools.data_utils import add_merge_metadata


class BasePipeline(ABC):
    """
    Base class for all ETL pipelines.
    
    This class provides common functionality and structure that all pipelines
    should inherit from to ensure consistency and reusability.
    """
    
    def __init__(self, name: str, description: str, schedule: str = "* * * * *"):
        """
        Initialize the base pipeline.
        
        Args:
            name: Pipeline name
            description: Pipeline description
            schedule: Cron schedule for the pipeline
        """
        self.name = name
        self.description = description
        self.schedule = schedule
        self._last_processed_time: Optional[datetime] = None
    
    @abstractmethod
    async def extract(self) -> pd.DataFrame:
        """
        Extract data from source systems.
        
        Returns:
            DataFrame containing extracted data
        """
        pass
    
    @abstractmethod
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the extracted data.
        
        Args:
            data: DataFrame containing extracted data
            
        Returns:
            DataFrame containing transformed data
        """
        pass
    
    @abstractmethod
    async def load(self, data: pd.DataFrame) -> bool:
        """
        Load the transformed data to destination.
        
        Args:
            data: DataFrame containing transformed data
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    async def execute(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            log_with_timestamp(f"Starting {self.name} pipeline", "Pipeline")
            
            # Extract data
            data = await self.extract()
            if data.empty:
                log_with_timestamp(f"No data to process in {self.name}", "Pipeline", "warning")
                return False
            
            # Transform data
            transformed_data = await self.transform(data)
            if transformed_data.empty:
                log_with_timestamp(f"No data after transformation in {self.name}", "Pipeline", "warning")
                return False
            
            # Load data
            result = await self.load(transformed_data)
            
            if result:
                log_with_timestamp(f"{self.name} pipeline completed successfully", "Pipeline")
            else:
                log_with_timestamp(f"{self.name} pipeline failed", "Pipeline", "error")
            
            return result
            
        except Exception as e:
            log_with_timestamp(f"Error in {self.name} pipeline: {e}", "Pipeline", "error")
            import traceback
            log_with_timestamp(f"Pipeline traceback: {traceback.format_exc()}", "Pipeline", "error")
            return False
    
    def run_backfill(self, days: int) -> bool:
        """
        Run backfill for specified number of days.
        
        Args:
            days: Number of days to backfill
            
        Returns:
            True if successful, False otherwise
        """
        return run_backfill(self.execute, days, self.name)
    
    def get_time_range(self):
        """Get the current time range (backfill or incremental)."""
        return backfill_manager.get_time_range()
    
    def is_backfill_mode(self) -> bool:
        """Check if currently in backfill mode."""
        return backfill_manager.is_backfill_mode()
    
    def set_backfill_time_range(self, start_date: datetime, end_date: datetime):
        """Set the time range for backfill operations."""
        backfill_manager.set_backfill_time_range(start_date, end_date)
    
    def update_last_processed_time(self, new_time: datetime):
        """Update the last processed time."""
        backfill_manager.update_last_processed_time(new_time)
    
    def get_pipeline_info(self) -> Dict[str, Any]:
        """
        Get pipeline information for registration.
        
        Returns:
            Dictionary containing pipeline information
        """
        return {
            "name": self.name,
            "description": self.description,
            "schedule": self.schedule,
            "execute": self.execute
        }


class MetabasePipeline(BasePipeline):
    """
    Base class for pipelines that extract data from Metabase.
    
    This class provides common Metabase-specific functionality.
    """
    
    def __init__(self, name: str, description: str, schedule: str = "* * * * *"):
        super().__init__(name, description, schedule)
        self.metabase_config = None
    
    def _get_metabase_config(self):
        """Get Metabase configuration."""
        if self.metabase_config is None:
            from core.config import config
            self.metabase_config = config.get_metabase_config()
        return self.metabase_config


class ClickHousePipeline(BasePipeline):
    """
    Base class for pipelines that load data to ClickHouse.
    
    This class provides common ClickHouse-specific functionality.
    """
    
    def __init__(self, name: str, description: str, schedule: str = "* * * * *"):
        super().__init__(name, description, schedule)
        self.clickhouse_config = None
    
    def _get_clickhouse_config(self):
        """Get ClickHouse configuration."""
        if self.clickhouse_config is None:
            from core.config import config
            self.clickhouse_config = config.get_clickhouse_config()
        return self.clickhouse_config
    
    def _add_merge_metadata(self, data: pd.DataFrame, merge_status: str = 'success') -> pd.DataFrame:
        """Add merge metadata to DataFrame."""
        return add_merge_metadata(data, self.name, merge_status)


# Public API
__all__ = [
    'BasePipeline',
    'MetabasePipeline', 
    'ClickHousePipeline',
]
