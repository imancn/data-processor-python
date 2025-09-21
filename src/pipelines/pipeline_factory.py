# src/pipelines/pipeline_factory.py
"""
Pipeline factory for creating various types of data processing pipelines.

This module provides factory functions for creating ETL, EL, parallel, sequential,
conditional, and retry pipelines with standardized error handling and logging.
"""
from typing import Callable, Optional, Dict, Any, List
import asyncio
import pandas as pd
from datetime import datetime

from core.logging import log_with_timestamp
from core.models import PipelineConfig

def create_el_pipeline(
    extractor: Callable[..., pd.DataFrame],
    loader: Callable[[pd.DataFrame], bool],
    name: str = "EL Pipeline"
) -> Callable[..., bool]:
    """
    Creates an Extract-Load (EL) pipeline with pandas DataFrame support.
    """
    async def el_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} (EL)", name)
        try:
            # Extract data
            data = await extractor(*args, **kwargs)
            if data.empty:
                log_with_timestamp(f"No data extracted in {name}", name, "warning")
                return True

            # Load data
            success = loader(data)
            if success:
                log_with_timestamp(f"Successfully completed {name} (EL)", name)
            else:
                log_with_timestamp(f"Failed to load data in {name}", name, "error")
            return success

        except Exception as e:
            log_with_timestamp(f"Pipeline {name} failed: {e}", name, "error")
            return False

    return el_pipeline_func

def create_etl_pipeline(
    extractor: Callable[..., pd.DataFrame],
    transformer: Callable[[pd.DataFrame], pd.DataFrame],
    loader: Callable[[pd.DataFrame], bool],
    name: str = "ETL Pipeline"
) -> Callable[..., bool]:
    """
    Creates an Extract-Transform-Load (ETL) pipeline with pandas DataFrame support.
    """
    async def etl_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} (ETL)", name)
        try:
            # Extract data
            data = await extractor(*args, **kwargs)
            if data.empty:
                log_with_timestamp(f"No data extracted in {name}", name, "warning")
                return True

            # Transform data
            transformed_data = transformer(data)
            if transformed_data.empty:
                log_with_timestamp(f"No data after transformation in {name}", name, "warning")
                return True

            # Load data
            success = loader(transformed_data)
            if success:
                log_with_timestamp(f"Successfully completed {name} (ETL)", name)
            else:
                log_with_timestamp(f"Failed to load data in {name}", name, "error")
            return success

        except Exception as e:
            log_with_timestamp(f"Pipeline {name} failed: {e}", name, "error")
            return False

    return etl_pipeline_func
def create_parallel_pipeline(
    pipelines: List[Callable[..., bool]],
    name: str = "Parallel Pipeline"
) -> Callable[..., bool]:
    """
    Creates a pipeline that runs multiple pipelines in parallel.
    
    Args:
        pipelines: List of pipeline functions to run in parallel
        name: Name of the parallel pipeline
        
    Returns:
        Pipeline function that runs all pipelines in parallel
    """
    async def parallel_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} with {len(pipelines)} parallel pipelines", name)
        try:
            # Run all pipelines in parallel
            tasks = [pipeline(*args, **kwargs) for pipeline in pipelines]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check results
            success_count = sum(1 for result in results if result is True)
            failure_count = len(results) - success_count
            
            if failure_count == 0:
                log_with_timestamp(f"All {len(pipelines)} pipelines in {name} completed successfully", name)
                return True
            else:
                log_with_timestamp(f"{failure_count} out of {len(pipelines)} pipelines failed in {name}", name, "warning")
                return success_count > 0

        except Exception as e:
            log_with_timestamp(f"Parallel pipeline {name} failed: {e}", name, "error")
            return False

    return parallel_pipeline_func

def create_sequential_pipeline(
    pipelines: List[Callable[..., bool]],
    name: str = "Sequential Pipeline"
) -> Callable[..., bool]:
    """
    Creates a pipeline that runs multiple pipelines sequentially.
    
    Args:
        pipelines: List of pipeline functions to run sequentially
        name: Name of the sequential pipeline
        
    Returns:
        Pipeline function that runs all pipelines sequentially
    """
    async def sequential_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} with {len(pipelines)} sequential pipelines", name)
        try:
            success_count = 0
            
            for i, pipeline in enumerate(pipelines):
                log_with_timestamp(f"Running pipeline {i+1}/{len(pipelines)} in {name}", name)
                result = await pipeline(*args, **kwargs)
                if result:
                    success_count += 1
                else:
                    log_with_timestamp(f"Pipeline {i+1} failed in {name}", name, "warning")
            
            if success_count == len(pipelines):
                log_with_timestamp(f"All {len(pipelines)} pipelines in {name} completed successfully", name)
                return True
            else:
                log_with_timestamp(f"{success_count} out of {len(pipelines)} pipelines succeeded in {name}", name, "warning")
                return success_count > 0

        except Exception as e:
            log_with_timestamp(f"Sequential pipeline {name} failed: {e}", name, "error")
            return False

    return sequential_pipeline_func

def create_conditional_pipeline(
    condition_func: Callable[..., bool],
    true_pipeline: Callable[..., bool],
    false_pipeline: Optional[Callable[..., bool]] = None,
    name: str = "Conditional Pipeline"
) -> Callable[..., bool]:
    """
    Creates a pipeline that conditionally runs different pipelines based on a condition.
    """
    async def conditional_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name}", name)
        try:
            # Check condition
            condition_result = condition_func(*args, **kwargs)
            
            if condition_result:
                log_with_timestamp(f"Condition met, running true pipeline in {name}", name)
                return await true_pipeline(*args, **kwargs)
            else:
                if false_pipeline:
                    log_with_timestamp(f"Condition not met, running false pipeline in {name}", name)
                    return await false_pipeline(*args, **kwargs)
                else:
                    log_with_timestamp(f"Condition not met, no false pipeline defined in {name}", name, "warning")
                    return True

        except Exception as e:
            log_with_timestamp(f"Conditional pipeline {name} failed: {e}", name, "error")
            return False

    return conditional_pipeline_func

def create_retry_pipeline(
    pipeline: Callable[..., bool],
    max_retries: int = 3,
    retry_delay: float = 1.0,
    name: str = "Retry Pipeline"
) -> Callable[..., bool]:
    """
    Creates a pipeline that retries on failure with pandas DataFrame support.
    """
    async def retry_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} with max {max_retries} retries", name)
        
        for attempt in range(max_retries + 1):
            try:
                result = await pipeline(*args, **kwargs)
                if result:
                    if attempt > 0:
                        log_with_timestamp(f"Pipeline succeeded on attempt {attempt + 1} in {name}", name)
                    return True
                else:
                    if attempt < max_retries:
                        log_with_timestamp(f"Pipeline failed on attempt {attempt + 1}, retrying in {retry_delay}s...", name, "warning")
                        await asyncio.sleep(retry_delay)
                    else:
                        log_with_timestamp(f"Pipeline failed after {max_retries + 1} attempts in {name}", name, "error")
                        return False
            except Exception as e:
                if attempt < max_retries:
                    log_with_timestamp(f"Pipeline exception on attempt {attempt + 1}: {e}, retrying in {retry_delay}s...", name, "warning")
                    await asyncio.sleep(retry_delay)
                else:
                    log_with_timestamp(f"Pipeline failed after {max_retries + 1} attempts with exception: {e}", name, "error")
                    return False
        
        return False

    return retry_pipeline_func


class PipelineFactory:
    """
    Factory for creating pipelines from configuration.
    
    This class provides a centralized way to create pipelines based on
    configuration objects, with support for different pipeline types.
    """
    
    def __init__(self):
        """Initialize the pipeline factory."""
        self.extractors = {}
        self.transformers = {}
        self.loaders = {}
    
    def create_pipeline(self, config: PipelineConfig) -> Callable[..., bool]:
        """
        Create a pipeline from configuration.
        
        Args:
            config: Pipeline configuration object
            
        Returns:
            Pipeline function based on configuration
            
        Note:
            This is a simplified implementation. In a real system, this would
            use the configuration to create appropriate extractors, transformers,
            and loaders based on the pipeline requirements.
        """
        # For now, create a simple ETL pipeline
        # In a real implementation, this would use the config to create appropriate components
        
        async def mock_extractor(*args, **kwargs) -> pd.DataFrame:
            """Mock extractor that returns sample data."""
            return pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        
        def mock_transformer(data: pd.DataFrame) -> pd.DataFrame:
            """Mock transformer that passes data through."""
            return data
        
        def mock_loader(data: pd.DataFrame) -> bool:
            """Mock loader that always succeeds."""
            return True
        
        return create_etl_pipeline(
            mock_extractor,
            mock_transformer, 
            mock_loader,
            config.name
        )
    
    def get_pipeline_types(self) -> List[str]:
        """Get available pipeline types."""
        return ['etl', 'el', 'parallel', 'sequential', 'conditional', 'retry']
    
    def register_extractor(self, name: str, extractor: Callable[..., pd.DataFrame]) -> None:
        """Register a custom extractor."""
        self.extractors[name] = extractor
    
    def register_transformer(self, name: str, transformer: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
        """Register a custom transformer."""
        self.transformers[name] = transformer
    
    def register_loader(self, name: str, loader: Callable[[pd.DataFrame], bool]) -> None:
        """Register a custom loader."""
        self.loaders[name] = loader
