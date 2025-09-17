# src/pipelines/pipeline_factory.py
from typing import List, Dict, Any, Callable, Optional
import asyncio
from datetime import datetime
from core.logging import log_with_timestamp

def create_el_pipeline(
    extractor: Callable[..., Any],
    loader: Callable[..., bool],
    name: str = "EL Pipeline"
) -> Callable[..., bool]:
    """
    Creates an Extract-Load (EL) pipeline.
    """
    async def el_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} (EL)", name)
        try:
            # Extract data
            data = await extractor(*args, **kwargs)
            if not data:
                log_with_timestamp(f"No data extracted in {name}", name, "warning")
                return True

            # Load data
            success = loader(data, *args, **kwargs)
            if success:
                log_with_timestamp(f"Successfully completed {name} (EL)", name)
            else:
                log_with_timestamp(f"Failed to load data in {name}", name, "error")
            return success
        except Exception as e:
            log_with_timestamp(f"Error in {name}: {e}", name, "error")
            return False
    return el_pipeline_func

def create_etl_pipeline(
    extractor: Callable[..., Any],
    transformer: Callable[..., List[Dict[str, Any]]],
    loader: Callable[..., bool],
    name: str = "ETL Pipeline"
) -> Callable[..., bool]:
    """
    Creates an Extract-Transform-Load (ETL) pipeline.
    """
    async def etl_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} (ETL)", name)
        try:
            # Extract data
            data = await extractor(*args, **kwargs)
            if not data:
                log_with_timestamp(f"No data extracted in {name}", name, "warning")
                return True

            # Transform data
            transformed_data = transformer(data, *args, **kwargs)
            if not transformed_data:
                log_with_timestamp(f"No data after transformation in {name}", name, "warning")
                return True

            # Load data
            success = loader(transformed_data, *args, **kwargs)
            if success:
                log_with_timestamp(f"Successfully completed {name} (ETL)", name)
            else:
                log_with_timestamp(f"Failed to load data in {name}", name, "error")
            return success
        except Exception as e:
            log_with_timestamp(f"Error in {name}: {e}", name, "error")
            return False
    return etl_pipeline_func

def create_elt_pipeline(
    extractor: Callable[..., Any],
    loader: Callable[..., bool],
    transformer: Callable[..., List[Dict[str, Any]]],
    name: str = "ELT Pipeline"
) -> Callable[..., bool]:
    """
    Creates an Extract-Load-Transform (ELT) pipeline.
    """
    async def elt_pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name} (ELT)", name)
        try:
            # Extract data
            data = await extractor(*args, **kwargs)
            if not data:
                log_with_timestamp(f"No data extracted in {name}", name, "warning")
                return True

            # Load raw data
            load_success = loader(data, *args, **kwargs)
            if not load_success:
                log_with_timestamp(f"Failed to load raw data in {name}", name, "error")
                return False

            # Transform data
            transformed_data = transformer(data, *args, **kwargs)
            if not transformed_data:
                log_with_timestamp(f"No data after transformation in {name}", name, "warning")
                return True

            # Load transformed data
            success = loader(transformed_data, *args, **kwargs)
            if success:
                log_with_timestamp(f"Successfully completed {name} (ELT)", name)
            else:
                log_with_timestamp(f"Failed to load transformed data in {name}", name, "error")
            return success
        except Exception as e:
            log_with_timestamp(f"Error in {name}: {e}", name, "error")
            return False
    return elt_pipeline_func

def run_pipeline(pipeline: Callable[..., bool], *args, **kwargs) -> bool:
    """
    Runs a pipeline synchronously.
    """
    return asyncio.run(pipeline(*args, **kwargs))
