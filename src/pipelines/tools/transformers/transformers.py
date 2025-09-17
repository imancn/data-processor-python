# src/transformers/lambda_transformer.py
from typing import List, Dict, Any, Callable, Optional
from datetime import datetime
import pytz
from core.logging import log_with_timestamp

def apply_transform(
    data: List[Dict[str, Any]],
    transform_func: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    name: str = "Transformer"
) -> List[Dict[str, Any]]:
    """
    Applies a transformation function to each record in the data.
    """
    if not data:
        log_with_timestamp(f"No data to transform for {name}", name, "info")
        return []

    if not isinstance(data, list):
        log_with_timestamp(f"Expected list data, got {type(data)} for {name}", name, "error")
        return []

    transformed_data = []
    skipped_count = 0
    
    for i, record in enumerate(data):
        try:
            if not isinstance(record, dict):
                log_with_timestamp(f"Record {i} is not a dictionary, skipping: {type(record)}", name, "warning")
                skipped_count += 1
                continue
                
            transformed_record = transform_func(record)
            if transformed_record is not None:
                transformed_data.append(transformed_record)
            else:
                skipped_count += 1
        except Exception as e:
            log_with_timestamp(f"Error transforming record {i} in {name}: {e}. Skipping record.", name, "error")
            skipped_count += 1
    
    log_with_timestamp(f"Transformed {len(data)} records into {len(transformed_data)} records for {name} (skipped: {skipped_count})", name, "debug")
    return transformed_data

def create_lambda_transformer(
    lambda_func: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
    name: str = "Lambda Transformer"
) -> Callable[..., List[Dict[str, Any]]]:
    """
    Factory function to create a transformer that applies a given lambda function to each record.
    """
    def transformer_func(data: List[Dict[str, Any]], *args, **kwargs) -> List[Dict[str, Any]]:
        log_with_timestamp(f"Running {name}", name)
        return apply_transform(data, lambda_func, name)
    return transformer_func

def create_pipeline_transformer(
    transform_pipeline: List[Callable[[Dict[str, Any]], Dict[str, Any]]],
    name: str = "Pipeline Transformer"
) -> Callable[..., List[Dict[str, Any]]]:
    """
    Factory function to create a transformer that applies a sequence of transformation functions.
    """
    def pipeline_func(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        current_record = record
        for func in transform_pipeline:
            if current_record is None:
                break
            current_record = func(current_record)
        return current_record

    def transformer_func(data: List[Dict[str, Any]], *args, **kwargs) -> List[Dict[str, Any]]:
        log_with_timestamp(f"Running {name} with {len(transform_pipeline)} steps", name)
        return apply_transform(data, pipeline_func, name)
    return transformer_func

def add_timestamp_metadata(record: Dict[str, Any]) -> Dict[str, Any]:
    """Adds _processed_at timestamp to a record."""
    tehran_tz = pytz.timezone('Asia/Tehran')
    record['_processed_at'] = datetime.now(tehran_tz).isoformat()
    return record

def select_fields(fields: List[str]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Returns a lambda to select specific fields from a record."""
    def _select(record: Dict[str, Any]) -> Dict[str, Any]:
        return {field: record.get(field) for field in fields}
    return _select

def rename_fields(mapping: Dict[str, str]) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """Returns a lambda to rename fields in a record."""
    def _rename(record: Dict[str, Any]) -> Dict[str, Any]:
        new_record = {}
        for old_name, new_name in mapping.items():
            if old_name in record:
                new_record[new_name] = record[old_name]
            else:
                new_record[old_name] = record.get(old_name)
        return new_record
    return _rename

def validate_fields(required_fields: List[str]) -> Callable[[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Returns a lambda to validate required fields in a record. Returns None if invalid."""
    def _validate(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for field in required_fields:
            if field not in record or record[field] is None:
                log_with_timestamp(f"Missing required field '{field}'. Skipping record: {record}", "Validator", "warning")
                return None
        return record
    return _validate
