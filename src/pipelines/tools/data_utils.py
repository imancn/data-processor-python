# src/pipelines/tools/data_utils.py
"""
Generic data utilities for pipeline processing.

This module provides common data processing utilities that can be reused
across different pipelines.
"""

import pandas as pd
from datetime import datetime
from typing import List, Optional, Dict, Any
from core.logging import log_with_timestamp

def convert_to_timestamp(series: pd.Series, field_name: str) -> pd.Series:
    """
    Convert datetime fields to Unix timestamps (UInt32).
    
    Args:
        series: Pandas Series containing datetime values
        field_name: Name of the field for logging purposes
        
    Returns:
        Pandas Series with Unix timestamps
    """
    if series.empty:
        return series
    
    # Special handling for date_of_birth - set to None to avoid issues
    if field_name == 'date_of_birth':
        return pd.Series([None] * len(series), dtype='object')
    
    # Log the original values for debugging
    log_with_timestamp(f"Converting {field_name} - Original values: {series.head(3).tolist()}", "Timestamp Converter")
    
    # Convert to datetime first, handling various formats
    datetime_series = pd.to_datetime(series, errors='coerce', utc=True)
    
    # Log the converted datetime values
    log_with_timestamp(f"Converted to datetime - Values: {datetime_series.head(3).tolist()}", "Timestamp Converter")
    
    # Convert to Unix timestamp (seconds since epoch)
    def convert_single_timestamp(ts):
        if pd.isna(ts):
            return None
        try:
            # Ensure we have a proper datetime object
            if hasattr(ts, 'timestamp'):
                timestamp = int(ts.timestamp())
                # Validate timestamp is reasonable (not 1 or very small)
                if timestamp < 1000000000:  # Before year 2001
                    log_with_timestamp(f"Warning: Very small timestamp {timestamp} for {field_name}", "Timestamp Converter", "warning")
                    return None
                return timestamp
            else:
                # If it's already a timestamp, return as is
                timestamp = int(ts)
                if timestamp < 1000000000:  # Before year 2001
                    log_with_timestamp(f"Warning: Very small timestamp {timestamp} for {field_name}", "Timestamp Converter", "warning")
                    return None
                return timestamp
        except (ValueError, AttributeError, TypeError) as e:
            log_with_timestamp(f"Error converting timestamp for {field_name}: {e}", "Timestamp Converter", "error")
            return None
    
    timestamp_series = datetime_series.apply(convert_single_timestamp)
    
    # Log some sample conversions for debugging
    if not timestamp_series.empty:
        sample_values = timestamp_series.dropna().head(3).tolist()
        log_with_timestamp(f"Sample {field_name} timestamps: {sample_values}", "Timestamp Converter")
        
        # Check for any remaining 1 values
        ones_count = (timestamp_series == 1).sum()
        if ones_count > 0:
            log_with_timestamp(f"Warning: {ones_count} records still have timestamp value 1 for {field_name}", "Timestamp Converter", "warning")
    
    return timestamp_series

def clean_data_for_clickhouse(data: pd.DataFrame, string_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Clean data for ClickHouse insertion with proper string handling.
    
    Args:
        data: DataFrame to clean
        string_columns: List of string column names to clean (optional)
        
    Returns:
        Cleaned DataFrame ready for ClickHouse insertion
    """
    data_copy = data.copy()
    
    # Default string columns if not provided
    if string_columns is None:
        string_columns = [
            'trade_uuid', 'customer_uuid', 'account_uuid', 'order_uuid', 'external_trace_id',
            'market_type', 'position_type', 'order_side_type', 'match_engine_trace_uuid',
            'order_class_type', 'phone_number', 'email', 'password', 'sex', 'first_name',
            'last_name', 'national_id', 'landline_number', 'postal_code', 'personal_image_path',
            'national_card_front_page_path', 'kyc_image_path', 'address', 'register_totp',
            'kyc_level_type', 'customer_type', 'customer_status_type', 'customer_activity_level_type',
            'national_card_image_status_type', 'personal_image_status_type', 'two_factor_status',
            'avatar_path', 'email_verification_status_type', 'kyc_flow_type', 'kyc_video_status_type',
            'personal_image_file_uuid', 'national_card_file_uuid', 'kyc_video_file_uuid',
            'merge_status', 'pipeline_name'
        ]
    
    # Clean string columns
    for col in string_columns:
        if col in data_copy.columns:
            # Convert to string and handle None values
            data_copy[col] = data_copy[col].astype(str).replace('nan', None)
            data_copy[col] = data_copy[col].where(data_copy[col] != 'None', None)
    
    # Log sample timestamp values before cleaning
    timestamp_columns = ['created_at_trade', 'updated_at_trade', 'match_engine_created_at',
                        'created_at_customer', 'updated_at_customer', 'date_of_birth', 'merged_at']
    
    for col in timestamp_columns:
        if col in data_copy.columns and not data_copy[col].empty:
            sample_values = data_copy[col].dropna().head(3).tolist()
            log_with_timestamp(f"Before cleaning - Sample {col} values: {sample_values}", "Data Cleaner")
    
    return data_copy

def deduplicate_data(data: pd.DataFrame, key_columns: List[str], sort_column: str) -> pd.DataFrame:
    """
    Deduplicate data by keeping the record with the highest value in sort_column.
    
    Args:
        data: DataFrame to deduplicate
        key_columns: Columns to use for deduplication
        sort_column: Column to sort by (highest value wins)
        
    Returns:
        Deduplicated DataFrame
    """
    if data.empty:
        return data
    
    # Sort by sort_column in descending order and drop duplicates
    deduplicated = data.sort_values(sort_column, ascending=False).drop_duplicates(subset=key_columns, keep='first')
    
    log_with_timestamp(f"After deduplication - Records: {len(deduplicated)}, Unique keys: {len(deduplicated[key_columns[0]].unique())}", "Data Deduplicator")
    
    return deduplicated

def add_merge_metadata(data: pd.DataFrame, pipeline_name: str, merge_status: str = 'success') -> pd.DataFrame:
    """
    Add merge metadata columns to DataFrame.
    
    Args:
        data: DataFrame to add metadata to
        pipeline_name: Name of the pipeline
        merge_status: Status of the merge operation
        
    Returns:
        DataFrame with added metadata columns
    """
    data_copy = data.copy()
    
    # Add merge metadata with timestamp
    data_copy['merged_at'] = int(datetime.now().timestamp())
    data_copy['merge_status'] = merge_status
    data_copy['pipeline_name'] = pipeline_name
    
    return data_copy

def prepare_datetime_columns(data: pd.DataFrame, datetime_mappings: Dict[str, str]) -> pd.DataFrame:
    """
    Prepare datetime columns by converting them to timestamp format.
    
    Args:
        data: DataFrame to process
        datetime_mappings: Dictionary mapping original column names to new column names
        
    Returns:
        DataFrame with converted datetime columns
    """
    data_copy = data.copy()
    
    for original_col, new_col in datetime_mappings.items():
        if original_col in data_copy.columns:
            data_copy[new_col] = convert_to_timestamp(data_copy[original_col], new_col)
            # Drop the original column to avoid conflicts
            data_copy = data_copy.drop(columns=[original_col])
    
    return data_copy

# Public API
__all__ = [
    'convert_to_timestamp',
    'clean_data_for_clickhouse',
    'deduplicate_data',
    'add_merge_metadata',
    'prepare_datetime_columns',
]
