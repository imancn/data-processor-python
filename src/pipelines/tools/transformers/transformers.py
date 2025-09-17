# src/transformers/transformers.py
from typing import Callable, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from core.logging import log_with_timestamp

def apply_transform(
    data: pd.DataFrame,
    transform_func: Callable[[pd.DataFrame], pd.DataFrame],
    name: str = "Transformer"
) -> pd.DataFrame:
    """
    Applies a transformation function to a pandas DataFrame.
    """
    if data.empty:
        log_with_timestamp(f"No data to transform for {name}", name, "info")
        return pd.DataFrame()

    if not isinstance(data, pd.DataFrame):
        log_with_timestamp(f"Expected DataFrame, got {type(data)} for {name}", name, "error")
        return pd.DataFrame()

    try:
        log_with_timestamp(f"Transforming {len(data)} records with {name}", name)
        transformed_data = transform_func(data)
        
        if not isinstance(transformed_data, pd.DataFrame):
            log_with_timestamp(f"Transform function must return DataFrame, got {type(transformed_data)}", name, "error")
            return pd.DataFrame()
            
        log_with_timestamp(f"Successfully transformed {len(transformed_data)} records with {name}", name)
        return transformed_data
        
    except Exception as e:
        log_with_timestamp(f"Transform failed for {name}: {e}", name, "error")
        return pd.DataFrame()

def create_lambda_transformer(
    transform_func: Callable[[pd.DataFrame], pd.DataFrame],
    name: str = "Lambda Transformer"
):
    """
    Factory function to create a lambda transformer with pandas DataFrame support.
    """
    def transformer(data: pd.DataFrame) -> pd.DataFrame:
        return apply_transform(data, transform_func, name)
    
    return transformer

def create_column_transformer(
    column_mapping: dict,
    drop_extra_columns: bool = True,
    name: str = "Column Transformer"
):
    """
    Create a transformer that maps/renames columns in a DataFrame.
    
    Args:
        column_mapping: Dictionary mapping old column names to new column names
        drop_extra_columns: Whether to drop columns not in the mapping
        name: Name for logging
    """
    def transform_func(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        # Rename columns according to mapping
        df_transformed = df.rename(columns=column_mapping)
        
        # Drop extra columns if requested
        if drop_extra_columns and column_mapping:
            keep_columns = list(column_mapping.values())
            df_transformed = df_transformed[keep_columns]
            
        return df_transformed
    
    return create_lambda_transformer(transform_func, name)

def create_type_converter(
    type_mapping: dict,
    name: str = "Type Converter"
):
    """
    Create a transformer that converts column types in a DataFrame.
    
    Args:
        type_mapping: Dictionary mapping column names to target types
        name: Name for logging
    """
    def transform_func(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        df_converted = df.copy()
        
        for column, target_type in type_mapping.items():
            if column in df_converted.columns:
                try:
                    if target_type == 'datetime':
                        df_converted[column] = pd.to_datetime(df_converted[column])
                    elif target_type == 'numeric':
                        df_converted[column] = pd.to_numeric(df_converted[column], errors='coerce')
                    elif target_type == 'string':
                        df_converted[column] = df_converted[column].astype(str)
                    elif target_type == 'category':
                        df_converted[column] = df_converted[column].astype('category')
                    else:
                        df_converted[column] = df_converted[column].astype(target_type)
                except Exception as e:
                    log_with_timestamp(f"Failed to convert column {column} to {target_type}: {e}", name, "warning")
                    
        return df_converted
    
    return create_lambda_transformer(transform_func, name)
def create_filter_transformer(
    filter_func: Callable[[pd.DataFrame], pd.Series],
    name: str = "Filter Transformer"
):
    """
    Create a transformer that filters rows based on a condition.
    
    Args:
        filter_func: Function that takes a DataFrame and returns a boolean Series
        name: Name for logging
    """
    def transform_func(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        try:
            mask = filter_func(df)
            filtered_df = df[mask]
            log_with_timestamp(f"Filtered {len(df) - len(filtered_df)} rows, kept {len(filtered_df)}", name)
            return filtered_df
        except Exception as e:
            log_with_timestamp(f"Filter failed: {e}", name, "error")
            return df
    
    return create_lambda_transformer(transform_func, name)

def create_aggregation_transformer(
    groupby_columns: list,
    aggregation_dict: dict,
    name: str = "Aggregation Transformer"
):
    """
    Create a transformer that performs groupby aggregations.
    
    Args:
        groupby_columns: Columns to group by
        aggregation_dict: Dictionary mapping columns to aggregation functions
        name: Name for logging
    """
    def transform_func(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        try:
            grouped = df.groupby(groupby_columns).agg(aggregation_dict)
            # Flatten multi-level column names
            grouped.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in grouped.columns]
            grouped = grouped.reset_index()
            log_with_timestamp(f"Aggregated {len(df)} rows into {len(grouped)} groups", name)
            return grouped
        except Exception as e:
            log_with_timestamp(f"Aggregation failed: {e}", name, "error")
            return df
    
    return create_lambda_transformer(transform_func, name)

def create_timezone_converter(
    timezone_columns: list,
    from_tz: str = 'UTC',
    to_tz: str = 'Asia/Tehran',
    name: str = "Timezone Converter"
):
    """
    Create a transformer that converts timezone for datetime columns.
    
    Args:
        timezone_columns: List of datetime columns to convert
        from_tz: Source timezone
        to_tz: Target timezone
        name: Name for logging
    """
    def transform_func(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        df_converted = df.copy()
        
        for column in timezone_columns:
            if column in df_converted.columns:
                try:
                    # Convert to datetime if not already
                    if not pd.api.types.is_datetime64_any_dtype(df_converted[column]):
                        df_converted[column] = pd.to_datetime(df_converted[column])
                    
                    # Convert timezone
                    df_converted[column] = df_converted[column].dt.tz_localize(from_tz).dt.tz_convert(to_tz)
                except Exception as e:
                    log_with_timestamp(f"Failed to convert timezone for column {column}: {e}", name, "warning")
                    
        return df_converted
    
    return create_lambda_transformer(transform_func, name)

def create_validation_transformer(
    validation_rules: dict,
    name: str = "Validation Transformer"
):
    """
    Create a transformer that validates data according to rules.
    
    Args:
        validation_rules: Dictionary mapping columns to validation functions
        name: Name for logging
    """
    def transform_func(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        df_validated = df.copy()
        validation_errors = []
        
        for column, validation_func in validation_rules.items():
            if column in df_validated.columns:
                try:
                    mask = validation_func(df_validated[column])
                    invalid_count = (~mask).sum()
                    if invalid_count > 0:
                        validation_errors.append(f"{column}: {invalid_count} invalid values")
                        # Optionally filter out invalid rows
                        # df_validated = df_validated[mask]
                except Exception as e:
                    validation_errors.append(f"{column}: validation error - {e}")
        
        if validation_errors:
            log_with_timestamp(f"Validation issues: {'; '.join(validation_errors)}", name, "warning")
        else:
            log_with_timestamp("All validations passed", name)
            
        return df_validated
    
    return create_lambda_transformer(transform_func, name)
