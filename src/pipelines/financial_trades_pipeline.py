# src/pipelines/financial_trades_pipeline.py
"""
Financial trades ETL pipeline with customer data merging.

This pipeline extracts trades data from allocation.trades_v2 and merges it with
customer data from customer.customers, then loads the merged data to ClickHouse.

Key features:
- Incremental processing with time tracking
- Pagination for large datasets
- Backfill capability for historical data
- Full sync (not incremental) - table stays in sync with sources
- Primary key: trade_uuid only
- All datetime fields stored as Unix timestamps (UInt32)
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

from core.logging import log_with_timestamp
from pipelines.base_pipeline import MetabasePipeline, ClickHousePipeline
from pipelines.tools.extractors.metabase_paginated_extractor import create_metabase_paginated_extractor
from pipelines.tools.data_utils import (
    convert_to_timestamp, clean_data_for_clickhouse, deduplicate_data,
    add_merge_metadata, prepare_datetime_columns
)
from pipelines.tools.clickhouse_replace_loader import create_clickhouse_replace_loader
from pipelines.pipeline_registry import pipeline_registry

# Database and table configuration
ALLOCATION_DATABASE_ID = 10  # allocation database ID
CUSTOMER_DATABASE_ID = 15    # customer database ID
TARGET_TABLE = "financial_trades_latest"
UNIQUE_KEY_COLUMNS = ["trade_uuid"]  # Only trade_uuid as primary key


class FinancialTradesPipeline(MetabasePipeline, ClickHousePipeline):
    """
    Financial trades ETL pipeline with customer data merging.
    
    This pipeline inherits from both MetabasePipeline and ClickHousePipeline
    to get common functionality for both data sources.
    """
    
    def __init__(self):
        super().__init__(
            name="financial_trades",
            description="Financial trades pipeline with customer data merging",
            schedule="* * * * *"  # Every minute
        )
        
        # Initialize extractors
        self.trades_extractor = create_metabase_paginated_extractor(ALLOCATION_DATABASE_ID)
        self.customers_extractor = create_metabase_paginated_extractor(CUSTOMER_DATABASE_ID)
        
        # Initialize loader
        self.loader = create_clickhouse_replace_loader(
            table_name=TARGET_TABLE,
            key_columns=UNIQUE_KEY_COLUMNS,
            sort_column='updated_at_trade',
            name="Financial Trades Loader"
        )
    
    async def extract(self) -> pd.DataFrame:
        """Extract trades data from allocation.trades_v2 table."""
        # Get time range from backfill manager
        start_time, end_time, is_backfill = self.get_time_range()
        
        if is_backfill:
            log_with_timestamp(f"Backfill mode: Extracting trades data from {start_time} to {end_time}", "Trades Extractor")
        else:
            log_with_timestamp(f"Incremental mode: Extracting trades data from {start_time} to {end_time}", "Trades Extractor")
        
        # Build the query
        query = f"""
        SELECT 
            trade_uuid,
            customer_uuid,
            account_uuid,
            order_uuid,
            external_trace_id,
            market_type,
            position_type,
            order_side_type,
            price,
            amount,
            platform_fee,
            fee_bonus,
            value_in_irr,
            created_at,
            updated_at,
            match_engine_created_at,
            match_engine_trace_uuid,
            order_class_type
        FROM trades_v2
        WHERE updated_at >= '{start_time.isoformat()}'
        AND updated_at <= '{end_time.isoformat()}'
        AND trade_uuid IS NOT NULL
        ORDER BY updated_at ASC
        """
        
        # Extract data with pagination
        data = await self.trades_extractor.extract_from_query(query, "Trades V2 Extractor")
        
        # Filter out records with null trade_uuid
        if not data.empty:
            data = data.dropna(subset=['trade_uuid'])
            
            # Update last processed time to the latest record
            if 'updated_at' in data.columns:
                latest_time = data['updated_at'].max()
                self.update_last_processed_time(latest_time)
        
        return data
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform and merge trades data with customer data."""
        if data.empty:
            log_with_timestamp("No trades data to transform", "Data Transformer", "warning")
            return pd.DataFrame()

        log_with_timestamp(f"Transforming and merging {len(data)} trades records", "Data Transformer")

        # Extract unique customer UUIDs from trades data
        customer_uuids = data['customer_uuid'].dropna().unique().tolist()
        log_with_timestamp(f"Found {len(customer_uuids)} unique customer UUIDs in trades data", "Data Transformer")

        if not customer_uuids:
            log_with_timestamp("No customer UUIDs found in trades data", "Data Transformer", "warning")
            return self._create_trades_only_dataframe(data)

        # Extract customer data for only the UUIDs found in trades
        customers_data = await self._extract_customers_data(customer_uuids)

        if customers_data.empty:
            log_with_timestamp("No customer data available for the found UUIDs", "Data Transformer", "warning")
            return self._create_trades_only_dataframe(data)

        # Prepare data for merging
        trades_data = self._prepare_trades_data(data)
        customers_data = self._prepare_customers_data(customers_data)

        # Merge trades with customers on customer_uuid
        merged_data = trades_data.merge(
            customers_data,
            on='customer_uuid',
            how='left',
            suffixes=('_trade', '_customer')
        )

        # Add merge metadata
        merged_data = self._add_merge_metadata(merged_data, 'success')

        log_with_timestamp(f"Successfully merged {len(merged_data)} records", "Data Transformer")
        return merged_data
    
    async def load(self, data: pd.DataFrame) -> bool:
        """Load merged data to ClickHouse with deduplication."""
        if data.empty:
            log_with_timestamp("No data to load", "ClickHouse Loader", "warning")
            return False

        log_with_timestamp(f"Loading {len(data)} records to ClickHouse", "ClickHouse Loader")
        
        # Get all unique trade_uuids from the current batch
        trade_uuids = data['trade_uuid'].dropna().unique().tolist()
        
        if not trade_uuids:
            log_with_timestamp("No valid trade_uuids found in data", "ClickHouse Loader", "warning")
            return False
        
        # Deduplicate data at application level before inserting
        data_deduplicated = deduplicate_data(data, UNIQUE_KEY_COLUMNS, 'updated_at_trade')
        
        # Use the replace loader
        result = await self.loader(data_deduplicated)
        
        if result:
            log_with_timestamp(f"Successfully loaded {len(data_deduplicated)} records to {TARGET_TABLE}", "ClickHouse Loader")
        else:
            log_with_timestamp(f"Failed to load data to {TARGET_TABLE}", "ClickHouse Loader", "error")
        
        return result
    
    async def _extract_customers_data(self, customer_uuids: List[str]) -> pd.DataFrame:
        """Extract customer data for specific UUIDs from customer.customers table."""
        if not customer_uuids:
            log_with_timestamp("No customer UUIDs provided", "Customer Extractor", "warning")
            return pd.DataFrame()
        
        # Create UUID list for SQL IN clause
        uuid_list = "', '".join(customer_uuids)
        
        query = f"""
        SELECT 
            customer_uuid,
            phone_number,
            email,
            password,
            sex,
            first_name,
            last_name,
            national_id,
            date_of_birth,
            landline_number,
            postal_code,
            personal_image_path,
            national_card_front_page_path,
            kyc_image_path,
            address,
            register_totp,
            kyc_level_type,
            customer_type,
            customer_status_type,
            created_at,
            updated_at,
            customer_activity_level_type,
            national_card_image_status_type,
            personal_image_status_type,
            two_factor_status,
            avatar_path,
            email_verification_status_type,
            kyc_flow_type,
            kyc_video_status_type,
            personal_image_file_uuid,
            national_card_file_uuid,
            kyc_video_file_uuid
        FROM customers
        WHERE customer_uuid IN ('{uuid_list}')
        AND customer_uuid IS NOT NULL
        """
        
        # Extract data with pagination
        data = await self.customers_extractor.extract_from_query(query, "Customers Extractor")
        
        # Filter out records with null customer_uuid
        if not data.empty:
            data = data.dropna(subset=['customer_uuid'])
        
        return data
    
    def _prepare_trades_data(self, trades_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare trades data for merging with timestamp conversion."""
        trades_data = trades_data.copy()
        
        # Convert datetime fields to timestamps
        datetime_mappings = {
            'created_at': 'created_at_trade',
            'updated_at': 'updated_at_trade',
            'match_engine_created_at': 'match_engine_created_at'
        }
        
        return prepare_datetime_columns(trades_data, datetime_mappings)
    
    def _prepare_customers_data(self, customers_data: pd.DataFrame) -> pd.DataFrame:
        """Prepare customer data for merging with timestamp conversion."""
        customers_data = customers_data.copy()
        
        # Convert datetime fields to timestamps
        datetime_mappings = {
            'created_at': 'created_at_customer',
            'updated_at': 'updated_at_customer',
            'date_of_birth': 'date_of_birth'
        }
        
        return prepare_datetime_columns(customers_data, datetime_mappings)
    
    def _create_trades_only_dataframe(self, trades_data: pd.DataFrame) -> pd.DataFrame:
        """Create a dataframe with trades data only when customer data is not available."""
        trades_data = self._prepare_trades_data(trades_data)
        
        # Add empty customer columns
        customer_columns = [
            'phone_number', 'email', 'password', 'sex', 'first_name', 'last_name',
            'national_id', 'date_of_birth', 'landline_number', 'postal_code',
            'personal_image_path', 'national_card_front_page_path', 'kyc_image_path',
            'address', 'register_totp', 'kyc_level_type', 'customer_type',
            'customer_status_type', 'customer_activity_level_type',
            'national_card_image_status_type', 'personal_image_status_type',
            'two_factor_status', 'avatar_path', 'email_verification_status_type',
            'kyc_flow_type', 'kyc_video_status_type', 'personal_image_file_uuid',
            'national_card_file_uuid', 'kyc_video_file_uuid', 'created_at_customer',
            'updated_at_customer'
        ]
        
        for col in customer_columns:
            trades_data[col] = None
        
        # Add merge metadata
        return self._add_merge_metadata(trades_data, 'trades_only')


# Legacy functions for backward compatibility
def create_financial_trades_pipeline() -> Dict[str, Any]:
    """Create the financial trades ETL pipeline (legacy function)."""
    pipeline = FinancialTradesPipeline()
    return pipeline.get_pipeline_info()

def run_backfill(days: int) -> bool:
    """Run backfill for specified number of days (legacy function)."""
    pipeline = FinancialTradesPipeline()
    return pipeline.run_backfill(days)

def register_pipelines():
    """Register pipelines for auto-discovery (legacy function)."""
    pipeline = FinancialTradesPipeline()
    pipeline_registry.register_pipeline(pipeline)
    log_with_timestamp(f"Registered pipeline: {pipeline.name}", "Pipeline Registry")