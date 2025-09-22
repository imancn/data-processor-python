-- Create financial_trades_latest table with Unix timestamps (UInt32)
-- This table stores financial trades data merged with customer information
-- All datetime fields are stored as Unix timestamps to avoid timezone issues

CREATE TABLE IF NOT EXISTS financial_trades_latest (
    trade_uuid String,
    customer_uuid Nullable(String),
    account_uuid Nullable(String),
    order_uuid Nullable(String),
    external_trace_id Nullable(String),
    market_type Nullable(String),
    position_type Nullable(String),
    order_side_type Nullable(String),
    price Nullable(Decimal(18, 2)),
    amount Nullable(Decimal(18, 2)),
    platform_fee Nullable(Decimal(18, 2)),
    fee_bonus Nullable(Decimal(18, 2)),
    value_in_irr Nullable(Decimal(18, 2)),
    created_at_trade Nullable(UInt32), -- Unix timestamp
    updated_at_trade UInt32, -- Unix timestamp (non-nullable for ReplacingMergeTree)
    match_engine_created_at Nullable(UInt32), -- Unix timestamp
    match_engine_trace_uuid Nullable(String),
    order_class_type Nullable(String),
    
    -- Customer fields
    phone_number Nullable(String),
    email Nullable(String),
    password Nullable(String),
    sex Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),
    national_id Nullable(String),
    date_of_birth Nullable(UInt32), -- Unix timestamp
    landline_number Nullable(String),
    postal_code Nullable(String),
    personal_image_path Nullable(String),
    national_card_front_page_path Nullable(String),
    kyc_image_path Nullable(String),
    address Nullable(String),
    register_totp Nullable(String),
    kyc_level_type Nullable(String),
    customer_type Nullable(String),
    customer_status_type Nullable(String),
    created_at_customer Nullable(UInt32), -- Unix timestamp
    updated_at_customer Nullable(UInt32), -- Unix timestamp
    customer_activity_level_type Nullable(String),
    national_card_image_status_type Nullable(String),
    personal_image_status_type Nullable(String),
    two_factor_status Nullable(String),
    avatar_path Nullable(String),
    email_verification_status_type Nullable(String),
    kyc_flow_type Nullable(String),
    kyc_video_status_type Nullable(String),
    personal_image_file_uuid Nullable(String),
    national_card_file_uuid Nullable(String),
    kyc_video_file_uuid Nullable(String),
    
    -- Pipeline metadata
    merged_at Nullable(UInt32), -- Unix timestamp
    merge_status Nullable(String),
    pipeline_name Nullable(String)
) ENGINE = ReplacingMergeTree(updated_at_trade)
ORDER BY (trade_uuid);