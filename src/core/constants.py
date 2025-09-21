# src/core/constants.py
"""
Constants and configuration values for the data processing framework.

This module centralizes all magic numbers, strings, and configuration
constants used throughout the framework to improve maintainability
and reduce code duplication.
"""

from typing import Dict, Any

# Framework metadata
FRAMEWORK_NAME = "Data Processing Framework"
FRAMEWORK_VERSION = "1.0.0"
FRAMEWORK_AUTHOR = "Data Processing Framework Team"

# Default configuration values
DEFAULT_CONFIG: Dict[str, Any] = {
    'LOG_LEVEL': 'INFO',
    'TIMEOUT': 30,
    'BATCH_SIZE': 1000,
    'MAX_RETRIES': 3,
    'RETRY_DELAY': 1.0,
    'RETRY_BACKOFF': 2.0,
}

# Database configuration
DEFAULT_CLICKHOUSE_CONFIG: Dict[str, Any] = {
    'host': 'localhost',
    'port': 8123,
    'database': 'default',
    'timeout': 30,
    'connect_timeout': 10,
    'send_receive_timeout': 300,
}

# Logging configuration
LOG_LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
LOG_FORMATS = {
    'default': '[%(asctime)s] %(levelname)s: %(message)s',
    'detailed': '[%(asctime)s] %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    'json': '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
}

# HTTP configuration
DEFAULT_HTTP_CONFIG: Dict[str, Any] = {
    'timeout': 30,
    'max_retries': 3,
    'retry_delay': 1.0,
    'retry_backoff': 2.0,
    'user_agent': f'{FRAMEWORK_NAME}/{FRAMEWORK_VERSION}',
}

# Pipeline configuration
PIPELINE_PATTERNS = {
    'pipeline_file': '*_pipeline.py',
    'migration_file': '*.sql',
    'config_file': '*.env',
}

# Time scopes for data processing
TIME_SCOPES = [
    'latest',
    'hourly', 
    'daily',
    'weekly',
    'monthly',
    'yearly'
]

# Status codes and messages
STATUS_CODES = {
    'SUCCESS': 0,
    'ERROR': 1,
    'WARNING': 2,
    'SKIPPED': 3,
}

STATUS_MESSAGES = {
    0: 'Success',
    1: 'Error', 
    2: 'Warning',
    3: 'Skipped',
}

# File extensions
SUPPORTED_EXTENSIONS = {
    'python': ['.py'],
    'sql': ['.sql'],
    'config': ['.env', '.ini', '.conf'],
    'data': ['.json', '.csv', '.parquet'],
}

# Environment variable names
ENV_VARS = {
    'CLICKHOUSE_HOST': 'CLICKHOUSE_HOST',
    'CLICKHOUSE_PORT': 'CLICKHOUSE_PORT', 
    'CLICKHOUSE_USER': 'CLICKHOUSE_USER',
    'CLICKHOUSE_PASSWORD': 'CLICKHOUSE_PASSWORD',
    'CLICKHOUSE_DATABASE': 'CLICKHOUSE_DATABASE',
    'LOG_DIR': 'LOG_DIR',
    'LOG_LEVEL': 'LOG_LEVEL',
    'TIMEOUT': 'TIMEOUT',
    'BATCH_SIZE': 'BATCH_SIZE',
}

# Error messages
ERROR_MESSAGES = {
    'CONFIG_NOT_FOUND': 'Configuration file not found: {}',
    'DATABASE_CONNECTION_FAILED': 'Failed to connect to database: {}',
    'PIPELINE_NOT_FOUND': 'Pipeline not found: {}',
    'INVALID_TIME_SCOPE': 'Invalid time scope: {}. Must be one of: {}',
    'MIGRATION_FAILED': 'Migration failed: {}',
    'EXTRACTION_FAILED': 'Data extraction failed: {}',
    'TRANSFORMATION_FAILED': 'Data transformation failed: {}',
    'LOADING_FAILED': 'Data loading failed: {}',
}

# Success messages
SUCCESS_MESSAGES = {
    'PIPELINE_COMPLETED': 'Pipeline completed successfully: {}',
    'MIGRATION_COMPLETED': 'Migration completed: {}',
    'DATABASE_CONNECTED': 'Connected to database: {}',
    'CONFIG_LOADED': 'Configuration loaded from: {}',
}

# Directory names
DIRECTORIES = {
    'logs': 'logs',
    'system_logs': 'logs/system',
    'job_logs': 'logs/jobs', 
    'migrations': 'migrations',
    'sql': 'migrations/sql',
    'src': 'src',
    'pipelines': 'src/pipelines',
    'tools': 'src/pipelines/tools',
    'tests': 'tests',
    'docs': 'docs',
}

# File names
FILES = {
    'main_log': 'application.log',
    'cron_log': 'cron.log',
    'migration_table': 'migrations',
    'env_example': 'env.example',
    'requirements': 'requirements.txt',
    'readme': 'README.md',
}
