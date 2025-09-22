# API Reference

Complete API documentation for the Data Processing Framework.

## 📋 Table of Contents

- [Core Framework](#core-framework)
- [Pipeline System](#pipeline-system)
- [New Pipeline Architecture](#new-pipeline-architecture)
- [Pipeline Tools](#pipeline-tools)
- [Configuration](#configuration)
- [Logging](#logging)
- [Validation](#validation)
- [Exceptions](#exceptions)

## 🧩 Core Framework

### Configuration (`src/core/config.py`)

#### `Config` Class

Main configuration manager using Pydantic BaseSettings.

```python
from core.config import config

# Get configuration values
log_level = config.log_level
clickhouse_config = config.get_clickhouse_config()
api_config = config.get_api_config()

# Update configuration
config.update(log_level='DEBUG', timeout=60)
```

**Properties:**
- `log_level: str` - Logging level
- `log_dir: str` - Log directory path
- `log_file: Optional[str]` - Main log file path
- `timeout: int` - Request timeout in seconds
- `batch_size: int` - Batch size for processing

**Methods:**
- `get(key: str, default: Any = None) -> Any` - Get configuration value
- `get_str(key: str, default: str = '') -> str` - Get string value
- `get_int(key: str, default: int = 0) -> int` - Get integer value
- `get_bool(key: str, default: bool = False) -> bool` - Get boolean value
- `get_clickhouse_config() -> Dict[str, Any]` - Get ClickHouse configuration
- `get_api_config() -> Optional[Dict[str, Any]]` - Get API configuration
- `get_metabase_config() -> Dict[str, Any]` - Get Metabase configuration
- `to_dict() -> Dict[str, Any]` - Convert to dictionary
- `update(**kwargs) -> None` - Update configuration values
- `reload() -> None` - Reload from environment

### Logging (`src/core/logging.py`)

#### `setup_logging(level: str, log_file: Optional[str] = None) -> None`

Setup the logging system.

```python
from core.logging import setup_logging

setup_logging('INFO', 'logs/app.log')
```

#### `log_with_timestamp(message: str, component: str, level: str = 'info') -> None`

Log a message with timestamp and component.

```python
from core.logging import log_with_timestamp

log_with_timestamp("Pipeline started", "MyPipeline", "info")
log_with_timestamp("Error occurred", "MyPipeline", "error")
```

#### `PerformanceLogger` Context Manager

Track performance metrics for operations.

```python
from core.logging import PerformanceLogger

with PerformanceLogger("MyOperation", "Pipeline"):
    # Your operation here
    pass
```

#### `@log_pipeline_stage` Decorator

Log pipeline stage execution.

```python
from core.logging import log_pipeline_stage

@log_pipeline_stage('Extraction', 'info')
def extract_data():
    # Extraction logic
    pass
```

## 🔄 Pipeline System

### Pipeline Factory (`src/pipelines/pipeline_factory.py`)

#### `create_etl_pipeline(extractor, transformer, loader, name) -> Callable`

Create an Extract-Transform-Load pipeline.

```python
from pipelines.pipeline_factory import create_etl_pipeline

pipeline = create_etl_pipeline(
    extractor=my_extractor,
    transformer=my_transformer,
    loader=my_loader,
    name="My ETL Pipeline"
)
```

#### `create_el_pipeline(extractor, loader, name) -> Callable`

Create an Extract-Load pipeline.

```python
from pipelines.pipeline_factory import create_el_pipeline

pipeline = create_el_pipeline(
    extractor=my_extractor,
    loader=my_loader,
    name="My EL Pipeline"
)
```

#### `create_parallel_pipeline(pipelines, name) -> Callable`

Create a pipeline that runs multiple pipelines in parallel.

```python
from pipelines.pipeline_factory import create_parallel_pipeline

pipeline = create_parallel_pipeline(
    pipelines=[pipeline1, pipeline2, pipeline3],
    name="Parallel Pipeline"
)
```

#### `create_sequential_pipeline(pipelines, name) -> Callable`

Create a pipeline that runs multiple pipelines sequentially.

```python
from pipelines.pipeline_factory import create_sequential_pipeline

pipeline = create_sequential_pipeline(
    pipelines=[pipeline1, pipeline2, pipeline3],
    name="Sequential Pipeline"
)
```

#### `create_conditional_pipeline(condition_func, true_pipeline, false_pipeline, name) -> Callable`

Create a pipeline that conditionally runs different pipelines.

```python
from pipelines.pipeline_factory import create_conditional_pipeline

def should_run_pipeline():
    return some_condition

pipeline = create_conditional_pipeline(
    condition_func=should_run_pipeline,
    true_pipeline=pipeline_a,
    false_pipeline=pipeline_b,
    name="Conditional Pipeline"
)
```

#### `create_retry_pipeline(pipeline, max_retries, retry_delay, name) -> Callable`

Create a pipeline that retries on failure.

```python
from pipelines.pipeline_factory import create_retry_pipeline

pipeline = create_retry_pipeline(
    pipeline=my_pipeline,
    max_retries=3,
    retry_delay=1.0,
    name="Retry Pipeline"
)
```

### Pipeline Registry (`src/pipelines/pipeline_registry.py`)

#### `PipelineRegistry` Class

Manage pipeline registration and discovery.

```python
from pipelines.pipeline_registry import PipelineRegistry

registry = PipelineRegistry()

# Register a pipeline
registry.register_pipeline('my_pipeline', pipeline_func, config)

# Get pipeline
pipeline = registry.get_pipeline('my_pipeline')

# List all pipelines
pipelines = registry.list_pipelines()
```

## 🏗️ New Pipeline Architecture

### Base Pipeline Classes (`src/pipelines/base_pipeline.py`)

The new architecture provides class-based pipeline development with inheritance patterns.

#### `BasePipeline` Class

Abstract base class for all pipelines.

```python
from pipelines.base_pipeline import BasePipeline

class MyPipeline(BasePipeline):
    def __init__(self):
        super().__init__(
            name="my_pipeline",
            description="My data pipeline",
            schedule="* * * * *"
        )
    
    async def extract(self) -> pd.DataFrame:
        """Extract data from source systems."""
        pass
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the extracted data."""
        pass
    
    async def load(self, data: pd.DataFrame) -> bool:
        """Load the transformed data to destination."""
        pass
```

**Methods:**
- `execute() -> bool` - Execute the complete ETL pipeline
- `run_backfill(days: int) -> bool` - Run backfill for specified days
- `get_time_range() -> Tuple[datetime, datetime, bool]` - Get current time range
- `is_backfill_mode() -> bool` - Check if in backfill mode
- `get_pipeline_info() -> Dict[str, Any]` - Get pipeline information

#### `MetabasePipeline` Class

Base class for pipelines that extract data from Metabase.

```python
from pipelines.base_pipeline import MetabasePipeline

class MyMetabasePipeline(MetabasePipeline):
    def __init__(self):
        super().__init__(
            name="my_metabase_pipeline",
            description="Pipeline using Metabase",
            schedule="0 * * * *"
        )
    
    def _get_metabase_config(self):
        """Get Metabase configuration."""
        return self.metabase_config
```

#### `ClickHousePipeline` Class

Base class for pipelines that load data to ClickHouse.

```python
from pipelines.base_pipeline import ClickHousePipeline

class MyClickHousePipeline(ClickHousePipeline):
    def __init__(self):
        super().__init__(
            name="my_clickhouse_pipeline",
            description="Pipeline using ClickHouse",
            schedule="0 * * * *"
        )
    
    def _get_clickhouse_config(self):
        """Get ClickHouse configuration."""
        return self.clickhouse_config
    
    def _add_merge_metadata(self, data: pd.DataFrame, merge_status: str = 'success') -> pd.DataFrame:
        """Add merge metadata to DataFrame."""
        return add_merge_metadata(data, self.name, merge_status)
```

### Pipeline Registry (`src/pipelines/pipeline_registry.py`)

Centralized registry for managing all pipelines.

#### `PipelineRegistry` Class

```python
from pipelines.pipeline_registry import pipeline_registry

# Register a pipeline
pipeline = MyPipeline()
pipeline_registry.register_pipeline(pipeline)

# Get pipeline information
pipeline_info = pipeline_registry.get_pipeline("my_pipeline")

# List all pipelines
all_pipelines = pipeline_registry.get_all_pipelines()
```

**Methods:**
- `register_pipeline(pipeline_instance: Any) -> bool` - Register a pipeline
- `register_pipeline_class(pipeline_class: Any, name: str, description: str, schedule: str) -> bool` - Register by class
- `get_pipeline(name: str) -> Optional[Dict[str, Any]]` - Get pipeline by name
- `get_all_pipelines() -> Dict[str, Dict[str, Any]]` - Get all pipelines
- `list_pipeline_names() -> List[str]` - List pipeline names
- `remove_pipeline(name: str) -> bool` - Remove pipeline
- `get_scheduled_pipelines() -> List[Dict[str, Any]]` - Get scheduled pipelines

### Generic Utilities (`src/pipelines/tools/`)

#### Data Utilities (`data_utils.py`)

```python
from pipelines.tools.data_utils import (
    convert_to_timestamp,
    clean_data_for_clickhouse,
    deduplicate_data,
    add_merge_metadata,
    prepare_datetime_columns
)

# Convert datetime fields to timestamps
timestamps = convert_to_timestamp(series, "field_name")

# Clean data for ClickHouse
cleaned_data = clean_data_for_clickhouse(data, string_columns)

# Deduplicate data
deduplicated = deduplicate_data(data, key_columns, sort_column)

# Add merge metadata
data_with_metadata = add_merge_metadata(data, pipeline_name, "success")

# Prepare datetime columns
prepared_data = prepare_datetime_columns(data, datetime_mappings)
```

#### Pagination Utilities (`pagination_utils.py`)

```python
from pipelines.tools.pagination_utils import extract_with_pagination, create_paginated_extractor

# Extract with pagination
data = await extract_with_pagination(
    extractor_func=create_extractor_func,
    base_query="SELECT * FROM my_table",
    batch_size=2000,
    name="My Extractor"
)

# Create paginated extractor
extractor = create_paginated_extractor(
    extractor_func=create_extractor_func,
    base_query="SELECT * FROM my_table",
    batch_size=2000
)
```

#### Backfill Utilities (`backfill_utils.py`)

```python
from pipelines.tools.backfill_utils import backfill_manager, run_backfill

# Set backfill time range
backfill_manager.set_backfill_time_range(start_date, end_date, "pipeline_name")

# Get current time range
start_time, end_time, is_backfill = backfill_manager.get_time_range()

# Run backfill
result = run_backfill(pipeline_func, days=30, pipeline_name="my_pipeline")
```

#### ClickHouse Replace Loader (`clickhouse_replace_loader.py`)

```python
from pipelines.tools.clickhouse_replace_loader import create_clickhouse_replace_loader

# Create replace loader
loader = create_clickhouse_replace_loader(
    table_name="my_table",
    key_columns=["id"],
    sort_column="updated_at",
    string_columns=["name", "description"],
    max_retries=3,
    name="My Loader"
)

# Load data with deduplication
result = await loader(data)
```

## 🛠️ Pipeline Tools

### Extractors

#### HTTP Extractor (`src/pipelines/tools/extractors/http_extractor.py`)

```python
from pipelines.tools.extractors.http_extractor import create_http_extractor

extractor = create_http_extractor(
    url="https://api.example.com/data",
    headers={"Accept": "application/json"},
    timeout=30,
    name="API Extractor"
)
```

#### ClickHouse Extractor (`src/pipelines/tools/extractors/clickhouse_extractor.py`)

```python
from pipelines.tools.extractors.clickhouse_extractor import create_clickhouse_extractor

extractor = create_clickhouse_extractor(
    query="SELECT * FROM my_table",
    name="Database Extractor"
)
```

#### Metabase Extractor (`src/pipelines/tools/extractors/metabase_extractor.py`)

Extract data from any table in any database added to Metabase using the Metabase API.

```python
from pipelines.tools.extractors.metabase_extractor import create_metabase_extractor

# Extract from a specific table
extractor = create_metabase_extractor(
    base_url="https://metabase.devinvex.com",
    api_key="CHANGE_ME",
    database_id=1,
    table_id=2,
    limit=1000,
    name="Metabase Table Extractor"
)

# Execute custom SQL query
query_extractor = create_metabase_extractor(
    base_url="https://metabase.devinvex.com",
    api_key="CHANGE_ME",
    database_id=1,
    native_query="SELECT COUNT(*) as total FROM users WHERE active = true",
    name="Metabase Query Extractor"
)

# Using configuration (set METABASE_BASE_URL and METABASE_API_KEY in .env)
config_extractor = create_metabase_extractor(
    database_id=1,
    table_id=2,
    name="Metabase Config Extractor"
)
```

**Configuration:**
```bash
# Add to .env file
METABASE_BASE_URL=https://metabase.devinvex.com
METABASE_API_KEY=CHANGE_ME
METABASE_TIMEOUT=30
```

**Additional Functions:**
```python
from pipelines.tools.extractors.metabase_extractor import (
    get_metabase_databases,
    get_metabase_tables
)

# List available databases
databases = await get_metabase_databases(
    base_url="https://metabase.devinvex.com",
    api_key="CHANGE_ME"
)

# List tables in a database
tables = await get_metabase_tables(
    base_url="https://metabase.devinvex.com",
    api_key="CHANGE_ME",
    database_id=1
)
```

### Transformers

#### Lambda Transformer (`src/pipelines/tools/transformers/transformers.py`)

```python
from pipelines.tools.transformers.transformers import create_lambda_transformer

def transform_data(df):
    df['processed_at'] = pd.Timestamp.now()
    return df

transformer = create_lambda_transformer(transform_data, "Data Transformer")
```

#### Type Converter (`src/pipelines/tools/transformers/transformers.py`)

```python
from pipelines.tools.transformers.transformers import create_type_converter

transformer = create_type_converter(
    column_types={'id': 'int64', 'created_at': 'datetime64'},
    name="Type Converter"
)
```

### Loaders

#### ClickHouse Loader (`src/pipelines/tools/loaders/clickhouse_loader.py`)

```python
from pipelines.tools.loaders.clickhouse_loader import create_clickhouse_loader

loader = create_clickhouse_loader(
    table_name="my_table",
    mode="upsert",
    batch_size=1000,
    name="Database Loader"
)
```

#### Console Loader (`src/pipelines/tools/loaders/console_loader.py`)

```python
from pipelines.tools.loaders.console_loader import create_console_loader

loader = create_console_loader("Console Loader")
```

## ⚙️ Configuration

### Pydantic Models (`src/core/models.py`)

#### `FrameworkSettings`

Main framework configuration model.

```python
from core.models import FrameworkSettings

settings = FrameworkSettings(
    log_level='INFO',
    clickhouse_host='localhost',
    clickhouse_port=8123,
    timeout=30
)
```

#### `PipelineConfig`

Pipeline configuration model.

```python
from core.models import PipelineConfig

config = PipelineConfig(
    name="my_pipeline",
    description="My data pipeline",
    schedule="0 * * * *",
    enabled=True,
    timeout=300
)
```

### Validation (`src/core/validators.py`)

#### `validate_pipeline_config(data: Dict[str, Any]) -> PipelineConfig`

Validate pipeline configuration data.

```python
from core.validators import validate_pipeline_config

config_data = {
    'name': 'my_pipeline',
    'schedule': '0 * * * *',
    'enabled': True
}

config = validate_pipeline_config(config_data)
```

## 📝 Logging

### Log Levels

- `DEBUG` - Detailed information for debugging
- `INFO` - General information about program execution
- `WARNING` - Warning messages for potential issues
- `ERROR` - Error messages for failures

### Log Files

- `logs/system/application.log` - System-level logs
- `logs/jobs/{job_name}.log` - Per-job detailed logs
- `logs/cron.log` - Cron execution logs

## ⚠️ Exceptions

### Exception Hierarchy

```python
FrameworkError
├── ConfigurationError
├── DatabaseError
│   ├── DatabaseConnectionError
│   └── DatabaseQueryError
├── PipelineError
│   ├── PipelineNotFoundError
│   ├── PipelineExecutionError
│   └── PipelineConfigurationError
├── ExtractionError
│   ├── HTTPExtractionError
│   └── DatabaseExtractionError
├── TransformationError
│   └── ValidationError
├── LoadingError
│   └── DatabaseLoadingError
├── MigrationError
│   ├── MigrationNotFoundError
│   └── MigrationExecutionError
├── CronError
│   ├── CronJobNotFoundError
│   └── CronScheduleError
├── TimeoutError
└── RetryError
```

### Using Exceptions

```python
from core.exceptions import PipelineError, create_exception

# Raise specific exception
raise PipelineError("Pipeline execution failed", {'pipeline': 'my_pipeline'})

# Create exception by type
exc = create_exception('pipeline_execution', 'Pipeline failed', {'step': 'extraction'})
raise exc
```

## 🔧 Main Application

### Main Functions (`src/main.py`)

#### `register_cron_job(job_name, pipeline, schedule, description, **kwargs) -> None`

Register a pipeline as a cron job.

```python
from main import register_cron_job

register_cron_job(
    job_name="my_job",
    pipeline=my_pipeline,
    schedule="0 * * * *",
    description="My scheduled job"
)
```

#### `run_cron_job(job_name, *args, **kwargs) -> bool`

Run a specific cron job.

```python
from main import run_cron_job

success = run_cron_job("my_job")
```

#### `list_cron_jobs() -> Dict[str, Any]`

List all registered cron jobs.

```python
from main import list_cron_jobs

jobs = list_cron_jobs()
```

## 📊 Migration System

### Migration Manager (`migrations/migration_manager.py`)

#### `ClickHouseMigrationManager` Class

Manage database migrations with comprehensive status tracking and rollback capabilities.

```python
from migrations.migration_manager import ClickHouseMigrationManager

manager = ClickHouseMigrationManager()

# Run migrations
manager.run_migrations()

# Show status
manager.show_status()

# Get migration status as dictionary
status = manager.get_migration_status()

# Rollback migrations
manager.rollback_migrations(count=1)
```

**Methods:**
- `run_migrations() -> bool` - Run all pending migrations
- `show_status() -> None` - Display migration status in console
- `get_migration_status() -> Dict[str, Any]` - Get migration status as dictionary
- `rollback_migrations(count: int = 1) -> bool` - Rollback the last N migrations
- `get_executed_migrations() -> List[Migration]` - Get list of executed migrations
- `get_pending_migrations() -> List[Migration]` - Get list of pending migrations

## 🧪 Testing

### Test Framework (`tests/framework/`)

#### Running Tests

```bash
# Run all tests
./run.sh test

# Run specific category
./run.sh test unit

# Run fast tests only
./run.sh test --fast
```

#### Test Categories

- `unit` - Unit tests for individual components
- `integration` - Integration tests for component interactions
- `performance` - Performance and load tests
- `deployment` - Deployment and operations tests

## 📚 Additional Resources

- [Architecture Documentation](./architecture/README.md)
- [Quick Start Guide](./quick-start.md)
- [Developer Guide](./developer-guide.md)
- [System Diagrams](./diagrams/README.md)
