# Migration Guide: Old to New Architecture

## Overview

This guide helps you migrate from the old function-based pipeline architecture to the new class-based architecture.

## 🔄 Key Changes

### 1. Pipeline Creation

**Old Approach:**
```python
def create_my_pipeline():
    extractor = create_http_extractor(...)
    transformer = create_lambda_transformer(...)
    loader = create_console_loader(...)
    
    return {
        'pipeline': create_etl_pipeline(extractor, transformer, loader, "My Pipeline"),
        'description': 'My data pipeline',
        'schedule': '0 * * * *'
    }
```

**New Approach:**
```python
class MyPipeline(MetabasePipeline, ClickHousePipeline):
    def __init__(self):
        super().__init__(
            name="my_pipeline",
            description="My data pipeline",
            schedule="0 * * * *"
        )
        
        self.extractor = create_http_extractor(...)
        self.loader = create_console_loader(...)
    
    async def extract(self) -> pd.DataFrame:
        return await self.extractor()
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data
    
    async def load(self, data: pd.DataFrame) -> bool:
        return await self.loader(data)
```

### 2. Data Processing

**Old Approach:**
```python
def _convert_to_timestamp(series, field_name):
    # Custom timestamp conversion
    pass

def _clean_data_for_clickhouse(data):
    # Custom data cleaning
    pass
```

**New Approach:**
```python
from pipelines.tools.data_utils import (
    convert_to_timestamp,
    clean_data_for_clickhouse,
    deduplicate_data,
    add_merge_metadata
)

# Use generic utilities
timestamps = convert_to_timestamp(series, field_name)
cleaned_data = clean_data_for_clickhouse(data)
```

### 3. Pagination

**Old Approach:**
```python
def extract_with_pagination():
    # Custom pagination logic
    pass
```

**New Approach:**
```python
from pipelines.tools.pagination_utils import extract_with_pagination

data = await extract_with_pagination(
    extractor_func=create_extractor_func,
    base_query="SELECT * FROM my_table",
    batch_size=2000
)
```

## 📋 Migration Steps

### Step 1: Create New Pipeline Class
1. Inherit from appropriate base classes (`MetabasePipeline`, `ClickHousePipeline`)
2. Implement `__init__()` method
3. Implement `extract()`, `transform()`, `load()` methods

### Step 2: Replace Custom Utilities
1. Use `convert_to_timestamp()` instead of custom conversion
2. Use `clean_data_for_clickhouse()` instead of custom cleaning
3. Use `deduplicate_data()` for data deduplication
4. Use `add_merge_metadata()` for metadata

### Step 3: Use Generic Extractors
1. Use `create_metabase_paginated_extractor()` for Metabase
2. Use `create_clickhouse_replace_loader()` for ClickHouse
3. Use `extract_with_pagination()` for large datasets

### Step 4: Register Pipeline
```python
from pipelines.pipeline_registry import pipeline_registry

def register_pipelines():
    pipeline = MyPipeline()
    pipeline_registry.register_pipeline(pipeline)
```

### Step 5: Add Legacy Functions (Optional)
```python
# For backward compatibility
def create_my_pipeline() -> Dict[str, Any]:
    pipeline = MyPipeline()
    return pipeline.get_pipeline_info()

def run_backfill(days: int) -> bool:
    pipeline = MyPipeline()
    return pipeline.run_backfill(days)
```

## ✅ Benefits After Migration

- **Cleaner Code**: No duplicate implementations
- **Better Organization**: Clear separation of concerns
- **Easier Maintenance**: Centralized utilities
- **Type Safety**: Full type hints and validation
- **Professional Structure**: Class-based design

## 🆘 Need Help?

- [Quick Start Guide](./quick-start.md) - Examples and setup
- [API Reference](./api-reference.md) - New classes and methods
- [Developer Guide](./developer-guide.md) - Best practices