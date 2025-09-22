# Developer Guide

Guidelines for contributing to and extending the Data Processing Framework.

## 🚀 Development Setup

```bash
# Clone and setup
git clone <repository-url>
cd data-processor
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt
```

## 🏗️ Project Structure

```
data-processor/
├── src/
│   ├── core/                   # Framework foundation
│   └── pipelines/              # Pipeline system
│       ├── base_pipeline.py   # Base pipeline classes
│       ├── pipeline_registry.py # Centralized registry
│       └── tools/              # Generic utilities
│           ├── data_utils.py
│           ├── pagination_utils.py
│           ├── backfill_utils.py
│           ├── extractors/
│           ├── loaders/
│           └── transformers/
├── tests/                      # Test suite
├── docs/                       # Documentation
├── migrations/                 # Database migrations
└── scripts/                    # Utility scripts
```

## 🧩 Core Components

### Configuration System
- **File**: `src/core/config.py`
- **Models**: `src/core/models.py`
- **Adding Config**: Add field to `FrameworkSettings` in `models.py`

### Logging System
- **File**: `src/core/logging.py`
- **Usage**: `log_with_timestamp("Message", "Component", "level")`

### Pipeline System
- **Base Classes**: `BasePipeline`, `MetabasePipeline`, `ClickHousePipeline`
- **Registry**: Centralized pipeline management
- **Tools**: Generic utilities for common tasks

## 🔧 Creating New Pipelines

### Using Class-Based Approach

```python
from pipelines.base_pipeline import MetabasePipeline, ClickHousePipeline
from pipelines.tools.data_utils import prepare_datetime_columns, add_merge_metadata
from pipelines.pipeline_registry import pipeline_registry

class MyNewPipeline(MetabasePipeline, ClickHousePipeline):
    def __init__(self):
        super().__init__(
            name="my_new_pipeline",
            description="My new ETL pipeline",
            schedule="0 */6 * * *"
        )
        
        # Initialize components
        self.extractor = create_metabase_paginated_extractor(database_id=1)
        self.loader = create_clickhouse_replace_loader(
            table_name="my_table",
            key_columns=["id"],
            sort_column="updated_at"
        )
    
    async def extract(self) -> pd.DataFrame:
        query = "SELECT * FROM my_table WHERE updated_at >= '2024-01-01'"
        return await self.extractor.extract_from_query(query, "My Extractor")
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data = prepare_datetime_columns(data, {'created_at': 'created_at'})
        data = self._add_merge_metadata(data, 'success')
        return data
    
    async def load(self, data: pd.DataFrame) -> bool:
        return await self.loader(data)

# Register pipeline
def register_pipelines():
    pipeline = MyNewPipeline()
    pipeline_registry.register_pipeline(pipeline)
```

### Benefits
- **Inheritance**: Get common functionality from base classes
- **Generic Utilities**: Use pre-built data processing tools
- **Consistent Structure**: All pipelines follow the same pattern
- **Easy Testing**: Clear separation of concerns

## 🛠️ Adding New Components

### New Extractor
```python
# Create src/pipelines/tools/extractors/my_extractor.py
import pandas as pd
from typing import Dict, Any, Callable, Awaitable

async def create_my_extractor(config: Dict[str, Any]) -> Callable[[], Awaitable[pd.DataFrame]]:
    async def extractor() -> pd.DataFrame:
        # Your extraction logic
        return pd.DataFrame()
    return extractor
```

### New Transformer
```python
# Create src/pipelines/tools/transformers/my_transformer.py
import pandas as pd
from typing import Callable

def create_my_transformer(transform_func: Callable, name: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
    def transformer(data: pd.DataFrame) -> pd.DataFrame:
        return transform_func(data)
    return transformer
```

### New Loader
```python
# Create src/pipelines/tools/loaders/my_loader.py
import pandas as pd
from typing import Callable, Awaitable

async def create_my_loader(config: Dict[str, Any]) -> Callable[[pd.DataFrame], Awaitable[bool]]:
    async def loader(data: pd.DataFrame) -> bool:
        # Your loading logic
        return True
    return loader
```

## 🧪 Testing

### Running Tests
```bash
./run.sh test                   # All tests
./run.sh test unit              # Unit tests only
./run.sh test integration       # Integration tests
./run.sh test --fast            # Fast tests only
```

### Test Structure
```
tests/
├── unit/                       # Component tests
├── integration/                # End-to-end tests
├── performance/                # Performance tests
└── results/                    # Test results
```

### Writing Tests
```python
import pytest
import pandas as pd
from pipelines.my_pipeline import MyPipeline

@pytest.mark.asyncio
async def test_pipeline_extract():
    pipeline = MyPipeline()
    data = await pipeline.extract()
    assert not data.empty
    assert 'id' in data.columns
```

## 📝 Code Standards

### Python Style
- Use type hints for all functions
- Follow PEP 8 naming conventions
- Document all public methods
- Use async/await for I/O operations

### Pipeline Standards
- Inherit from appropriate base classes
- Use generic utilities when possible
- Handle errors gracefully
- Add comprehensive logging

### Testing Standards
- Write unit tests for all components
- Include integration tests for pipelines
- Aim for >80% code coverage
- Test error conditions

## 🚀 Deployment

### Local Testing
```bash
./run.sh check                  # Check dependencies
./run.sh test                   # Run tests
./run.sh list                   # List pipelines
```

### Production Deployment
```bash
./deploy.sh username hostname   # Deploy to server
```

## 🔍 Debugging

### Logs
- **System**: `logs/system/application.log`
- **Jobs**: `logs/jobs/{job_name}.log`
- **Cron**: `logs/cron.log`

### Common Issues
- **Import Errors**: Check Python path and dependencies
- **Database Errors**: Verify ClickHouse configuration
- **Pipeline Errors**: Check logs for specific error messages

## 📚 Resources

- [Quick Start Guide](./quick-start.md)
- [API Reference](./api-reference.md)
- [Architecture Overview](./architecture/README.md)
- [Migration Guide](./migration-guide.md)