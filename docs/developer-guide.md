# Developer Guide

Guidelines for contributing to and extending the Data Processing Framework.

## 🚀 Getting Started

### Prerequisites

- Python 3.9 or higher
- Git
- Basic understanding of ETL/ELT concepts
- Familiarity with pandas, ClickHouse, and async programming

### Development Setup

```bash
# Clone the repository
git clone <repository-url>
cd data-processor

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install development dependencies
pip install -r requirements-test.txt

# Setup pre-commit hooks (optional)
pre-commit install
```

## 🏗️ Project Structure

```
data-processor/
├── src/                    # Source code
│   ├── core/              # Core framework
│   └── pipelines/         # Pipeline system
├── tests/                 # Test suite
├── docs/                  # Documentation
├── migrations/            # Database migrations
├── scripts/               # Utility scripts
└── logs/                  # Log files
```

## 🧩 Core Components

### Configuration System

The configuration system uses Pydantic for validation and type safety.

**Key Files:**
- `src/core/config.py` - Main configuration manager
- `src/core/models.py` - Pydantic models
- `src/core/constants.py` - Framework constants

**Adding New Configuration:**
1. Add field to `FrameworkSettings` in `models.py`
2. Add validation if needed
3. Update `Config` class in `config.py`
4. Add to environment template

### Logging System

Structured logging with multiple levels and components.

**Key Files:**
- `src/core/logging.py` - Logging implementation
- `src/core/validators.py` - Logging decorators

**Adding New Logging:**
```python
from core.logging import log_with_timestamp, PerformanceLogger

# Simple logging
log_with_timestamp("Message", "Component", "level")

# Performance logging
with PerformanceLogger("Operation", "Component"):
    # Your code here
    pass
```

### Pipeline System

Extensible pipeline architecture with auto-discovery.

**Key Files:**
- `src/pipelines/pipeline_factory.py` - Pipeline creation
- `src/pipelines/pipeline_registry.py` - Pipeline management
- `src/pipelines/tools/` - Pipeline components

## 🔧 Adding New Features

### 1. New Pipeline Components

#### Adding an Extractor

Create `src/pipelines/tools/extractors/my_extractor.py`:

```python
import pandas as pd
from typing import Dict, Any, Optional
from core.logging import log_with_timestamp

def create_my_extractor(
    source: str,
    options: Optional[Dict[str, Any]] = None,
    name: str = "My Extractor"
) -> callable:
    """Create a custom extractor."""
    
    async def extractor_func(*args, **kwargs) -> pd.DataFrame:
        log_with_timestamp(f"Extracting from {source}", name)
        
        try:
            # Your extraction logic here
            data = extract_from_source(source, options)
            df = pd.DataFrame(data)
            
            log_with_timestamp(f"Extracted {len(df)} records", name)
            return df
            
        except Exception as e:
            log_with_timestamp(f"Extraction failed: {e}", name, "error")
            return pd.DataFrame()
    
    return extractor_func
```

#### Available Extractors

The framework includes several built-in extractors:

##### HTTP Extractor
Extract data from HTTP APIs:

```python
from pipelines.tools.extractors import create_http_extractor

extractor = create_http_extractor(
    url="https://api.example.com/data",
    headers={"Authorization": "Bearer token"},
    timeout=30
)
```

##### ClickHouse Extractor
Extract data from ClickHouse databases:

```python
from pipelines.tools.extractors import create_clickhouse_extractor

extractor = create_clickhouse_extractor(
    query="SELECT * FROM my_table WHERE created_at > '2023-01-01'",
    database="my_database"
)
```

##### Metabase Extractor
Extract data from any table in any database added to Metabase:

```python
from pipelines.tools.extractors import create_metabase_extractor

# Extract from a specific table
extractor = create_metabase_extractor(
    base_url="https://metabase.devinvex.com",
    api_key="CHANGE_ME",
    database_id=1,
    table_id=2,
    limit=1000
)

# Execute custom SQL query
query_extractor = create_metabase_extractor(
    base_url="https://metabase.devinvex.com",
    api_key="CHANGE_ME",
    database_id=1,
    native_query="SELECT COUNT(*) as total FROM users WHERE active = true"
)

# Using configuration (set METABASE_BASE_URL and METABASE_API_KEY in .env)
config_extractor = create_metabase_extractor(
    database_id=1,
    table_id=2
)
```

**Metabase Configuration:**
Add to your `.env` file:
```bash
METABASE_BASE_URL=https://metabase.devinvex.com
METABASE_API_KEY=CHANGE_ME
METABASE_TIMEOUT=30
```

**Metabase Features:**
- Extract data from any table in any Metabase database
- Execute custom SQL queries
- Database and table discovery
- Comprehensive error handling
- Pagination support with limit/offset

#### Adding a Transformer

Create `src/pipelines/tools/transformers/my_transformer.py`:

```python
import pandas as pd
from typing import Callable, Optional
from core.logging import log_with_timestamp

def create_my_transformer(
    transform_func: Callable[[pd.DataFrame], pd.DataFrame],
    name: str = "My Transformer"
) -> callable:
    """Create a custom transformer."""
    
    def transformer_func(data: pd.DataFrame) -> pd.DataFrame:
        log_with_timestamp(f"Transforming {len(data)} records", name)
        
        try:
            result = transform_func(data)
            log_with_timestamp(f"Transformed {len(result)} records", name)
            return result
            
        except Exception as e:
            log_with_timestamp(f"Transformation failed: {e}", name, "error")
            return data
    
    return transformer_func
```

#### Adding a Loader

Create `src/pipelines/tools/loaders/my_loader.py`:

```python
import pandas as pd
from typing import Dict, Any, Optional
from core.logging import log_with_timestamp

def create_my_loader(
    destination: str,
    options: Optional[Dict[str, Any]] = None,
    name: str = "My Loader"
) -> callable:
    """Create a custom loader."""
    
    def loader_func(data: pd.DataFrame) -> bool:
        log_with_timestamp(f"Loading {len(data)} records to {destination}", name)
        
        try:
            # Your loading logic here
            success = load_to_destination(data, destination, options)
            
            if success:
                log_with_timestamp(f"Successfully loaded {len(data)} records", name)
            else:
                log_with_timestamp("Loading failed", name, "error")
            
            return success
            
        except Exception as e:
            log_with_timestamp(f"Loading failed: {e}", name, "error")
            return False
    
    return loader_func
```

### 2. New Pipeline Types

Add new pipeline types to `pipeline_factory.py`:

```python
def create_my_pipeline_type(
    components: List[Any],
    name: str = "My Pipeline Type"
) -> Callable[..., bool]:
    """Create a custom pipeline type."""
    
    async def pipeline_func(*args, **kwargs) -> bool:
        log_with_timestamp(f"Starting {name}", name)
        
        try:
            # Your pipeline logic here
            for component in components:
                result = await component(*args, **kwargs)
                if not result:
                    return False
            
            log_with_timestamp(f"Completed {name}", name)
            return True
            
        except Exception as e:
            log_with_timestamp(f"{name} failed: {e}", name, "error")
            return False
    
    return pipeline_func
```

### 3. New Configuration Options

Add new configuration to `models.py`:

```python
class FrameworkSettings(BaseSettings):
    # ... existing fields ...
    
    # New configuration
    my_new_option: str = Field(
        default='default_value',
        description="Description of new option"
    )
    
    @field_validator('my_new_option')
    @classmethod
    def validate_my_new_option(cls, v):
        """Validate new option."""
        if not v:
            raise ValueError("New option cannot be empty")
        return v
```

## 🔄 Migration System

### Enhanced Migration Capabilities

The migration system now includes comprehensive status tracking and rollback capabilities.

**Key Features:**
- **Status Tracking**: Get detailed migration status as dictionary
- **Rollback Support**: Rollback the last N migrations safely
- **Comprehensive Logging**: Detailed logging for all migration operations
- **Error Handling**: Robust error handling with detailed error messages

**Usage Examples:**

```python
from migrations.migration_manager import ClickHouseMigrationManager

manager = ClickHouseMigrationManager()

# Get detailed status
status = manager.get_migration_status()
print(f"Executed: {status['executed_count']}")
print(f"Pending: {status['pending_count']}")

# Rollback migrations
success = manager.rollback_migrations(count=2)
if success:
    print("Successfully rolled back 2 migrations")
```

**Migration Commands:**
```bash
# Run migrations
./run.sh migrate

# Check status
./run.sh migrate_status

# Rollback (if supported by run.sh)
./run.sh migrate_rollback 1
```

## 🧪 Testing

### Test Structure

```
tests/
├── unit/                   # Unit tests
├── integration/            # Integration tests
├── performance/            # Performance tests
├── framework/              # Test framework
└── conftest.py            # Test configuration
```

### Writing Tests

#### Unit Tests

Create `tests/unit/test_my_component.py`:

```python
import pytest
import pandas as pd
from src.pipelines.tools.extractors.my_extractor import create_my_extractor

class TestMyExtractor:
    def test_create_extractor(self):
        """Test extractor creation."""
        extractor = create_my_extractor("test_source")
        assert callable(extractor)
    
    @pytest.mark.asyncio
    async def test_extractor_execution(self):
        """Test extractor execution."""
        extractor = create_my_extractor("test_source")
        result = await extractor()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 0
```

#### Integration Tests

Create `tests/integration/test_my_pipeline.py`:

```python
import pytest
from src.pipelines.pipeline_factory import create_etl_pipeline
from src.pipelines.tools.extractors.my_extractor import create_my_extractor
from src.pipelines.tools.loaders.my_loader import create_my_loader

class TestMyPipeline:
    @pytest.mark.asyncio
    async def test_pipeline_execution(self):
        """Test complete pipeline execution."""
        extractor = create_my_extractor("test_source")
        loader = create_my_loader("test_destination")
        
        pipeline = create_etl_pipeline(
            extractor=extractor,
            transformer=lambda x: x,  # Pass-through transformer
            loader=loader,
            name="Test Pipeline"
        )
        
        result = await pipeline()
        assert result is True
```

### Running Tests

```bash
# Run all tests
./run.sh test

# Run specific category
./run.sh test unit

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/unit/test_my_component.py

# Run tests with JSON report
pytest --json-report --json-report-file=tests/results/test_summary.json
```

**Test Dependencies:**
The framework includes comprehensive test dependencies:
- `pytest>=7.0.0` - Testing framework
- `pytest-cov>=4.0.0` - Coverage reporting
- `pytest-json-report>=1.5.0` - JSON test reporting
- `psutil>=5.9.0` - System monitoring for performance tests

**Production Testing:**
All tests are designed to run in production environments with proper dependency management and virtual environment support.

## 📝 Code Style

### Python Style

Follow PEP 8 with these additions:

- Line length: 100 characters
- Use type hints for all functions
- Use docstrings for all public functions
- Use async/await for I/O operations

### Code Formatting

```bash
# Format code
black src/ tests/

# Check style
flake8 src/ tests/

# Type checking
mypy src/
```

### Documentation

- Use Google-style docstrings
- Include examples in docstrings
- Update README.md for new features
- Add to API reference for new APIs

## 🔄 Git Workflow

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring

### Commit Messages

Use conventional commits:

```
feat: add new extractor for CSV files
fix: resolve memory leak in pipeline execution
docs: update API reference for new features
refactor: simplify configuration management
```

### Pull Request Process

1. Create feature branch
2. Make changes with tests
3. Run tests and linting
4. Update documentation
5. Create pull request
6. Address review feedback
7. Merge after approval

## 🚀 Deployment

### Local Testing

```bash
# Test locally
./run.sh test
./run.sh check

# Test specific pipeline
./run.sh cron_run my_pipeline
```

### Production Deployment

```bash
# Deploy to staging
./deploy.sh user staging-server

# Deploy to production
./deploy.sh user production-server
```

The deployment process includes:
- **8-Step Deployment**: Clean, sync, provision, migrate, test, cron, integrity, verify
- **Production Integrity Checks**: ClickHouse connectivity, Metabase integration, database schema validation
- **Dependency Management**: Automatic installation with fallback mechanisms
- **Health Verification**: Comprehensive post-deployment validation

## 🐛 Debugging

### Log Analysis

```bash
# View system logs
tail -f logs/system/application.log

# View job logs
tail -f logs/jobs/my_job.log

# Search logs
grep "ERROR" logs/**/*.log
```

### Common Issues

1. **Import Errors**: Check Python path and module structure
2. **Configuration Errors**: Verify environment variables
3. **Database Errors**: Check ClickHouse connection and permissions
4. **Pipeline Errors**: Check logs for specific error messages
5. **Test Failures**: Ensure test dependencies are installed

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
./run.sh cron_run my_pipeline
```

## 📚 Resources

### Documentation

- [Architecture Overview](./architecture/README.md)
- [API Reference](./api-reference.md)
- [Quick Start Guide](./quick-start.md)

### External Resources

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs/)
- [Pydantic Documentation](https://pydantic-docs.helpmanual.io/)
- [Python Async/Await](https://docs.python.org/3/library/asyncio.html)

## 🤝 Contributing

### Before Contributing

1. Read this guide thoroughly
2. Check existing issues and pull requests
3. Discuss major changes in issues first
4. Ensure your changes align with project goals

### Contribution Checklist

- [ ] Code follows style guidelines
- [ ] Tests pass locally
- [ ] New features have tests
- [ ] Documentation is updated
- [ ] Commit messages are clear
- [ ] Pull request description is complete

### Getting Help

- Create an issue for questions
- Use discussions for general questions
- Check existing documentation first
- Be specific about your problem

## 📄 License

This project is licensed under the MIT License. By contributing, you agree that your contributions will be licensed under the same license.