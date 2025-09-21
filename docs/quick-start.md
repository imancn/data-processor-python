# Quick Start Guide

Get up and running with the Data Processing Framework in minutes.

## 🚀 Prerequisites

- Python 3.9 or higher
- ClickHouse database access
- Basic understanding of ETL/ELT concepts

## ⚡ Quick Setup

### 1. Environment Configuration

```bash
# Copy environment template
cp env.example .env

# Edit configuration
vim .env
```

Key configuration variables:
```bash
# ClickHouse Database
CLICKHOUSE_HOST=your_clickhouse_host
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=your_username
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=data_warehouse

# Logging
LOG_LEVEL=INFO
```

### 2. Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Or using poetry (if available)
poetry install
```

### 3. Setup Database

```bash
# Setup database and run migrations
./run.sh setup_db
./run.sh migrate
```

### 4. Verify Installation

```bash
# Check system health
./run.sh check

# Run tests
./run.sh test

# List available pipelines (none initially)
./run.sh list
```

## 🏗️ Create Your First Pipeline

### 1. Create Pipeline Module

Create `src/pipelines/my_first_pipeline.py`:

```python
import pandas as pd
from pipelines.pipeline_factory import create_etl_pipeline
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.loaders.console_loader import create_console_loader
from pipelines.tools.transformers.transformers import create_lambda_transformer

# Pipeline registry
PIPELINE_REGISTRY = {}

def create_my_pipeline():
    """Create your first data pipeline."""
    
    # Create extractor
    extractor = create_http_extractor(
        url="https://jsonplaceholder.typicode.com/posts",
        headers={"Accept": "application/json"},
        name="Posts Extractor"
    )
    
    # Create transformer
    def transform_data(df):
        df['processed_at'] = pd.Timestamp.now()
        df['word_count'] = df['body'].str.split().str.len()
        return df
    
    transformer = create_lambda_transformer(transform_data, "Posts Transformer")
    
    # Create loader
    loader = create_console_loader("Posts Console Loader")
    
    # Create pipeline
    pipeline = create_etl_pipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        name="My First Pipeline"
    )
    
    return {
        'pipeline': pipeline,
        'description': 'Extract posts from JSONPlaceholder API',
        'schedule': '0 * * * *',  # Hourly
        'table_name': 'posts'
    }

def register_pipelines():
    """Register all pipelines in this module."""
    PIPELINE_REGISTRY['my_first_pipeline'] = create_my_pipeline()

def get_pipeline_registry():
    """Get the pipeline registry."""
    return PIPELINE_REGISTRY

# Auto-register when module is imported
register_pipelines()
```

### 2. Test Your Pipeline

```bash
# List pipelines (should now show your pipeline)
./run.sh list

# Run your pipeline
./run.sh cron_run my_first_pipeline

# Check logs
tail -f logs/jobs/my_first_pipeline.log
```

### 3. Deploy to Production

```bash
# Deploy to remote server
./deploy.sh username hostname

# Verify deployment
ssh username@hostname "cd data-processor && ./run.sh list"
```

## 🔧 Common Operations

### Pipeline Management
```bash
# List all pipelines
./run.sh list

# Run specific pipeline
./run.sh cron_run pipeline_name

# Setup cron for pipeline
./run.sh setup_cron pipeline_name
```

### Database Operations
```bash
# Run migrations
./run.sh migrate

# Check migration status
./run.sh migrate_status

# Drop and recreate database
./run.sh drop_db
```

### Testing
```bash
# Run all tests
./run.sh test

# Run specific test category
./run.sh test unit

# Run fast tests only
./run.sh test --fast
```

### Data Operations
```bash
# Backfill historical data
./run.sh backfill 30  # Last 30 days

# Check data counts
./run.sh backfill_counts

# List available jobs
./run.sh backfill_list
```

## 📊 Monitoring

### Logs
- **System logs**: `logs/system/application.log`
- **Job logs**: `logs/jobs/{job_name}.log`
- **Cron logs**: `logs/cron.log`

### Health Checks
```bash
# Check system health
./run.sh check

# Verify data counts
./run.sh backfill_counts

# Check migration status
./run.sh migrate_status
```

## 🆘 Troubleshooting

### Common Issues

**"No pipelines found"**
- Ensure pipeline files are in `src/pipelines/` with `*_pipeline.py` naming
- Check that `register_pipelines()` is called

**"Database connection failed"**
- Verify ClickHouse configuration in `.env`
- Check network connectivity and credentials

**"Tests failing"**
- Install test dependencies: `pip install pytest pytest-asyncio`
- Check test results in `tests/results/`

### Getting Help

1. Check logs in `logs/` directory
2. Run tests: `./run.sh test`
3. Check configuration: `./run.sh check`
4. Review test results in `tests/results/`

## 🎯 Next Steps

1. **Explore Architecture**: Read the [Architecture Overview](../architecture/README.md)
2. **Build More Pipelines**: Add additional pipeline modules
3. **Customize Components**: Create custom extractors, transformers, or loaders
4. **Production Deployment**: Follow the [Deployment Guide](../architecture/04-operations-deployment.md)
5. **Extend Framework**: See [Extension Guide](../architecture/08-extension-customization.md)

## 📚 Additional Resources

- [Complete Architecture Documentation](../architecture/README.md)
- [System Diagrams](../diagrams/README.md)
- [API Reference](../api-reference.md)
- [Developer Guide](../developer-guide.md)
