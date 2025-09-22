# Quick Start Guide

Get up and running with the Data Processing Framework in minutes.

## 🚀 Prerequisites

- Python 3.9+
- ClickHouse database access
- Basic ETL/ELT knowledge

## ⚡ Setup

### 1. Configure Environment
```bash
cp env.example .env
# Edit .env with your settings
```

### 2. Install & Setup
```bash
pip install -r requirements.txt
./run.sh setup_db
./run.sh migrate
```

### 3. Verify Installation
```bash
./run.sh check
./run.sh test
./run.sh list
```

## 🏗️ Create Your First Pipeline

Create `src/pipelines/my_first_pipeline.py`:

```python
import pandas as pd
from pipelines.base_pipeline import MetabasePipeline, ClickHousePipeline
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.clickhouse_replace_loader import create_clickhouse_replace_loader
from pipelines.pipeline_registry import pipeline_registry

class MyFirstPipeline(MetabasePipeline, ClickHousePipeline):
    def __init__(self):
        super().__init__(
            name="my_first_pipeline",
            description="Extract posts from JSONPlaceholder API",
            schedule="0 * * * *"  # Hourly
        )
        
        self.http_extractor = create_http_extractor(
            url="https://jsonplaceholder.typicode.com/posts",
            headers={"Accept": "application/json"},
            name="Posts Extractor"
        )
        
        self.loader = create_clickhouse_replace_loader(
            table_name="posts",
            key_columns=["id"],
            sort_column="updated_at",
            name="Posts Loader"
        )
    
    async def extract(self) -> pd.DataFrame:
        return await self.http_extractor()
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['processed_at'] = pd.Timestamp.now()
        data['word_count'] = data['body'].str.split().str.len()
        return self._add_merge_metadata(data, 'success')
    
    async def load(self, data: pd.DataFrame) -> bool:
        return await self.loader(data)

# Register pipeline
def register_pipelines():
    pipeline = MyFirstPipeline()
    pipeline_registry.register_pipeline(pipeline)
```

## 🧪 Test Your Pipeline

```bash
# List pipelines
./run.sh list

# Run pipeline
./run.sh cron_run my_first_pipeline

# Check logs
tail -f logs/jobs/my_first_pipeline.log
```

## 🚀 Deploy to Production

```bash
# Deploy to server
./deploy.sh username hostname

# Verify deployment
ssh username@hostname "cd data-processor && ./run.sh list"
```

## 🔧 Common Commands

### Pipeline Management
```bash
./run.sh list                   # List pipelines
./run.sh cron_run NAME          # Run pipeline
./run.sh setup_cron NAME        # Setup cron
```

### Database Operations
```bash
./run.sh migrate                # Run migrations
./run.sh migrate_status         # Check status
./run.sh backfill 30            # Backfill 30 days
```

### Testing
```bash
./run.sh test                   # Run all tests
./run.sh test unit              # Unit tests only
./run.sh test --fast            # Fast tests only
```

## 📊 Monitoring

### Logs
- **System**: `logs/system/application.log`
- **Jobs**: `logs/jobs/{job_name}.log`
- **Cron**: `logs/cron.log`

### Health Checks
```bash
./run.sh check                  # System health
./run.sh backfill_counts        # Data counts
./run.sh migrate_status         # Migration status
```

## 🆘 Troubleshooting

**"No pipelines found"**
- Ensure pipeline files are in `src/pipelines/` with `*_pipeline.py` naming

**"Database connection failed"**
- Check ClickHouse configuration in `.env`

**"Tests failing"**
- Install test dependencies: `pip install -r requirements-test.txt`

## 🎯 Next Steps

1. **Explore Architecture**: [Architecture Overview](./architecture/README.md)
2. **Build More Pipelines**: Add additional pipeline modules
3. **Production Deployment**: [Deployment Guide](./architecture/04-operations-deployment.md)
4. **Extend Framework**: [Extension Guide](./architecture/08-extension-customization.md)