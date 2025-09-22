# Data Processing Framework

A **production-ready, domain-agnostic data processing platform** built with Python and ClickHouse. This framework provides a comprehensive foundation for building robust ETL/ELT pipelines with enterprise-grade features.

## 🌟 Key Features

- **🎯 Zero Business Logic**: Completely generic - works for any data processing use case
- **🔄 Auto-Discovery**: Just add your pipeline files - framework handles the rest
- **🚀 Production Ready**: Idempotent deployment, process management, health monitoring
- **🧪 Test-Driven**: Comprehensive test suite with coverage reporting
- **📊 Observable**: Structured logging, monitoring, and operational insights
- **🔧 Extensible**: Plugin architecture for extractors, transformers, and loaders
- **🏗️ Class-Based Design**: Professional architecture with inheritance patterns
- **📦 Generic Utilities**: Reusable data processing and pagination tools

## 🚀 Quick Start

### 1. Setup
```bash
cp env.example .env
# Edit .env with your settings
pip install -r requirements.txt
./run.sh setup_db
./run.sh migrate
```

### 2. Create Pipeline
Create `src/pipelines/my_pipeline.py`:

```python
import pandas as pd
from pipelines.base_pipeline import MetabasePipeline, ClickHousePipeline
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.clickhouse_replace_loader import create_clickhouse_replace_loader
from pipelines.pipeline_registry import pipeline_registry

class MyPipeline(MetabasePipeline, ClickHousePipeline):
    def __init__(self):
        super().__init__(
            name="my_pipeline",
            description="My data pipeline",
            schedule="0 * * * *"
        )
        
        self.extractor = create_http_extractor(
            url="https://api.example.com/data",
            headers={"Accept": "application/json"}
        )
        
        self.loader = create_clickhouse_replace_loader(
            table_name="my_data",
            key_columns=["id"],
            sort_column="updated_at"
        )
    
    async def extract(self) -> pd.DataFrame:
        return await self.extractor()
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['processed_at'] = pd.Timestamp.now()
        return self._add_merge_metadata(data, 'success')
    
    async def load(self, data: pd.DataFrame) -> bool:
        return await self.loader(data)

def register_pipelines():
    pipeline = MyPipeline()
    pipeline_registry.register_pipeline(pipeline)
```

### 3. Test & Deploy
```bash
./run.sh list                   # List pipelines
./run.sh cron_run my_pipeline   # Run pipeline
./deploy.sh user hostname       # Deploy to production
```

## 🔧 Available Commands

### Local Operations
```bash
./run.sh check                  # Check dependencies
./run.sh test                   # Run all tests
./run.sh cron_run NAME          # Run a pipeline
./run.sh list                   # List available pipelines
./run.sh migrate                # Run database migrations
./run.sh backfill 30            # Backfill 30 days
./run.sh setup_cron NAME        # Setup cron for pipeline
```

### Deployment
```bash
./deploy.sh user host           # Deploy to server
./deploy.sh user host --clean   # Deploy with cleanup
```

## 📁 Project Structure

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

## 🏗️ Architecture

The framework follows **clean architecture principles** with clear separation of concerns:

![Layered Architecture](docs/diagrams/layered-architecture.puml)

### Key Patterns

- **🔍 Auto-Discovery**: Zero-config pipeline registration
- **🏭 Factory Pattern**: Standardized component creation
- **📋 Registry Pattern**: Centralized pipeline management
- **🔌 Plugin Architecture**: Extensible extractors, transformers, loaders

## 📝 Configuration

Key environment variables in `.env`:

```bash
# ClickHouse Database
CLICKHOUSE_HOST=172.30.63.35
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=data-processor
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=data_warehouse

# Logging
LOG_LEVEL=INFO
TIMEOUT=30
BATCH_SIZE=1000
```

## 📊 Testing

```bash
./run.sh test                   # All tests
./run.sh test unit              # Unit tests only
./run.sh test integration       # Integration tests
./run.sh test --fast            # Fast tests only
```

Test results are saved in `tests/results/` with coverage reports and JUnit XML.

## 🔍 Monitoring

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

## 📚 Documentation

- **[🚀 Quick Start Guide](docs/quick-start.md)** - Get up and running quickly
- **[🏗️ Architecture Overview](docs/architecture/README.md)** - System design and components
- **[📊 System Diagrams](docs/diagrams/README.md)** - Visual architecture representations
- **[📚 API Reference](docs/api-reference.md)** - Complete API documentation
- **[🛠️ Developer Guide](docs/developer-guide.md)** - Contributing and extending
- **[🔄 Migration Guide](docs/migration-guide.md)** - Migrating to new architecture
- **[📋 Change Logs](docs/change_logs/)** - Release notes and change history

## 🆘 Troubleshooting

**"No pipelines found"**
- Add pipeline modules to `src/pipelines/` following the naming pattern `*_pipeline.py`

**"Database connection failed"**
- Check ClickHouse configuration in `.env`

**"Tests failing"**
- Install test dependencies: `pip install -r requirements-test.txt`

## 🤝 Contributing

1. Add your pipeline modules to `src/pipelines/`
2. Follow the class-based pipeline pattern
3. Add tests in `tests/unit/` and `tests/integration/`
4. Run the test suite: `./run.sh test`
5. Update documentation as needed

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## 🎉 Ready to Build Something Amazing?

This framework provides everything you need to build **production-grade data processing systems** without the infrastructure overhead. Focus on your data, your logic, your business value - we've got the rest covered.

**Start building your first pipeline in minutes:**

1. **Clone & Configure**: `cp env.example .env`
2. **Create Pipeline**: Add `src/pipelines/my_pipeline.py`
3. **Test Locally**: `./run.sh list && ./run.sh cron_run my_pipeline`
4. **Deploy**: `./deploy.sh user hostname`
5. **Monitor**: Check logs, data counts, and health metrics

**Welcome to the future of data processing! 🚀**