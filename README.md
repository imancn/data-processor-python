# Data Processing Framework

A **production-ready, domain-agnostic data processing platform** built with Python and ClickHouse. This framework provides a comprehensive foundation for building robust ETL/ELT pipelines with enterprise-grade features including automatic pipeline discovery, idempotent deployment, comprehensive testing, and operational monitoring.

## 🌟 Why This Framework?

- **🎯 Zero Business Logic**: Completely generic - works for any data processing use case
- **🔄 Auto-Discovery**: Just add your pipeline files - framework handles the rest
- **🚀 Production Ready**: Idempotent deployment, process management, health monitoring
- **🧪 Test-Driven**: Comprehensive test suite with coverage reporting
- **📊 Observable**: Structured logging, monitoring, and operational insights
- **⚙️ Operational**: Complete toolkit for development, deployment, and maintenance
- **🔧 Extensible**: Plugin architecture for extractors, transformers, and loaders
- **📈 Scalable**: Designed for enterprise data processing workloads

## 🏆 Enterprise Features

### **Deployment & Operations**
- ✅ **Idempotent Deployment**: Safe, repeatable deployments with automatic cleanup
- ✅ **Process Management**: Automatic process lifecycle and cron job management  
- ✅ **Health Monitoring**: Post-deployment verification and continuous health checks
- ✅ **Log Management**: Centralized, structured logging with job-specific logs
- ✅ **Migration System**: Version-controlled database schema management
- ✅ **Backfill System**: Historical data processing and monitoring

### **Development & Testing**
- ✅ **Auto-Discovery**: Zero-config pipeline registration and scheduling
- ✅ **Test Framework**: Unit + Integration + E2E tests with coverage reporting
- ✅ **Local Development**: Complete local development environment
- ✅ **CI/CD Ready**: JUnit XML, coverage reports, and automation-friendly
- ✅ **Documentation**: Comprehensive architecture and usage documentation

### **Architecture & Reliability**
- ✅ **Plugin Architecture**: Extensible extractors, transformers, and loaders
- ✅ **Error Handling**: Comprehensive error handling with retry logic
- ✅ **Configuration Management**: Environment-based configuration with validation
- ✅ **Security**: No hardcoded secrets, secure connection management
- ✅ **Performance**: Optimized for high-throughput data processing

## 📁 Project Structure

```
data-processor/                   # 🏠 Production-Ready Data Processing Platform
├── 📋 docs/                     # 📖 Complete Documentation
│   ├── architecture/            # 🏗️ System Architecture
│   ├── diagrams/                # 📊 Visual Diagrams
│   ├── quick-start.md           # 🚀 Getting Started Guide
│   ├── api-reference.md         # 📚 Complete API Reference
│   └── developer-guide.md       # 🛠️ Development Guidelines
├── 🧠 src/                      # 💻 Core Framework Source Code
│   ├── core/                   # ⚙️ Framework Foundation
│   │   ├── config.py          # 🔧 Configuration management & validation
│   │   ├── logging.py         # 📝 Multi-level structured logging system
│   │   ├── models.py          # 📊 Pydantic data models
│   │   ├── exceptions.py      # ⚠️ Custom exception hierarchy
│   │   └── validators.py      # ✅ Data validation utilities
│   ├── pipelines/             # 🔄 Pipeline System (ADD YOUR PIPELINES HERE)
│   │   ├── tools/             # 🛠️ Reusable Pipeline Components
│   │   │   ├── extractors/    # 📥 Data Sources (HTTP, DB, Files)
│   │   │   ├── transformers/  # 🔄 Data Processing
│   │   │   └── loaders/       # 📤 Data Destinations
│   │   ├── pipeline_factory.py # 🏭 ETL/ELT pipeline creation
│   │   └── pipeline_registry.py # 📋 Pipeline registration system
│   └── main.py                # 🚀 Main application & auto-discovery
├── 🗄️ migrations/              # 📊 Database Schema Management
│   ├── sql/                   # 📄 Versioned SQL migration files
│   └── migration_manager.py   # ⚙️ Migration orchestration & tracking
├── 📜 scripts/                 # 🔧 Utility Scripts
│   ├── run.py                 # ▶️ Pipeline runner with job logging
│   ├── backfill.py            # ⏮️ Historical data processing
│   └── run_tests.py           # 🧪 Comprehensive test runner
├── 🧪 tests/                   # 🔬 Comprehensive Test Suite
│   ├── unit/                  # 🧩 Component unit tests
│   ├── integration/           # 🔗 End-to-end system tests
│   ├── performance/           # 📈 Performance and load tests
│   └── results/               # 📊 Auto-generated test results
├── 🚀 deploy.sh               # 🎯 Idempotent deployment automation
├── ⚙️ run.sh                  # 🛠️ Local operations toolkit (14 commands)
├── 📋 env.example             # 🔧 Environment configuration template
└── 📄 requirements.txt        # 📦 Python dependencies
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Copy environment template
cp env.example .env

# Edit configuration
vim .env
```

### 2. Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Setup database
./run.sh setup_db
./run.sh migrate
```

### 3. Create Your First Pipeline

Create `src/pipelines/my_pipeline.py`:

```python
import pandas as pd
from pipelines.pipeline_factory import create_etl_pipeline
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.loaders.console_loader import create_console_loader
from pipelines.tools.transformers.transformers import create_lambda_transformer

# Pipeline registry
PIPELINE_REGISTRY = {}

def create_my_pipeline():
    """Create your custom pipeline."""
    
    # Create extractor
    extractor = create_http_extractor(
        url="https://api.example.com/data",
        headers={"Accept": "application/json"},
        name="My Data Extractor"
    )
    
    # Create transformer
    def transform_data(df):
        df['processed_at'] = pd.Timestamp.now()
        return df
    
    transformer = create_lambda_transformer(transform_data, "My Transformer")
    
    # Create loader
    loader = create_console_loader("My Console Loader")
    
    # Create pipeline
    pipeline = create_etl_pipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        name="My Pipeline"
    )
    
    return {
        'pipeline': pipeline,
        'description': 'My custom data pipeline',
        'schedule': '0 * * * *',  # Hourly
        'table_name': 'my_data'
    }

def register_pipelines():
    """Register all pipelines in this module."""
    PIPELINE_REGISTRY['my_pipeline'] = create_my_pipeline()

def get_pipeline_registry():
    """Get the pipeline registry."""
    return PIPELINE_REGISTRY

# Auto-register when module is imported
register_pipelines()
```

### 4. Test Your Pipeline

```bash
# List pipelines (should now show your pipeline)
./run.sh list

# Run your pipeline
./run.sh cron_run my_pipeline

# Check logs
tail -f logs/jobs/my_pipeline.log
```

### 5. Deploy to Production

```bash
# Deploy to remote server
./deploy.sh username hostname

# Verify deployment
ssh username@hostname "cd data-processor && ./run.sh list"
```

## 🔧 Available Commands

### Local Operations (`run.sh`)

```bash
./run.sh check                   # Check dependencies
./run.sh test                    # Run all tests
./run.sh cron_run NAME          # Run a pipeline
./run.sh list                   # List available pipelines
./run.sh setup_db               # Setup database
./run.sh drop_db                # Drop and recreate database
./run.sh migrate                # Run database migrations
./run.sh migrate_status         # Show migration status
./run.sh backfill [DAYS] [JOBS] # Backfill historical data
./run.sh backfill_list          # List available jobs
./run.sh backfill_counts        # Show data counts
./run.sh setup_cron NAME        # Setup cron for pipeline
./run.sh kill                   # Kill all running processes
./run.sh clean                  # Clean and restart
./run.sh help                   # Show help
```

### Deployment (`deploy.sh`)

```bash
./deploy.sh user host           # Deploy to server
./deploy.sh user host --clean   # Deploy with cleanup
```

## 📊 Testing

The framework includes comprehensive testing with detailed reporting:

```bash
# Run all tests
./run.sh test

# Run only unit tests
./run.sh test unit

# Run only integration tests
./run.sh test integration

# Run fast tests (exclude slow tests)
./run.sh test --fast
```

Test results are saved in `tests/results/` with:
- Test execution reports (JSON)
- Code coverage reports (HTML + JSON)
- JUnit XML for CI/CD integration

## 🏗️ Architecture

### System Design Philosophy
This framework follows **clean architecture principles** with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    🎯 Your Pipeline Logic                   │  ← Domain Layer
├─────────────────────────────────────────────────────────────┤
│  🔄 Pipeline Framework (Discovery, Factory, Registry)      │  ← Application Layer
├─────────────────────────────────────────────────────────────┤
│  🛠️ Tools (Extractors, Transformers, Loaders)             │  ← Service Layer
├─────────────────────────────────────────────────────────────┤
│  ⚙️ Core (Config, Logging, Migration, Cron)               │  ← Infrastructure Layer
└─────────────────────────────────────────────────────────────┘
```

### Key Architectural Patterns

#### 🔍 **Auto-Discovery Pattern**
- **Zero Configuration**: Just add `*_pipeline.py` files
- **Dynamic Loading**: Framework discovers and registers automatically
- **Convention Over Configuration**: Follow naming patterns, get functionality

#### 🏭 **Factory Pattern**
- **Pipeline Creation**: `create_etl_pipeline()`, `create_el_pipeline()`
- **Component Factory**: Standardized creation of extractors, transformers, loaders
- **Dependency Injection**: Components injected at runtime

#### 📋 **Registry Pattern**
- **Central Registration**: All pipelines registered in central registry
- **Runtime Discovery**: Pipelines discovered and registered at startup
- **Scheduling Integration**: Registry automatically creates cron jobs

#### 🔌 **Plugin Architecture**
- **Extractors**: HTTP, Database, File extractors
- **Transformers**: Lambda, Type, Column transformers  
- **Loaders**: ClickHouse, Console, File loaders
- **Extensible**: Easy to add new components

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
LOG_DIR=logs

# Application
TIMEOUT=30
BATCH_SIZE=1000

# Add your pipeline-specific variables here
API_KEY=your_api_key_here
API_BASE_URL=https://api.example.com
```

## 🔍 Monitoring & Debugging

### Logs

Logs are organized in the `logs/` directory:
- `logs/system/application.log` - System-level logs
- `logs/jobs/{job_name}.log` - Per-job detailed logs
- `logs/cron.log` - Cron execution logs

### Data Monitoring

```bash
# Check data counts in all tables
./run.sh backfill_counts

# List available jobs
./run.sh backfill_list

# Backfill specific jobs
./run.sh backfill 7 job1 job2  # Last 7 days for job1 and job2
```

### Migration Status

```bash
# Check migration status
./run.sh migrate_status

# Run pending migrations
./run.sh migrate
```

## 🚀 Production Deployment

1. **Prepare Server**: Ensure Python 3.9+, ClickHouse access, and network connectivity
2. **Configure Environment**: Set up `.env` with production values
3. **Deploy**: `./deploy.sh user hostname`
4. **Verify**: Check logs, data counts, and cron jobs
5. **Monitor**: Set up monitoring for logs and data freshness

### Deployment Checklist

- [ ] Server access and permissions configured
- [ ] ClickHouse database accessible
- [ ] Environment variables configured
- [ ] API keys and external service access verified
- [ ] Network connectivity tested
- [ ] Monitoring and alerting configured

## 📚 Documentation

- **[📖 Complete Documentation](docs/README.md)** - Comprehensive documentation index
- **[🚀 Quick Start Guide](docs/quick-start.md)** - Get up and running quickly
- **[🏗️ Architecture Overview](docs/architecture/README.md)** - System design and components
- **[📊 System Diagrams](docs/diagrams/README.md)** - Visual architecture representations
- **[📚 API Reference](docs/api-reference.md)** - Complete API documentation
- **[🛠️ Developer Guide](docs/developer-guide.md)** - Contributing and extending

## 🤝 Contributing

1. Add your pipeline modules to `src/pipelines/`
2. Follow the pipeline module pattern (see example above)
3. Add tests in `tests/unit/` and `tests/integration/`
4. Run the test suite: `./run.sh test`
5. Update documentation as needed

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Troubleshooting

### Common Issues

**"No pipelines found"**
- Add pipeline modules to `src/pipelines/` following the naming pattern `*_pipeline.py`

**"Database connection failed"**
- Check ClickHouse configuration in `.env`
- Verify network connectivity and credentials

**"Tests failing"**
- Install test dependencies: `pip install pytest pytest-asyncio`
- Check test results in `tests/results/latest_test_results.json`

**"Deployment fails"**
- Check server access and permissions
- Verify deployment directory is writable
- Check logs in deployment output

### Getting Help

1. Check the logs in `logs/` directory
2. Run tests to verify system health: `./run.sh test`
3. Check configuration: `./run.sh check`
4. Review test results in `tests/results/`

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