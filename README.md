# Data Processing Framework

A flexible, functional programming-based data processing framework inspired by Spring Boot patterns, designed for building robust ETL/ELT pipelines.

## 🏗️ Architecture

The framework follows a modular, Spring Boot-inspired structure:

```
data-processor/
├── src/                          # Main source code
│   ├── core/                     # Core utilities
│   │   ├── config.py            # Configuration management
│   │   └── logging.py           # Logging utilities
│   ├── extractors/              # Data extraction modules
│   │   ├── http_extractor.py    # HTTP/API extractors
│   │   └── clickhouse_extractor.py # ClickHouse extractors
│   ├── loaders/                 # Data loading modules
│   │   ├── clickhouse_loader.py # ClickHouse loaders
│   │   └── console_loader.py    # Console/debug loaders
│   ├── transformers/            # Data transformation modules
│   │   └── lambda_transformer.py # Lambda-based transformers
│   ├── pipelines/               # Pipeline orchestration
│   │   ├── pipeline_factory.py  # Pipeline creation utilities
│   │   └── cmc_pipeline.py      # CoinMarketCap pipeline implementation
│   ├── utils/                   # Utility functions
│   │   └── cron_helper.py       # Cron job management
│   └── main.py                  # Main application entry point
├── samples/                     # Sample implementations
│   └── examples/                # Other examples
│       └── weather_pipeline.py  # Weather data example
├── scripts/                     # Execution scripts
│   └── run.py                   # Pipeline runner
├── tests/                       # Test suites
│   ├── unit/                    # Unit tests
│   └── integration/             # Integration tests
└── run.sh                       # Main execution script
```

## 🚀 Features

- **Functional Programming**: Lambda-based transformations and function passing
- **Generic Components**: Reusable extractors, loaders, and transformers
- **Pipeline Types**: Support for EL, ETL, and ELT patterns
- **Multiple Data Sources**: HTTP APIs, ClickHouse, with placeholders for Kafka, Metabase
- **Cron Integration**: Easy conversion of pipelines to scheduled jobs
- **Spring Boot Patterns**: Clean separation of concerns and modular architecture
- **Comprehensive Testing**: Unit and integration test suites

## 📦 Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd data-processor
   ```

2. **Set up environment**:
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

3. **Install dependencies**:
   ```bash
   ./run.sh check
   ```

## 🔧 Configuration

The framework uses environment variables for configuration. Copy `env.example` to `.env` and configure:

```bash
# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/application.log

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DATABASE=invex_data

# APIs
CMC_API_KEY=your_coinmarketcap_api_key
WEATHER_API_KEY=your_weather_api_key
```

## 🎯 Usage

### Running Pipelines

```bash
# List available pipelines
./run.sh list

# Run a specific pipeline
./run.sh cron_run cmc_latest_quotes

# Run weather data pipeline
./run.sh cron_run weather_data
```

### Available Pipelines

- **cmc_latest_quotes**: CoinMarketCap latest cryptocurrency quotes
- **cmc_historical_data**: CoinMarketCap historical data
- **weather_data**: Weather data for Tehran

### Database Setup

```bash
# Setup ClickHouse database
./run.sh setup_db

# Drop database (clean slate)
./run.sh drop_db
```

## 🧪 Testing

```bash
# Run all tests
./run.sh test

# Run specific test
python -m pytest tests/unit/test_pipeline_factory.py
```

## 🔨 Creating Custom Pipelines

### 1. Create an Extractor

```python
from extractors.http_extractor import create_http_extractor

# Create HTTP extractor
extractor = create_http_extractor(
    url="https://api.example.com/data",
    headers={"Authorization": "Bearer token"},
    name="My API Extractor"
)
```

### 2. Create a Transformer

```python
from transformers.lambda_transformer import create_lambda_transformer

# Create lambda transformer
transformer = create_lambda_transformer(
    lambda record: {
        "id": record["id"],
        "processed_at": datetime.now().isoformat(),
        "value": record["amount"] * 2
    },
    name="My Transformer"
)
```

### 3. Create a Loader

```python
from loaders.clickhouse_loader import create_clickhouse_loader

# Create ClickHouse loader
loader = create_clickhouse_loader(
    table_name="my_table",
    name="My Loader"
)
```

### 4. Create a Pipeline

```python
from pipelines.pipeline_factory import create_etl_pipeline

# Create ETL pipeline
pipeline = create_etl_pipeline(
    extractor=extractor,
    transformer=transformer,
    loader=loader,
    name="My ETL Pipeline"
)
```

### 5. Register as Cron Job

```python
from utils.cron_helper import register_cron_job

# Register pipeline as cron job
register_cron_job(
    job_name="my_pipeline",
    pipeline=pipeline,
    schedule="0 */2 * * *",  # Every 2 hours
    description="My custom pipeline"
)
```

## 🏛️ Architecture Patterns

### Spring Boot Inspired Structure

- **Core**: Configuration and logging utilities
- **Extractors**: Data source abstraction layer
- **Loaders**: Data destination abstraction layer
- **Transformers**: Data processing layer
- **Pipelines**: Orchestration layer
- **Utils**: Cross-cutting concerns

### Functional Programming

- Lambda-based transformations
- Function passing for flexibility
- Immutable data processing
- Pure functions where possible

### Pipeline Patterns

- **EL**: Extract → Load (raw data)
- **ETL**: Extract → Transform → Load (processed data)
- **ELT**: Extract → Load → Transform (data warehouse pattern)

## 🔄 Cron Job Management

The framework provides easy cron job management:

```python
from utils.cron_helper import (
    register_cron_job,
    run_cron_job,
    list_cron_jobs,
    unregister_cron_job
)

# Register a job
register_cron_job("my_job", pipeline, "0 */6 * * *")

# Run a job
success = run_cron_job("my_job")

# List all jobs
jobs = list_cron_jobs()
```

## 🧪 Testing Strategy

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **Pipeline Tests**: Test complete data flows
- **Mocking**: External dependencies are mocked in tests

## 📈 Extensibility

The framework is designed for easy extension:

1. **New Extractors**: Add to `src/extractors/`
2. **New Loaders**: Add to `src/loaders/`
3. **New Transformers**: Add to `src/transformers/`
4. **New Pipelines**: Add to `samples/` or create new sample directories
5. **New Utilities**: Add to `src/utils/`

## 🚀 Future Enhancements

- Kafka integration for streaming data
- Metabase API integration
- More data source connectors
- Advanced scheduling options
- Monitoring and alerting
- Data quality validation

## 📝 License

[Add your license information here]

## 🤝 Contributing

[Add contribution guidelines here]