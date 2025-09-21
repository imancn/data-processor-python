# Data Processing Framework - Architecture Overview

## 🏗️ System Architecture

The Data Processing Framework is designed as a **domain-agnostic, extensible platform** for building robust ETL/ELT data pipelines. The architecture follows clean separation of concerns with pluggable components and automatic service discovery.

> 📊 **Visual Architecture**: See [System Diagrams](../diagrams/README.md) for complete visual representations of the framework architecture.

## 📊 High-Level Architecture

The framework follows a layered architecture pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                    🎯 Your Pipeline Logic                       │  ← Domain Layer
├─────────────────────────────────────────────────────────────────┤
│  🔄 Pipeline Framework (Discovery, Factory, Registry)          │  ← Application Layer
├─────────────────────────────────────────────────────────────────┤
│  🛠️ Tools (Extractors, Transformers, Loaders)                 │  ← Service Layer
├─────────────────────────────────────────────────────────────────┤
│  ⚙️ Core (Config, Logging, Migration, Cron)                   │  ← Infrastructure Layer
└─────────────────────────────────────────────────────────────────┘
```

## 🧩 Core Components

### **Core Framework** (`src/core/`)
- **Configuration System**: Environment-based config with Pydantic validation
- **Logging System**: Multi-level structured logging with job separation
- **Data Models**: Pydantic models for type safety and validation
- **Exception Handling**: Comprehensive error hierarchy and management

### **Pipeline System** (`src/pipelines/`)
- **Auto-Discovery**: Automatically finds and registers `*_pipeline.py` modules
- **Pipeline Factory**: Creates ETL, EL, parallel, sequential, and conditional pipelines
- **Pipeline Tools**: Extensible extractors, transformers, and loaders
- **Registry System**: Centralized pipeline management and scheduling

### **Main Application** (`src/main.py`)
- **Pipeline Registration**: Auto-discovers and registers all pipelines
- **Cron Job Management**: Automatic cron job creation and management
- **Command Interface**: CLI for pipeline operations and system management

### **Migration System** (`migrations/`)
- **Version Control**: SQL-based database versioning
- **Automatic Execution**: Runs pending migrations on startup
- **Status Tracking**: Migration history and status monitoring

### **Operations & Deployment**
- **Idempotent Deployment**: Safe, repeatable deployments with health checks
- **Process Management**: Automatic process lifecycle management
- **Testing Framework**: Comprehensive test suite with coverage reporting
- **Backfill System**: Historical data processing and monitoring

## 🔄 Data Flow

### Pipeline Execution
1. **Discovery**: Auto-find `*_pipeline.py` modules
2. **Registration**: Import and register pipelines
3. **Execution**: Extract → Transform → Load
4. **Monitoring**: Log results and track status

### Data Storage
- **ClickHouse**: Primary data warehouse
- **Schema Management**: Version-controlled migrations
- **Data Patterns**: ReplacingMergeTree for latest, MergeTree for historical

## 🔧 Extension Points

### Adding New Pipelines
1. Create `src/pipelines/my_pipeline.py`
2. Implement required interface functions
3. Framework auto-discovers and registers

### Adding New Tools
- **Extractors**: Data source connectors
- **Transformers**: Data processing logic
- **Loaders**: Data destination connectors

## 📊 Monitoring

### Logging
- **Structured Logs**: JSON-friendly format
- **Multi-level**: DEBUG → INFO → WARNING → ERROR
- **Job Separation**: Per-job log files

### Health Monitoring
- **Data Counts**: Automatic table monitoring
- **Pipeline Status**: Success/failure tracking
- **Deployment Verification**: Post-deployment checks

## 🛡️ Security & Reliability

### Security
- **Environment Variables**: No hardcoded secrets
- **Secure Connections**: Encrypted database connections
- **Access Control**: Proper authentication and authorization

### Reliability
- **Error Handling**: Graceful degradation and retry logic
- **Idempotent Operations**: Safe to retry
- **Health Checks**: Continuous monitoring
- **Data Integrity**: Schema validation and constraints

This architecture provides a **production-ready foundation** for any data processing use case while maintaining flexibility and extensibility.
