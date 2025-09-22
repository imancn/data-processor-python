# AI Prompts Directory

This directory contains AI prompts to help create, debug, and optimize data processing pipelines using the Data Processing Framework.

## Available Prompts

### 1. [Create a Production-Ready Pipeline](prompts.md#prompt-1-create-a-production-ready-pipeline)
**Use Case**: Creating new pipelines from scratch
**When to Use**: When you need to build a complete ETL/ELT pipeline
**Key Features**: 
- Class-based architecture
- Multiple data sources
- Time-based processing
- Error handling and logging

### 2. [Debug Pipeline Issues](prompts.md#prompt-2-debug-pipeline-issues)
**Use Case**: Troubleshooting existing pipelines
**When to Use**: When pipelines are failing or performing poorly
**Key Features**:
- Root cause analysis
- Step-by-step debugging
- Performance monitoring
- Error resolution

### 3. [Optimize Pipeline Performance](prompts.md#prompt-3-optimize-pipeline-performance)
**Use Case**: Improving existing pipeline performance
**When to Use**: When pipelines are slow or resource-intensive
**Key Features**:
- Performance analysis
- Memory optimization
- Database tuning
- Resource utilization

### 4. [Add New Data Source](prompts.md#prompt-4-add-new-data-source)
**Use Case**: Extending pipelines with new data sources
**When to Use**: When integrating new data sources
**Key Features**:
- New extractor implementation
- Data transformation
- Error handling
- Testing strategies

## How to Use

1. **Choose the appropriate prompt** for your use case
2. **Copy the prompt** and customize it with your specific requirements
3. **Provide context** about your data sources, transformations, and business logic
4. **Use the generated code** as a starting point
5. **Test and customize** according to your needs

## Framework Reference

These prompts are designed to work with:
- **Base Classes**: `BasePipeline`, `MetabasePipeline`, `ClickHousePipeline`
- **Generic Utilities**: Data processing, pagination, backfill management
- **Extractors**: Metabase, HTTP, File extractors
- **Loaders**: ClickHouse, Console, File loaders
- **Data Utils**: Timestamp conversion, deduplication, metadata

## Example Pipeline

For a complete example, see [`financial_trades_pipeline.py`](../../src/pipelines/financial_trades_pipeline.py) which demonstrates:
- Multiple data source integration
- Complex data merging
- Time-based processing
- Comprehensive error handling
- Production-ready patterns

## Best Practices

- **Test thoroughly** before deploying to production
- **Follow framework patterns** from existing pipelines
- **Include comprehensive logging** and error handling
- **Validate data** at each processing step
- **Monitor performance** and resource usage
- **Document your customizations** and business logic
