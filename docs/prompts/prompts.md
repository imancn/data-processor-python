# AI Prompts for Data Processing Framework

This directory contains AI prompts to help create and manage data processing pipelines using the Data Processing Framework.

## Prompt 1: Create a Production-Ready Pipeline

### Context
You are working with a production-ready data processing framework that uses class-based pipeline architecture. The framework supports Metabase as a data source and ClickHouse as a destination, with automatic pagination, backfill capabilities, and data deduplication.

### Framework Architecture
- **Base Classes**: `BasePipeline`, `MetabasePipeline`, `ClickHousePipeline`
- **Generic Utilities**: Data processing, pagination, backfill management, deduplication
- **Extractors**: `create_metabase_paginated_extractor()` for Metabase data sources
- **Loaders**: `create_clickhouse_replace_loader()` for ClickHouse destinations
- **Data Utils**: `prepare_datetime_columns()`, `deduplicate_data()`, `add_merge_metadata()`

### Requirements
Create a complete production-ready pipeline that:

1. **Inherits from appropriate base classes** (`MetabasePipeline`, `ClickHousePipeline`)
2. **Implements the three core methods**: `extract()`, `transform()`, `load()`
3. **Handles multiple data sources** if needed (like merging trades with customer data)
4. **Uses time-based processing** with incremental/backfill modes
5. **Includes comprehensive error handling** and logging
6. **Uses generic utilities** for data processing
7. **Implements data validation** and filtering
8. **Handles edge cases** (empty data, missing records, etc.)
9. **Includes helper methods** for data preparation
10. **Registers the pipeline** for auto-discovery

### Pipeline Specifications
Please provide:
- **Pipeline name and description**
- **Schedule** (cron expression)
- **Source database(s)** and table(s)
- **Target table** and primary key columns
- **Data transformation requirements**
- **Any specific business logic**
- **Error handling requirements**

### Expected Output
Generate a complete Python file with:
1. **Class definition** inheriting from base classes
2. **`__init__` method** with component initialization
3. **`extract()` method** with time-based querying and pagination
4. **`transform()` method** with data processing and merging
5. **`load()` method** with deduplication and error handling
6. **Helper methods** for data preparation and validation
7. **Legacy functions** for backward compatibility
8. **Comprehensive logging** at each step
9. **Type hints** for all methods
10. **Docstrings** explaining the business logic

### Code Style
- Use async/await for all I/O operations
- Include comprehensive logging with `log_with_timestamp()`
- Handle empty datasets gracefully
- Use generic utilities from `pipelines.tools`
- Follow the patterns from `financial_trades_pipeline.py`
- Include proper error handling and validation
- Use descriptive variable names and comments

### Example Structure
```python
class YourPipeline(MetabasePipeline, ClickHousePipeline):
    def __init__(self):
        # Initialize with name, description, schedule
        # Set up extractors and loaders
    
    async def extract(self) -> pd.DataFrame:
        # Get time range, build query, extract with pagination
        # Handle empty data, update last processed time
    
    async def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        # Process data, merge if needed, convert timestamps
        # Add metadata, handle edge cases
    
    async def load(self, data: pd.DataFrame) -> bool:
        # Deduplicate data, load to ClickHouse
        # Handle errors, log results
    
    # Helper methods for data preparation
    # Legacy functions for registration
```

Please create a complete pipeline following these specifications and patterns.

## Prompt 2: Debug Pipeline Issues

### Context
You are debugging a data processing pipeline that is experiencing issues. The pipeline uses the class-based architecture with Metabase sources and ClickHouse destinations.

### Common Issues to Address
- Data extraction failures
- Transformation errors
- Loading problems
- Performance issues
- Data quality problems
- Memory issues with large datasets

### Debugging Steps
1. **Check logs** for specific error messages
2. **Verify data sources** and connectivity
3. **Test individual components** (extract, transform, load)
4. **Check data quality** and validation
5. **Monitor resource usage** and performance
6. **Validate configuration** and environment

### Expected Output
Provide:
- **Root cause analysis** of the issue
- **Step-by-step debugging** approach
- **Code fixes** or configuration changes
- **Prevention strategies** for future issues
- **Monitoring recommendations**

## Prompt 3: Optimize Pipeline Performance

### Context
You need to optimize a data processing pipeline for better performance, scalability, and resource efficiency.

### Optimization Areas
- **Data extraction** efficiency
- **Memory usage** optimization
- **Processing speed** improvements
- **Database performance** tuning
- **Error handling** optimization
- **Resource utilization** improvements

### Performance Metrics
- Processing time per batch
- Memory consumption
- Database query performance
- Error rates
- Throughput (records per minute)

### Expected Output
Provide:
- **Performance analysis** of current pipeline
- **Specific optimization** recommendations
- **Code improvements** for better performance
- **Configuration changes** for optimization
- **Monitoring setup** for performance tracking

## Prompt 4: Add New Data Source

### Context
You need to add a new data source to an existing pipeline or create a new pipeline with a different data source.

### Supported Sources
- **Metabase** (PostgreSQL, MySQL, etc.)
- **HTTP APIs** (REST, GraphQL)
- **Files** (CSV, JSON, Parquet)
- **Databases** (ClickHouse, PostgreSQL, MySQL)

### Requirements
- **Data extraction** from new source
- **Data transformation** for consistency
- **Error handling** for new source
- **Performance optimization** for new data type
- **Testing** and validation

### Expected Output
Provide:
- **New extractor** implementation
- **Pipeline modifications** for new source
- **Data transformation** logic
- **Error handling** strategies
- **Testing approach** for new source

## Usage Instructions

1. **Copy the relevant prompt** for your use case
2. **Fill in the specific requirements** for your pipeline
3. **Provide context** about your data sources and requirements
4. **Use the generated code** as a starting point
5. **Customize and test** according to your needs

## Best Practices

- **Always test** generated code thoroughly
- **Follow the framework patterns** from existing pipelines
- **Include comprehensive logging** and error handling
- **Validate data** at each step
- **Monitor performance** and resource usage
- **Document your changes** and customizations
