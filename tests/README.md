# Testing Framework

## 🧪 Overview

The testing framework provides a comprehensive, modular testing system for the data processing framework. It allows you to run any part of the project tests easily and is designed to be extensible for new test categories.

## 🏗️ Test Structure

```
tests/
├── test_runner.py              # Main test runner
├── unit/                       # Unit tests
│   ├── test_core_config.py    # Core configuration tests
│   ├── test_core_logging.py   # Core logging tests
│   ├── test_core_validation.py # Core validation tests
│   ├── test_core_exceptions.py # Core exception tests
│   └── test_pydantic_validation.py # Pydantic validation tests
├── integration/                # Integration tests
│   ├── test_framework_integration.py # Framework integration tests
│   ├── test_pydantic_integration.py # Pydantic integration tests
│   └── test_deployment.py     # Deployment tests
├── performance/                # Performance tests
│   └── (performance test files)
└── results/                    # Test results and coverage
    ├── README.md              # Results documentation
    ├── .gitkeep              # Git placeholder
    └── (generated test results)
```

## 🎯 Test Categories

### Core Framework Tests (`core`)
- **Description**: Tests for core framework components (config, logging, validation, exceptions)
- **Test Files**: 
  - `test_core_config.py` - Configuration system tests
  - `test_core_logging.py` - Logging system tests
  - `test_core_validation.py` - Validation system tests
  - `test_core_exceptions.py` - Exception system tests
- **Coverage**: `src/core`

### Pipeline System Tests (`pipelines`)
- **Description**: Tests for pipeline system (discovery, factory, registry, tools)
- **Test Files**: (to be created)
  - `test_pipeline_discovery.py` - Pipeline discovery tests
  - `test_pipeline_factory.py` - Pipeline factory tests
  - `test_pipeline_registry.py` - Pipeline registry tests
  - `test_pipeline_tools.py` - Pipeline tools tests
- **Coverage**: `src/pipelines`

### Validation System Tests (`validation`)
- **Description**: Tests for Pydantic validation system
- **Test Files**:
  - `test_pydantic_validation.py` - Pydantic validation unit tests
  - `test_pydantic_integration.py` - Pydantic validation integration tests
- **Coverage**: `src/core`

### Migration System Tests (`migrations`)
- **Description**: Tests for database migration system
- **Test Files**: (to be created)
  - `test_migration_manager.py` - Migration manager tests
  - `test_migration_integration.py` - Migration integration tests
- **Coverage**: `migrations`

### Deployment System Tests (`deployment`)
- **Description**: Tests for deployment and operations
- **Test Files**:
  - `test_deployment.py` - Deployment tests
  - `test_operations.py` - Operations tests (to be created)
- **Coverage**: (none)

### Integration Tests (`integration`)
- **Description**: End-to-end integration tests
- **Test Files**:
  - `test_framework_integration.py` - Framework integration tests
  - `test_data_flow.py` - Data flow tests (to be created)
  - `test_error_handling.py` - Error handling tests (to be created)
- **Coverage**: `src`

### Performance Tests (`performance`)
- **Description**: Performance and load tests
- **Test Files**: (to be created)
  - `test_performance.py` - Performance tests
  - `test_load.py` - Load tests
- **Coverage**: `src`

### All Tests (`all`)
- **Description**: Run all available tests
- **Test Files**: All test files in the project
- **Coverage**: `src`, `migrations`

## 🚀 Usage

### Command Line Interface

```bash
# Run all tests
python3 tests/test_runner.py

# Run specific category
python3 tests/test_runner.py core
python3 tests/test_runner.py validation
python3 tests/test_runner.py integration

# List available categories
python3 tests/test_runner.py --list

# Create test template
python3 tests/test_runner.py --create pipelines discovery

# Run without coverage
python3 tests/test_runner.py core --no-coverage

# Quiet mode
python3 tests/test_runner.py core --quiet
```

### Using run.sh

```bash
# Run all tests
./run.sh test

# Run specific category
./run.sh test core
./run.sh test validation
./run.sh test integration

# List available categories
./run.sh list_tests

# Create test template
./run.sh create_test pipelines discovery
```

## 📊 Test Results

### Results Directory Structure

```
tests/results/
├── README.md                           # Results documentation
├── .gitkeep                           # Git placeholder
├── test_summary_core.json             # Core tests summary
├── test_summary_validation.json       # Validation tests summary
├── test_summary_overall.json          # Overall test summary
├── test_results_core.json             # Detailed core test results
├── coverage_core.json                 # Core tests coverage data
├── coverage_html_core/                # Core tests HTML coverage
└── (other category-specific files)
```

### Understanding Results

- **Test Summary**: High-level test results with counts and timing
- **Coverage Reports**: Code coverage analysis and metrics
- **Detailed Results**: Complete test execution data
- **Overall Summary**: Aggregated results across all categories

## 🔧 Creating New Tests

### Using the Test Template Generator

```bash
# Create a new test for any category
./run.sh create_test CATEGORY TEST_NAME

# Examples
./run.sh create_test pipelines discovery
./run.sh create_test migrations manager
./run.sh create_test performance load
```

### Manual Test Creation

1. **Choose the appropriate directory**:
   - `tests/unit/` - Unit tests
   - `tests/integration/` - Integration tests
   - `tests/performance/` - Performance tests

2. **Follow the naming convention**: `test_*.py`

3. **Use the test template structure**:
   ```python
   """
   Category Tests - Test Name
   
   Description of what this test covers
   """
   
   import pytest
   import sys
   from pathlib import Path
   
   # Add project root to Python path
   project_root = Path(__file__).parent.parent.parent
   sys.path.insert(0, str(project_root))
   
   class TestTestName:
       """Test class for test_name."""
       
       def test_example(self):
           """Example test method."""
           # Add your test logic here
           assert True
   
   if __name__ == '__main__':
       pytest.main([__file__])
   ```

## 🎯 Best Practices

### Test Development
- **Write comprehensive unit tests** for individual components
- **Include integration tests** for system workflows
- **Add performance tests** for critical paths
- **Test error scenarios** and edge cases

### Test Organization
- **Group related tests** in the same file
- **Use descriptive test names** that explain what is being tested
- **Follow the existing naming conventions**
- **Keep tests focused** on a single responsibility

### Test Coverage
- **Aim for high coverage** on critical components
- **Test all public interfaces**
- **Include error handling tests**
- **Validate configuration scenarios**

## 🔍 Troubleshooting

### Common Issues

**"No module named 'pydantic'"**
- Install required dependencies: `pip install pydantic pydantic-settings`
- Or run tests without Pydantic-dependent categories

**"Test file not found"**
- Check that test files exist in the correct directory
- Verify the naming convention (`test_*.py`)
- Use the template generator to create new tests

**"Import errors"**
- Ensure the project root is in the Python path
- Check that all required modules are available
- Verify the test file structure

### Debugging Tests

```bash
# Run tests with verbose output
python3 tests/test_runner.py core -v

# Run specific test file
python3 -m pytest tests/unit/test_core_config.py -v

# Run tests with debugging
python3 -m pytest tests/unit/test_core_config.py -v -s
```

## 📈 Extending the Testing System

### Adding New Test Categories

1. **Update `test_runner.py`**:
   ```python
   self.test_categories['new_category'] = {
       'name': 'New Category Tests',
       'description': 'Description of new category',
       'test_files': [
           'tests/unit/test_new_category.py',
           'tests/integration/test_new_category.py'
       ],
       'coverage_paths': ['src/new_category']
   }
   ```

2. **Create test files** in the appropriate directories

3. **Update documentation** to include the new category

### Adding New Test Features

- **Custom test runners** for specific scenarios
- **Test data generators** for consistent test data
- **Mock utilities** for external dependencies
- **Performance benchmarks** for critical operations

## 🎉 Summary

The testing framework provides:

- ✅ **Modular Testing**: Run any part of the project tests
- ✅ **Easy Extension**: Simple to add new test categories
- ✅ **Comprehensive Coverage**: Unit, integration, and performance tests
- ✅ **Rich Reporting**: Detailed results and coverage analysis
- ✅ **Template Generation**: Easy creation of new tests
- ✅ **Command Line Interface**: Both direct and run.sh integration
- ✅ **Flexible Configuration**: Customizable test execution

This system makes it easy to maintain high code quality and ensure the reliability of the data processing framework.
