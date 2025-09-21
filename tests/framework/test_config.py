"""
Test configuration management for the data processing framework.
"""

from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass

__all__ = ['TestCategoryConfig', 'TestFrameworkConfig']


@dataclass
class TestCategoryConfig:
    """Configuration for a test category."""
    name: str
    description: str
    test_files: List[str]
    coverage_paths: List[str]
    test_type: str = "unit"  # unit, integration, performance


class TestFrameworkConfig:
    """Centralized test configuration management."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.results_dir = project_root / 'tests' / 'results'
        self.results_dir.mkdir(exist_ok=True)
        
        # Define test categories
        self.categories = {
            'core': TestCategoryConfig(
                name='Core Framework Tests',
                description='Tests for core framework components (config, logging, validation, exceptions)',
                test_files=[
                    'tests/unit/test_core_config.py',
                    'tests/unit/test_core_logging.py', 
                    'tests/unit/test_core_validation.py',
                    'tests/unit/test_core_exceptions.py',
                    'tests/unit/test_core_components.py'
                ],
                coverage_paths=['src/core'],
                test_type='unit'
            ),
            'pipelines': TestCategoryConfig(
                name='Pipeline System Tests',
                description='Tests for pipeline system (discovery, factory, registry, tools)',
                test_files=[
                    'tests/unit/test_pipeline_discovery.py',
                    'tests/unit/test_pipeline_factory.py',
                    'tests/unit/test_pipeline_registry.py',
                    'tests/unit/test_pipeline_tools.py'
                ],
                coverage_paths=['src/pipelines'],
                test_type='unit'
            ),
            'validation': TestCategoryConfig(
                name='Validation System Tests',
                description='Tests for Pydantic validation system',
                test_files=[
                    'tests/unit/test_pydantic_validation.py',
                    'tests/integration/test_pydantic_integration.py'
                ],
                coverage_paths=['src/core'],
                test_type='mixed'
            ),
            'migrations': TestCategoryConfig(
                name='Migration System Tests',
                description='Tests for database migration system',
                test_files=[
                    'tests/unit/test_migration_manager.py',
                    'tests/integration/test_migration_integration.py'
                ],
                coverage_paths=['migrations'],
                test_type='mixed'
            ),
            'deployment': TestCategoryConfig(
                name='Deployment System Tests',
                description='Tests for deployment and operations',
                test_files=[
                    'tests/integration/test_deployment.py',
                    'tests/integration/test_operations.py'
                ],
                coverage_paths=[],
                test_type='integration'
            ),
            'integration': TestCategoryConfig(
                name='Integration Tests',
                description='End-to-end integration tests',
                test_files=[
                    'tests/integration/test_framework_integration.py',
                    'tests/integration/test_data_flow.py',
                    'tests/integration/test_error_handling.py'
                ],
                coverage_paths=['src'],
                test_type='integration'
            ),
            'performance': TestCategoryConfig(
                name='Performance Tests',
                description='Performance and load tests',
                test_files=[
                    'tests/performance/test_performance.py',
                    'tests/performance/test_load.py'
                ],
                coverage_paths=['src'],
                test_type='performance'
            ),
            'all': TestCategoryConfig(
                name='All Tests',
                description='Run all available tests',
                test_files=[],  # Will be populated dynamically
                coverage_paths=['src', 'migrations'],
                test_type='mixed'
            )
        }
    
    def get_category(self, category: str) -> 'TestCategoryConfig':
        """Get a specific test category."""
        return self.categories.get(category)
    
    def get_available_categories(self) -> List[str]:
        """Get list of available test categories."""
        return list(self.categories.keys())
    
    def get_categories_by_type(self, test_type: str) -> List[str]:
        """Get categories filtered by test type."""
        return [
            name for name, category in self.categories.items()
            if category.test_type == test_type or category.test_type == 'mixed'
        ]
