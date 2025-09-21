"""
Unit tests for core framework components.
"""
import os
import sys
import time
import tempfile
from pathlib import Path
from datetime import datetime
import pytest
import pandas as pd

# Add src to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from core.config import config
from core.logging import setup_logging, log_with_timestamp
from pipelines.pipeline_factory import create_etl_pipeline, create_el_pipeline
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.loaders.console_loader import create_console_loader
from pipelines.tools.transformers.transformers import create_lambda_transformer

@pytest.mark.unit
class TestCoreConfig:
    """Unit tests for configuration system."""
    
    def test_config_get_method(self, test_results_collector):
        """Test config.get() method."""
        start_time = time.time()
        
        try:
            # Test getting existing env var
            os.environ['LOG_LEVEL'] = 'DEBUG'
            config.reload()  # Reload config to pick up environment variable changes
            value = config.get('LOG_LEVEL')
            assert value == 'DEBUG'
            
            # Test getting non-existent var with default
            value = config.get('NON_EXISTENT_VAR', 'default_value')
            assert value == 'default_value'
            
            # Test getting non-existent var without default
            value = config.get('NON_EXISTENT_VAR')
            assert value is None
            
            # Cleanup
            if 'LOG_LEVEL' in os.environ:
                del os.environ['LOG_LEVEL']
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_config_get_method',
                'PASSED',
                duration,
                {'config_operations': 3}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_config_get_method',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_clickhouse_config(self, test_env, test_results_collector):
        """Test ClickHouse configuration."""
        start_time = time.time()
        
        try:
            ch_config = config.get_clickhouse_config()
            
            required_keys = ['host', 'port', 'user', 'password', 'database']
            for key in required_keys:
                assert key in ch_config
            
            # Test type conversions
            assert isinstance(ch_config['port'], int)
            assert isinstance(ch_config['host'], str)
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_clickhouse_config',
                'PASSED',
                duration,
                {'required_keys': len(required_keys)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_clickhouse_config',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.unit
class TestLoggingSystem:
    """Unit tests for logging system."""
    
    def test_log_with_timestamp(self, test_env, test_results_collector):
        """Test log_with_timestamp function."""
        start_time = time.time()
        
        try:
            # Test different log levels
            log_with_timestamp("Info message", "TestUnit", "info")
            log_with_timestamp("Warning message", "TestUnit", "warning")
            log_with_timestamp("Error message", "TestUnit", "error")
            log_with_timestamp("Debug message", "TestUnit", "debug")
            
            # Test with category
            log_with_timestamp("Categorized message", "TestUnit", "info", "test_category")
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_log_with_timestamp',
                'PASSED',
                duration,
                {'log_levels_tested': 4, 'category_test': 1}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_log_with_timestamp',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_setup_logging(self, test_env, test_results_collector):
        """Test setup_logging function."""
        start_time = time.time()
        
        try:
            # Test different log levels
            setup_logging('DEBUG')
            setup_logging('INFO')
            setup_logging('WARNING')
            setup_logging('ERROR')
            
            # Test with custom log file
            temp_log = os.path.join(test_env['temp_dir'], 'custom.log')
            setup_logging('INFO', temp_log)
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_setup_logging',
                'PASSED',
                duration,
                {'log_levels_tested': 4, 'custom_file_test': 1}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_setup_logging',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.unit
class TestPipelineFactory:
    """Unit tests for pipeline factory."""
    
    def test_create_etl_pipeline(self, sample_pipeline_data, test_results_collector):
        """Test ETL pipeline creation."""
        start_time = time.time()
        
        try:
            # Create mock components
            async def mock_extractor():
                return pd.DataFrame(sample_pipeline_data['test_data'])
            
            def mock_transformer(df):
                df['transformed'] = True
                return df
            
            def mock_loader(df):
                return len(df) > 0
            
            # Create pipeline
            pipeline = create_etl_pipeline(
                extractor=mock_extractor,
                transformer=create_lambda_transformer(mock_transformer, "Mock Transformer"),
                loader=mock_loader,
                name="Test ETL Pipeline"
            )
            
            # Test pipeline execution
            import asyncio
            result = asyncio.run(pipeline())
            assert result is True
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_create_etl_pipeline',
                'PASSED',
                duration,
                {'pipeline_components': 3}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_create_etl_pipeline',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_create_el_pipeline(self, sample_pipeline_data, test_results_collector):
        """Test EL pipeline creation."""
        start_time = time.time()
        
        try:
            # Create mock components
            async def mock_extractor():
                return pd.DataFrame(sample_pipeline_data['test_data'])
            
            def mock_loader(df):
                return len(df) > 0
            
            # Create pipeline
            pipeline = create_el_pipeline(
                extractor=mock_extractor,
                loader=mock_loader,
                name="Test EL Pipeline"
            )
            
            # Test pipeline execution
            import asyncio
            result = asyncio.run(pipeline())
            assert result is True
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_create_el_pipeline',
                'PASSED',
                duration,
                {'pipeline_components': 2}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_create_el_pipeline',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.unit
class TestTransformers:
    """Unit tests for transformers."""
    
    def test_lambda_transformer(self, sample_pipeline_data, test_results_collector):
        """Test lambda transformer creation and execution."""
        start_time = time.time()
        
        try:
            # Create test data
            df = pd.DataFrame(sample_pipeline_data['test_data'])
            
            # Create transformer
            def add_processed_flag(data):
                data['processed'] = True
                data['process_time'] = datetime.now()
                return data
            
            transformer = create_lambda_transformer(add_processed_flag, "Test Lambda Transformer")
            
            # Test transformer execution
            result = transformer(df)
            
            assert 'processed' in result.columns
            assert 'process_time' in result.columns
            assert all(result['processed'] == True)
            assert len(result) == len(df)
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_lambda_transformer',
                'PASSED',
                duration,
                {'input_records': len(df), 'output_records': len(result)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_lambda_transformer',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.unit
class TestLoaders:
    """Unit tests for loaders."""
    
    def test_console_loader(self, sample_pipeline_data, test_results_collector):
        """Test console loader."""
        start_time = time.time()
        
        try:
            # Create test data
            df = pd.DataFrame(sample_pipeline_data['test_data'])
            
            # Create console loader
            loader = create_console_loader("Test Console Loader")
            
            # Test loader execution
            result = loader(df)
            
            # Console loader should return True for successful "loading"
            assert result is True
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_console_loader',
                'PASSED',
                duration,
                {'records_loaded': len(df)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_console_loader',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.unit
class TestExtractors:
    """Unit tests for extractors."""
    
    def test_http_extractor_creation(self, test_results_collector):
        """Test HTTP extractor creation."""
        start_time = time.time()
        
        try:
            # Create HTTP extractor
            extractor = create_http_extractor(
                url="https://httpbin.org/json",
                headers={"Accept": "application/json"},
                params={"test": "value"},
                name="Test HTTP Extractor"
            )
            
            # Test that extractor is callable
            assert callable(extractor)
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_http_extractor_creation',
                'PASSED',
                duration,
                {'extractor_created': True}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_http_extractor_creation',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise

@pytest.mark.unit
class TestMainModule:
    """Unit tests for main module functions."""
    
    def test_pipeline_registration(self, test_env, test_results_collector):
        """Test pipeline registration system."""
        start_time = time.time()
        
        try:
            from main import register_all_pipelines, list_cron_jobs, _cron_registry
            
            # Clear registry
            _cron_registry.clear()
            
            # Test registration with no pipelines
            register_all_pipelines()
            jobs = list_cron_jobs()
            
            # Should be empty for blank project
            assert len(jobs) == 0
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_pipeline_registration',
                'PASSED',
                duration,
                {'registered_jobs': len(jobs)}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_pipeline_registration',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
    
    def test_cron_job_management(self, test_env, test_results_collector):
        """Test cron job management functions."""
        start_time = time.time()
        
        try:
            from main import register_cron_job, list_cron_jobs, unregister_cron_job, _cron_registry
            
            # Clear registry
            _cron_registry.clear()
            
            # Test job registration
            def dummy_pipeline():
                return True
            
            register_cron_job(
                job_name="test_job",
                pipeline=dummy_pipeline,
                schedule="0 * * * *",
                description="Test job"
            )
            
            jobs = list_cron_jobs()
            assert "test_job" in jobs
            assert jobs["test_job"]["config"].schedule == "0 * * * *"
            
            # Test job unregistration
            result = unregister_cron_job("test_job")
            assert result is True
            
            jobs = list_cron_jobs()
            assert "test_job" not in jobs
            
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_cron_job_management',
                'PASSED',
                duration,
                {'job_operations': 3}
            )
            
        except Exception as e:
            duration = time.time() - start_time
            test_results_collector.add_result(
                'test_cron_job_management',
                'FAILED',
                duration,
                {'error': str(e)}
            )
            raise
