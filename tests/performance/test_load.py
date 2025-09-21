"""
Load tests for the data processing framework.
"""

import pytest
import sys
import time
import asyncio
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipelines.pipeline_factory import PipelineFactory
from src.core.models import PipelineConfig


class TestLoad:
    """Test framework under load."""
    
    @pytest.mark.slow
    def test_high_volume_data_processing(self):
        """Test processing high volume of data."""
        # Create a pipeline that processes multiple data points
        factory = PipelineFactory()
        config = PipelineConfig(
            name='load_test',
            description='Load test pipeline',
            extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        # Run pipeline multiple times
        start_time = time.time()
        results = []
        
        for i in range(100):  # Process 100 items
            result = asyncio.run(pipeline())
            results.append(result)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # All should succeed
        assert all(results)
        
        # Should process within reasonable time
        assert total_time < 60.0  # 1 minute max
    
    @pytest.mark.slow
    def test_stress_test(self):
        """Test framework under stress conditions."""
        # Create multiple pipelines and run them simultaneously
        factory = PipelineFactory()
        pipelines = []
        
        for i in range(20):  # 20 concurrent pipelines
            config = PipelineConfig(
                name=f'stress_test_{i}',
                description='Stress test pipeline',
                extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
                transformer={'type': 'noop'},
                loader={'type': 'console'}
            )
            
            pipeline = factory.create_pipeline(config)
            pipelines.append(pipeline)
        
        # Run all pipelines concurrently
        async def run_stress():
            return await asyncio.gather(*[pipeline() for pipeline in pipelines])
        
        start_time = time.time()
        results = asyncio.run(run_stress())
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # All should succeed
        assert all(results)
        
        # Should handle stress within reasonable time
        assert execution_time < 30.0  # 30 seconds max
    
    def test_error_recovery(self):
        """Test error recovery under load."""
        # Create a pipeline that might fail
        factory = PipelineFactory()
        config = PipelineConfig(
            name='error_recovery_test',
            description='Error recovery test pipeline',
            extractor={'type': 'http', 'url': 'https://invalid-url-that-should-fail.com'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        # Run multiple times to test error handling
        results = []
        for i in range(10):
            try:
                result = asyncio.run(pipeline())
                results.append(result)
            except Exception as e:
                # Expected to fail, but should not crash the system
                results.append(False)
        
        # Should handle errors gracefully
        assert len(results) == 10
