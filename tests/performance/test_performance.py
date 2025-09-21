"""
Performance tests for the data processing framework.
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


class TestPerformance:
    """Test framework performance."""
    
    def test_pipeline_execution_time(self):
        """Test pipeline execution time."""
        start_time = time.time()
        
        # Create a simple pipeline
        factory = PipelineFactory()
        config = PipelineConfig(
            name='performance_test',
            description='Performance test pipeline',
            extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        # Run pipeline
        result = asyncio.run(pipeline())
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Pipeline should complete within reasonable time
        assert execution_time < 10.0  # 10 seconds max
        assert result is True
    
    def test_memory_usage(self):
        """Test memory usage during pipeline execution."""
        try:
            import psutil
            import os
        except ImportError:
            pytest.skip("psutil not available, skipping memory test")
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create and run multiple pipelines
        factory = PipelineFactory()
        for i in range(10):
            config = PipelineConfig(
                name=f'perf_test_{i}',
                description='Performance test pipeline',
                extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
                transformer={'type': 'noop'},
                loader={'type': 'console'}
            )
            
            pipeline = factory.create_pipeline(config)
            asyncio.run(pipeline())
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 100MB)
        assert memory_increase < 100 * 1024 * 1024  # 100MB
    
    def test_concurrent_pipeline_execution(self):
        """Test concurrent pipeline execution."""
        async def run_pipeline(pipeline_id):
            factory = PipelineFactory()
            config = PipelineConfig(
                name=f'concurrent_test_{pipeline_id}',
                description='Concurrent test pipeline',
                extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
                transformer={'type': 'noop'},
                loader={'type': 'console'}
            )
            
            pipeline = factory.create_pipeline(config)
            return await pipeline()
        
        # Run 5 pipelines concurrently
        async def run_concurrent():
            return await asyncio.gather(*[run_pipeline(i) for i in range(5)])
        
        start_time = time.time()
        results = asyncio.run(run_concurrent())
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # All pipelines should succeed
        assert all(results)
        
        # Concurrent execution should be faster than sequential
        assert execution_time < 5.0  # Should complete within 5 seconds
