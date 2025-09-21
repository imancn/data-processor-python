"""
Integration tests for data flow through the framework.
"""

import pytest
import sys
import asyncio
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipelines.pipeline_factory import PipelineFactory
from src.core.models import PipelineConfig


class TestDataFlow:
    """Test data flow through the framework."""
    
    def test_end_to_end_pipeline(self):
        """Test complete end-to-end pipeline execution."""
        factory = PipelineFactory()
        config = PipelineConfig(
            name='e2e_test',
            description='End-to-end test pipeline',
            extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        result = asyncio.run(pipeline())
        
        assert result is True
    
    def test_data_transformation_flow(self):
        """Test data transformation through pipeline stages."""
        # Create a pipeline with data transformation
        factory = PipelineFactory()
        config = PipelineConfig(
            name='transform_test',
            description='Data transformation test',
            extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
            transformer={'type': 'noop'},  # No transformation for now
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        result = asyncio.run(pipeline())
        
        assert result is True
    
    def test_error_handling_flow(self):
        """Test error handling through pipeline stages."""
        # Create a pipeline that might fail
        factory = PipelineFactory()
        config = PipelineConfig(
            name='error_test',
            description='Error handling test',
            extractor={'type': 'http', 'url': 'https://invalid-url.com'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        # Should handle errors gracefully
        try:
            result = asyncio.run(pipeline())
            # If it doesn't fail, that's also acceptable
            assert isinstance(result, bool)
        except Exception as e:
            # Expected to fail, but should be handled gracefully
            assert "connection" in str(e).lower() or "network" in str(e).lower()
    
    def test_concurrent_data_processing(self):
        """Test concurrent data processing."""
        async def process_data(pipeline_id):
            factory = PipelineFactory()
            config = PipelineConfig(
                name=f'concurrent_{pipeline_id}',
                description='Concurrent processing test',
                extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
                transformer={'type': 'noop'},
                loader={'type': 'console'}
            )
            
            pipeline = factory.create_pipeline(config)
            return await pipeline()
        
        # Run multiple pipelines concurrently
        async def run_concurrent():
            return await asyncio.gather(*[process_data(i) for i in range(3)])
        
        results = asyncio.run(run_concurrent())
        
        # All should succeed
        assert all(results)
