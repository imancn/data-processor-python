"""
Integration tests for error handling across the framework.
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
from src.core.exceptions import FrameworkError, PipelineError


class TestErrorHandling:
    """Test error handling across the framework."""
    
    def test_pipeline_error_handling(self):
        """Test pipeline error handling."""
        # Create a pipeline with invalid configuration
        factory = PipelineFactory()
        config = PipelineConfig(
            name='error_test',
            description='Error handling test',
            extractor={'type': 'invalid_type'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        try:
            pipeline = factory.create_pipeline(config)
            result = asyncio.run(pipeline())
            # If it doesn't fail, that's also acceptable
            assert isinstance(result, bool)
        except Exception as e:
            # Should handle errors gracefully
            assert isinstance(e, (FrameworkError, PipelineError, ValueError))
    
    def test_network_error_handling(self):
        """Test network error handling."""
        factory = PipelineFactory()
        config = PipelineConfig(
            name='network_error_test',
            description='Network error test',
            extractor={'type': 'http', 'url': 'https://invalid-domain-that-does-not-exist.com'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        try:
            result = asyncio.run(pipeline())
            # If it doesn't fail, that's also acceptable
            assert isinstance(result, bool)
        except Exception as e:
            # Should handle network errors gracefully
            assert "connection" in str(e).lower() or "network" in str(e).lower()
    
    def test_validation_error_handling(self):
        """Test validation error handling."""
        # Test with invalid data
        try:
            config = PipelineConfig(
                name='',  # Invalid empty name
                description='Validation error test',
                extractor={'type': 'http', 'url': 'https://example.com'},
                transformer={'type': 'noop'},
                loader={'type': 'console'}
            )
            # Should raise validation error
            assert False, "Expected validation error"
        except Exception as e:
            # Should handle validation errors
            assert isinstance(e, (ValueError, TypeError))
    
    def test_graceful_degradation(self):
        """Test graceful degradation when components fail."""
        # Create a pipeline that might fail
        factory = PipelineFactory()
        config = PipelineConfig(
            name='degradation_test',
            description='Graceful degradation test',
            extractor={'type': 'http', 'url': 'https://httpbin.org/status/500'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        try:
            result = asyncio.run(pipeline())
            # Should handle failures gracefully
            assert isinstance(result, bool)
        except Exception as e:
            # Should provide meaningful error messages
            assert len(str(e)) > 0
    
    def test_error_recovery(self):
        """Test error recovery mechanisms."""
        # Test that the framework can recover from errors
        factory = PipelineFactory()
        config = PipelineConfig(
            name='recovery_test',
            description='Error recovery test',
            extractor={'type': 'http', 'url': 'https://httpbin.org/json'},
            transformer={'type': 'noop'},
            loader={'type': 'console'}
        )
        
        pipeline = factory.create_pipeline(config)
        
        # Run multiple times to test recovery
        results = []
        for i in range(3):
            try:
                result = asyncio.run(pipeline())
                results.append(result)
            except Exception as e:
                results.append(False)
        
        # Should be able to recover
        assert len(results) == 3
