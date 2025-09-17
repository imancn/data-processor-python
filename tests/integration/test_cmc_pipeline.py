# tests/integration/test_cmc_pipeline.py
"""
Integration tests for CMC pipeline.
"""
import pytest
import asyncio
import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from pipelines.cmc_pipeline import register_cmc_pipelines, get_cmc_pipeline, list_cmc_pipelines

class TestCMCPipeline:
    """Test cases for CMC pipeline integration."""
    
    def test_register_cmc_pipelines(self):
        """Test registering CMC pipelines."""
        # This should not raise an exception
        register_cmc_pipelines()
        
        # Check that pipelines are registered (if API key is available)
        pipelines = list_cmc_pipelines()
        if os.getenv('CMC_API_KEY'):
            assert len(pipelines) > 0
            assert "cmc_latest_quotes" in pipelines
            assert "cmc_historical_data" in pipelines
        else:
            # Without API key, no pipelines should be registered
            assert len(pipelines) == 0
    
    def test_get_cmc_pipeline(self):
        """Test getting a CMC pipeline."""
        register_cmc_pipelines()
        
        if os.getenv('CMC_API_KEY'):
            # Get a pipeline
            pipeline = get_cmc_pipeline("cmc_latest_quotes")
            assert pipeline is not None
            assert callable(pipeline)
        else:
            # Without API key, pipeline should be None
            pipeline = get_cmc_pipeline("cmc_latest_quotes")
            assert pipeline is None
        
        # Test getting non-existent pipeline
        non_existent = get_cmc_pipeline("non_existent")
        assert non_existent is None
    
    @pytest.mark.skipif(not os.getenv('CMC_API_KEY'), reason="CMC_API_KEY not set")
    def test_cmc_latest_quotes_pipeline(self):
        """Test running CMC latest quotes pipeline (requires API key)."""
        register_cmc_pipelines()
        
        pipeline = get_cmc_pipeline("cmc_latest_quotes")
        assert pipeline is not None
        
        # Run pipeline (this will make actual API calls if API key is available)
        result = asyncio.run(pipeline())
        
        # Should return True (success) or False (failure, but no exception)
        assert isinstance(result, bool)
    
    def test_cmc_pipeline_without_api_key(self):
        """Test CMC pipeline behavior without API key."""
        # Temporarily remove API key
        original_key = os.environ.get('CMC_API_KEY')
        if 'CMC_API_KEY' in os.environ:
            del os.environ['CMC_API_KEY']
        
        try:
            register_cmc_pipelines()
            pipeline = get_cmc_pipeline("cmc_latest_quotes")
            
            # Without API key, pipeline should be None
            assert pipeline is None
            
        finally:
            # Restore original API key
            if original_key:
                os.environ['CMC_API_KEY'] = original_key
