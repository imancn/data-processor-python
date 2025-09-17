# tests/unit/test_pipeline_factory.py
"""
Unit tests for pipeline factory.
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock

# Add src to path
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from pipelines.pipeline_factory import create_el_pipeline, create_etl_pipeline, run_pipeline

class TestPipelineFactory:
    """Test cases for pipeline factory."""
    
    def test_create_el_pipeline(self):
        """Test creating an EL pipeline."""
        # Mock extractor and loader
        extractor = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        loader = Mock(return_value=True)
        
        # Create pipeline
        pipeline = create_el_pipeline(extractor, loader, "Test EL Pipeline")
        
        # Run pipeline
        result = asyncio.run(pipeline())
        
        # Assertions
        assert result is True
        extractor.assert_called_once()
        loader.assert_called_once_with([{"id": 1, "name": "test"}])
    
    def test_create_etl_pipeline(self):
        """Test creating an ETL pipeline."""
        # Mock components
        extractor = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        transformer = Mock(return_value=[{"id": 1, "name": "transformed"}])
        loader = Mock(return_value=True)
        
        # Create pipeline
        pipeline = create_etl_pipeline(extractor, transformer, loader, "Test ETL Pipeline")
        
        # Run pipeline
        result = asyncio.run(pipeline())
        
        # Assertions
        assert result is True
        extractor.assert_called_once()
        transformer.assert_called_once_with([{"id": 1, "name": "test"}])
        loader.assert_called_once_with([{"id": 1, "name": "transformed"}])
    
    def test_pipeline_with_no_data(self):
        """Test pipeline behavior with no data."""
        # Mock extractor returning empty data
        extractor = AsyncMock(return_value=[])
        loader = Mock(return_value=True)
        
        # Create pipeline
        pipeline = create_el_pipeline(extractor, loader, "Test Empty Pipeline")
        
        # Run pipeline
        result = asyncio.run(pipeline())
        
        # Should still return True (no data is not an error)
        assert result is True
        loader.assert_not_called()  # Loader should not be called with empty data
    
    def test_pipeline_with_extractor_error(self):
        """Test pipeline behavior when extractor fails."""
        # Mock extractor that raises an exception
        extractor = AsyncMock(side_effect=Exception("Extraction failed"))
        loader = Mock(return_value=True)
        
        # Create pipeline
        pipeline = create_el_pipeline(extractor, loader, "Test Error Pipeline")
        
        # Run pipeline
        result = asyncio.run(pipeline())
        
        # Should return False on error
        assert result is False
        loader.assert_not_called()
    
    def test_pipeline_with_loader_error(self):
        """Test pipeline behavior when loader fails."""
        # Mock components
        extractor = AsyncMock(return_value=[{"id": 1, "name": "test"}])
        loader = Mock(return_value=False)  # Loader fails
        
        # Create pipeline
        pipeline = create_el_pipeline(extractor, loader, "Test Loader Error Pipeline")
        
        # Run pipeline
        result = asyncio.run(pipeline())
        
        # Should return False when loader fails
        assert result is False
        extractor.assert_called_once()
        loader.assert_called_once()
