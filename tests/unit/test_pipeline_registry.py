"""
Unit tests for pipeline registry functionality.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipelines import register_pipeline, get_pipeline, get_all_pipelines, remove_pipeline, list_pipeline_names


class TestPipelineRegistry:
    """Test pipeline registry functionality."""
    
    def test_register_pipeline(self):
        """Test pipeline registration."""
        def mock_pipeline():
            return True
        
        pipeline_data = {
            'pipeline': mock_pipeline,
            'schedule': '0 * * * *',
            'description': 'A test pipeline'
        }
        
        register_pipeline('test_pipeline', pipeline_data)
        pipelines = get_all_pipelines()
        assert 'test_pipeline' in pipelines
    
    def test_get_pipeline(self):
        """Test getting a pipeline from registry."""
        def mock_pipeline():
            return True
        
        pipeline_data = {
            'pipeline': mock_pipeline,
            'schedule': '0 * * * *',
            'description': 'A test pipeline'
        }
        
        register_pipeline('test_pipeline', pipeline_data)
        pipeline = get_pipeline('test_pipeline')
        assert pipeline is not None
        assert pipeline['pipeline'] == mock_pipeline
        assert pipeline['description'] == 'A test pipeline'
    
    def test_list_pipeline_names(self):
        """Test listing all pipeline names."""
        names = list_pipeline_names()
        assert isinstance(names, list)
    
    def test_remove_pipeline(self):
        """Test removing a pipeline from registry."""
        def mock_pipeline():
            return True
        
        pipeline_data = {
            'pipeline': mock_pipeline,
            'schedule': '0 * * * *',
            'description': 'A test pipeline'
        }
        
        register_pipeline('test_pipeline', pipeline_data)
        assert 'test_pipeline' in get_all_pipelines()
        
        result = remove_pipeline('test_pipeline')
        assert result is True
        assert 'test_pipeline' not in get_all_pipelines()
    
    def test_get_all_pipelines(self):
        """Test getting all pipelines."""
        pipelines = get_all_pipelines()
        assert isinstance(pipelines, dict)
