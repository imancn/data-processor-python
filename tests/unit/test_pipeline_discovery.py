"""
Unit tests for pipeline discovery functionality.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipelines import discover_and_load_pipelines, get_all_pipelines, list_pipeline_names


class TestPipelineDiscovery:
    """Test pipeline discovery functionality."""
    
    def test_discover_and_load_pipelines(self):
        """Test pipeline discovery and loading."""
        count = discover_and_load_pipelines()
        assert isinstance(count, int)
        assert count >= 0
    
    def test_get_all_pipelines(self):
        """Test getting all pipelines."""
        pipelines = get_all_pipelines()
        assert isinstance(pipelines, dict)
    
    def test_list_pipeline_names(self):
        """Test listing pipeline names."""
        names = list_pipeline_names()
        assert isinstance(names, list)
    
    def test_pipeline_registry_structure(self):
        """Test pipeline registry structure."""
        pipelines = get_all_pipelines()
        
        for name, pipeline_data in pipelines.items():
            assert isinstance(name, str)
            assert isinstance(pipeline_data, dict)
            # Pipeline data should have required keys
            assert 'pipeline' in pipeline_data
