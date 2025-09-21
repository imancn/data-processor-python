"""
Unit tests for pipeline factory functionality.
"""

import pytest
import sys
import asyncio
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipelines import create_etl_pipeline, create_el_pipeline
from src.core.models import PipelineConfig


class TestPipelineFactory:
    """Test pipeline factory functionality."""
    
    def test_create_etl_pipeline(self):
        """Test ETL pipeline creation."""
        import pandas as pd
        
        def mock_extractor():
            return pd.DataFrame({'test': [1, 2, 3]})
        
        def mock_transformer(df):
            return df
        
        def mock_loader(df):
            return len(df) > 0
        
        pipeline = create_etl_pipeline(
            extractor=mock_extractor,
            transformer=mock_transformer,
            loader=mock_loader,
            name="Test ETL Pipeline"
        )
        
        assert pipeline is not None
        assert callable(pipeline)
    
    def test_create_el_pipeline(self):
        """Test EL pipeline creation."""
        async def mock_extractor():
            import pandas as pd
            return pd.DataFrame({'test': [1, 2, 3]})
        
        def mock_loader(df):
            return len(df) > 0
        
        pipeline = create_el_pipeline(
            extractor=mock_extractor,
            loader=mock_loader,
            name="Test EL Pipeline"
        )
        
        assert pipeline is not None
        assert callable(pipeline)
    
    def test_pipeline_execution(self):
        """Test pipeline execution."""
        async def mock_extractor():
            import pandas as pd
            return pd.DataFrame({'test': [1, 2, 3]})
        
        def mock_loader(df):
            return len(df) > 0
        
        pipeline = create_el_pipeline(
            extractor=mock_extractor,
            loader=mock_loader,
            name="Test Pipeline"
        )
        
        # Test pipeline execution
        result = asyncio.run(pipeline())
        assert result is True
