"""
Unit tests for pipeline tools functionality.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.pipelines.tools import (
    create_http_extractor,
    create_clickhouse_extractor,
    create_metabase_extractor,
    create_lambda_transformer,
    create_clickhouse_loader,
    create_console_loader
)


class TestPipelineTools:
    """Test pipeline tools functionality."""
    
    def test_http_extractor_creation(self):
        """Test creating an HTTP extractor."""
        extractor = create_http_extractor('https://example.com')
        assert extractor is not None
        assert callable(extractor)
    
    def test_clickhouse_extractor_creation(self):
        """Test creating a ClickHouse extractor."""
        extractor = create_clickhouse_extractor('SELECT * FROM test_table')
        assert extractor is not None
        assert callable(extractor)
    
    def test_metabase_extractor_creation(self):
        """Test creating a Metabase extractor."""
        extractor = create_metabase_extractor(
            base_url="https://test.metabase.com",
            api_key="test_key",
            database_id=1,
            table_id=2
        )
        assert extractor is not None
        assert callable(extractor)
    
    def test_lambda_transformer_creation(self):
        """Test creating a lambda transformer."""
        transformer = create_lambda_transformer(lambda df: df)
        assert transformer is not None
        assert callable(transformer)
    
    def test_clickhouse_loader_creation(self):
        """Test creating a ClickHouse loader."""
        loader = create_clickhouse_loader('test_table', ['id', 'name'])
        assert loader is not None
        assert callable(loader)
    
    def test_console_loader_creation(self):
        """Test creating a console loader."""
        loader = create_console_loader()
        assert loader is not None
        assert callable(loader)
    
    def test_tools_are_callable(self):
        """Test that all tools return callable functions."""
        # Test HTTP extractor
        http_extractor = create_http_extractor('https://example.com')
        assert callable(http_extractor)
        
        # Test ClickHouse extractor
        ch_extractor = create_clickhouse_extractor('SELECT 1')
        assert callable(ch_extractor)
        
        # Test Metabase extractor
        metabase_extractor = create_metabase_extractor(
            base_url="https://test.metabase.com",
            api_key="test_key",
            database_id=1,
            table_id=2
        )
        assert callable(metabase_extractor)
        
        # Test transformer
        transformer = create_lambda_transformer(lambda df: df)
        assert callable(transformer)
        
        # Test loaders
        ch_loader = create_clickhouse_loader('test', ['id'])
        assert callable(ch_loader)
        
        console_loader = create_console_loader()
        assert callable(console_loader)
    
    # Metabase Extractor Tests
    def test_metabase_extractor_query_creation(self):
        """Test creating a query extractor."""
        extractor = create_metabase_extractor(
            base_url="https://test.metabase.com",
            api_key="test_key",
            database_id=1,
            native_query="SELECT * FROM users",
            name="Test Query Extractor"
        )
        
        assert callable(extractor)
        assert extractor.__name__ == "extractor_func"
    
    def test_create_metabase_extractor_invalid_params(self):
        """Test creating extractor with invalid parameters."""
        # Test with neither table_id nor native_query
        with pytest.raises(ValueError, match="Either table_id or native_query must be provided"):
            create_metabase_extractor(
                base_url="https://test.metabase.com",
                api_key="test_key",
                database_id=1
            )
        
        # Test with both table_id and native_query
        with pytest.raises(ValueError, match="Cannot specify both table_id and native_query"):
            create_metabase_extractor(
                base_url="https://test.metabase.com",
                api_key="test_key",
                database_id=1,
                table_id=2,
                native_query="SELECT * FROM users"
            )
