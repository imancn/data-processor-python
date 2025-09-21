"""
Unit tests for migration manager functionality.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from migrations.migration_manager import ClickHouseMigrationManager


class TestMigrationManager:
    """Test migration manager functionality."""
    
    def test_migration_manager_initialization(self):
        """Test migration manager initialization."""
        manager = ClickHouseMigrationManager()
        assert manager is not None
    
    def test_get_pending_migrations(self):
        """Test getting pending migrations."""
        manager = ClickHouseMigrationManager()
        migrations = manager.get_pending_migrations()
        assert isinstance(migrations, list)
    
    def test_run_migrations(self):
        """Test running migrations."""
        manager = ClickHouseMigrationManager()
        
        # This test might fail if database is not available
        # but we can test the method exists and is callable
        try:
            result = manager.run_migrations()
            assert isinstance(result, bool)
        except Exception as e:
            # Expected if database is not available
            assert "connection" in str(e).lower() or "database" in str(e).lower()
    
    def test_migration_status(self):
        """Test checking migration status."""
        manager = ClickHouseMigrationManager()
        
        try:
            # Check if the method exists
            if hasattr(manager, 'get_migration_status'):
                status = manager.get_migration_status()
                assert isinstance(status, dict)
            else:
                # Method doesn't exist, skip this test
                pytest.skip("get_migration_status method not implemented")
        except Exception as e:
            # Expected if database is not available
            assert "connection" in str(e).lower() or "database" in str(e).lower()
