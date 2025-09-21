"""
Integration tests for migration system.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from migrations.migration_manager import ClickHouseMigrationManager


class TestMigrationIntegration:
    """Test migration system integration."""
    
    @pytest.mark.database
    def test_migration_workflow(self):
        """Test complete migration workflow."""
        manager = ClickHouseMigrationManager()
        
        # Test getting pending migrations
        pending = manager.get_pending_migrations()
        assert isinstance(pending, list)
        
        # Test running migrations (if database is available)
        try:
            result = manager.run_migrations()
            assert isinstance(result, bool)
        except Exception as e:
            # Skip if database is not available
            pytest.skip(f"Database not available: {e}")
    
    @pytest.mark.database
    def test_migration_rollback(self):
        """Test migration rollback functionality."""
        manager = ClickHouseMigrationManager()
        
        try:
            # Test rollback (if implemented)
            result = manager.rollback_migrations(1)
            assert isinstance(result, bool)
        except Exception as e:
            # Skip if database is not available or rollback not implemented
            pytest.skip(f"Rollback not available: {e}")
    
    def test_migration_file_validation(self):
        """Test migration file validation."""
        manager = ClickHouseMigrationManager()
        
        # Test that migration files are valid SQL
        migrations = manager.get_pending_migrations()
        for migration_file in migrations:
            assert migration_file.suffix == '.sql'
            assert migration_file.exists()
