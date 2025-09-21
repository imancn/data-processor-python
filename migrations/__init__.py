# migrations/__init__.py
"""
Database migration system for the data processing framework.

This package provides database schema migration capabilities,
allowing for versioned database changes and rollbacks.

Example:
    >>> from migrations.migration_manager import ClickHouseMigrationManager
    >>> manager = ClickHouseMigrationManager()
    >>> manager.run_migrations()
"""

from .migration_manager import ClickHouseMigrationManager

# Public API
__all__ = [
    'ClickHouseMigrationManager',
]
"""
ClickHouse Migration System
"""
