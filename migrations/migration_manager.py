# migrations/migration_manager.py
"""
ClickHouse Migration Manager
Handles database schema migrations and DDL operations.
"""
import os
import sys
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.config import config
from core.logging import log_with_timestamp
import clickhouse_connect


class ClickHouseMigrationManager:
    """Manages ClickHouse database migrations."""
    
    def __init__(self):
        self.config = config.get_clickhouse_config()
        self.client = None
        self.migrations_dir = Path(__file__).parent / "sql"
        self.migrations_dir.mkdir(exist_ok=True)
        
    def connect(self):
        """Connect to ClickHouse database."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.config['host'],
                port=self.config['port'],
                username=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
            log_with_timestamp("Connected to ClickHouse successfully", "Migration Manager")
            return True
        except Exception as e:
            log_with_timestamp(f"Failed to connect to ClickHouse: {e}", "Migration Manager", "error")
            return False
    
    def create_migrations_table(self):
        """Create the migrations tracking table."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS migrations (
            id UInt32,
            name String,
            executed_at DateTime DEFAULT now(),
            checksum String
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        
        try:
            self.client.command(create_table_sql)
            log_with_timestamp("Migrations table created/verified", "Migration Manager")
            return True
        except Exception as e:
            log_with_timestamp(f"Failed to create migrations table: {e}", "Migration Manager", "error")
            return False
    
    def get_executed_migrations(self) -> List[str]:
        """Get list of executed migrations."""
        try:
            qr = self.client.query("SELECT name FROM migrations ORDER BY id")
            return [row[0] for row in qr.result_rows]
        except Exception as e:
            log_with_timestamp(f"Failed to get executed migrations: {e}", "Migration Manager", "error")
            return []
    
    def get_pending_migrations(self) -> List[Path]:
        """Get list of pending migration files."""
        if not self.migrations_dir.exists():
            return []
        
        migration_files = sorted(self.migrations_dir.glob("*.sql"))
        executed = self.get_executed_migrations()
        
        pending = []
        for migration_file in migration_files:
            if migration_file.stem not in executed:
                pending.append(migration_file)
        
        return pending
    
    def execute_migration(self, migration_file: Path) -> bool:
        """Execute a single migration file."""
        try:
            with open(migration_file, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for statement in statements:
                if statement:
                    self.client.command(statement)
            
            # Record migration as executed
            migration_name = migration_file.stem
            checksum = str(hash(sql_content))
            
            self.client.command(
                f"INSERT INTO migrations (id, name, checksum) VALUES ({len(self.get_executed_migrations()) + 1}, '{migration_name}', '{checksum}')"
            )
            
            log_with_timestamp(f"Executed migration: {migration_name}", "Migration Manager")
            return True
            
        except Exception as e:
            log_with_timestamp(f"Failed to execute migration {migration_file.name}: {e}", "Migration Manager", "error")
            return False
    
    def run_migrations(self) -> bool:
        """Run all pending migrations."""
        if not self.connect():
            return False
        
        if not self.create_migrations_table():
            return False
        
        pending_migrations = self.get_pending_migrations()
        
        if not pending_migrations:
            log_with_timestamp("No pending migrations", "Migration Manager")
            return True
        
        log_with_timestamp(f"Found {len(pending_migrations)} pending migrations", "Migration Manager")
        
        for migration_file in pending_migrations:
            if not self.execute_migration(migration_file):
                log_with_timestamp(f"Migration failed: {migration_file.name}", "Migration Manager", "error")
                return False
        
        log_with_timestamp("All migrations completed successfully", "Migration Manager")
        return True
    
    def show_status(self):
        """Show migration status."""
        if not self.connect():
            return
        
        executed = self.get_executed_migrations()
        pending = self.get_pending_migrations()
        
        print(f"\n=== Migration Status ===")
        print(f"Executed migrations: {len(executed)}")
        for migration in executed:
            print(f"  ✓ {migration}")
        
        print(f"\nPending migrations: {len(pending)}")
        for migration in pending:
            print(f"  ⏳ {migration.name}")
        
        print(f"Total: {len(executed) + len(pending)} migrations")


def main():
    """Main function for running migrations."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ClickHouse Migration Manager')
    parser.add_argument('command', choices=['run', 'status'], help='Command to execute')
    
    args = parser.parse_args()
    
    manager = ClickHouseMigrationManager()
    
    if args.command == 'run':
        success = manager.run_migrations()
        sys.exit(0 if success else 1)
    elif args.command == 'status':
        manager.show_status()


if __name__ == "__main__":
    main()
