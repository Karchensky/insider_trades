"""
Migration Management System
Handles database schema migrations with proper ordering and tracking.
Ensures migrations are applied once and in the correct sequence.
"""

import os
import sys
import logging
import importlib.util
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


class MigrationManager:
    """
    Manages database migrations with version tracking and ordering.
    """
    
    def __init__(self):
        self.migrations_dir = Path(__file__).parent
        self.migration_table = "schema_migrations"
        self._ensure_migration_table()
    
    def _ensure_migration_table(self):
        """Create the schema_migrations table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            version VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255) NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            checksum VARCHAR(64),
            execution_time_ms INTEGER
        );
        
        CREATE INDEX IF NOT EXISTS idx_schema_migrations_version 
        ON schema_migrations (version);
        """
        
        # RLS setup for schema_migrations table
        rls_sql = [
            # Enable RLS on the migration table
            "ALTER TABLE schema_migrations ENABLE ROW LEVEL SECURITY;",
            
            # Allow authenticated users to read migration history
            """
            DO $$ BEGIN
                CREATE POLICY "Allow read for authenticated users" ON schema_migrations
                FOR SELECT
                USING (auth.role() = 'authenticated');
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;
            """,
            
            # Allow service role full access for migrations
            """
            DO $$ BEGIN
                CREATE POLICY "Allow all for service role" ON schema_migrations
                FOR ALL
                USING (auth.jwt() ->> 'role' = 'service_role');
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;
            """,
            
            # Allow migration system to insert/update (for systems without service role)
            """
            DO $$ BEGIN
                CREATE POLICY "Allow migration operations" ON schema_migrations
                FOR ALL
                USING (true);
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;
            """
        ]
        
        try:
            db.execute_command(create_table_sql)
            logger.debug("Migration tracking table ensured")
            
            # Set up RLS
            for sql in rls_sql:
                try:
                    db.execute_command(sql)
                except Exception as e:
                    logger.debug(f"RLS setup for schema_migrations: {e}")
            
            logger.debug("RLS configured for schema_migrations table")
            
        except Exception as e:
            logger.error(f"Failed to create migration tracking table: {e}")
            raise
    
    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions."""
        query = "SELECT version FROM schema_migrations ORDER BY version"
        try:
            results = db.execute_query(query)
            return [row['version'] for row in results]
        except Exception as e:
            logger.error(f"Failed to get applied migrations: {e}")
            return []
    
    def discover_migrations(self) -> List[Dict[str, Any]]:
        """
        Discover all migration files in the migrations directory.
        Returns list of migration info dictionaries ordered by version.
        """
        migrations = []
        
        # Look for migration files with pattern: YYYYMMDD_HHMMSS_name.py
        for file_path in self.migrations_dir.glob("*.py"):
            if file_path.name.startswith("migration_manager") or file_path.name.startswith("__"):
                continue
            
            # Extract version and name from filename
            filename = file_path.stem
            
            # Support both old format (create_daily_stock_snapshot) and new format (YYYYMMDD_HHMMSS_name)
            if "_" in filename and len(filename.split("_")[0]) >= 8:
                # New format: YYYYMMDD_HHMMSS_description
                parts = filename.split("_", 2)
                if len(parts) >= 2:
                    try:
                        # Try to parse as datetime
                        version_part = f"{parts[0]}_{parts[1]}"
                        datetime.strptime(version_part, "%Y%m%d_%H%M%S")
                        version = version_part
                        name = parts[2] if len(parts) > 2 else "migration"
                    except ValueError:
                        # Fallback to filename as version
                        version = filename
                        name = filename
                else:
                    version = filename
                    name = filename
            else:
                # Old format or simple name - use timestamp prefix
                version = f"20240101_000000_{filename}"
                name = filename
            
            migrations.append({
                'version': version,
                'name': name,
                'filename': filename,
                'file_path': file_path
            })
        
        # Sort by version to ensure proper ordering
        migrations.sort(key=lambda x: x['version'])
        return migrations
    
    def load_migration_module(self, file_path: Path) -> Any:
        """Load a migration module from file path."""
        try:
            spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            logger.error(f"Failed to load migration module {file_path}: {e}")
            raise
    
    def apply_migration(self, migration_info: Dict[str, Any]) -> bool:
        """Apply a single migration."""
        version = migration_info['version']
        name = migration_info['name']
        file_path = migration_info['file_path']
        
        logger.info(f"Applying migration {version}: {name}")
        
        try:
            start_time = datetime.now()
            
            # Load and execute migration
            module = self.load_migration_module(file_path)
            
            # Look for standard migration functions
            if hasattr(module, 'up'):
                module.up()
            elif hasattr(module, 'create_daily_stock_snapshot_table'):
                # Support legacy function name
                module.create_daily_stock_snapshot_table()
            elif hasattr(module, 'main'):
                module.main()
            else:
                logger.error(f"Migration {version} has no up(), create_*_table(), or main() function")
                return False
            
            end_time = datetime.now()
            execution_time = int((end_time - start_time).total_seconds() * 1000)
            
            # Record migration as applied
            record_sql = """
            INSERT INTO schema_migrations (version, name, applied_at, execution_time_ms)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (version) DO NOTHING
            """
            
            db.execute_command(record_sql, (version, name, end_time, execution_time))
            
            logger.info(f"Migration {version} applied successfully in {execution_time}ms")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply migration {version}: {e}")
            raise
    
    def rollback_migration(self, migration_info: Dict[str, Any]) -> bool:
        """Rollback a single migration."""
        version = migration_info['version']
        name = migration_info['name']
        file_path = migration_info['file_path']
        
        logger.info(f"Rolling back migration {version}: {name}")
        
        try:
            # Load and execute rollback
            module = self.load_migration_module(file_path)
            
            # Look for rollback functions
            if hasattr(module, 'down'):
                module.down()
            elif hasattr(module, 'rollback_daily_stock_snapshot_table'):
                # Support legacy function name
                module.rollback_daily_stock_snapshot_table()
            else:
                logger.warning(f"Migration {version} has no down() or rollback_*() function")
                return False
            
            # Remove from migration table
            delete_sql = "DELETE FROM schema_migrations WHERE version = %s"
            db.execute_command(delete_sql, (version,))
            
            logger.info(f"Migration {version} rolled back successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rollback migration {version}: {e}")
            raise
    
    def migrate(self, target_version: Optional[str] = None) -> bool:
        """
        Apply all pending migrations up to target version.
        If target_version is None, applies all available migrations.
        """
        logger.info("Starting migration process...")
        
        # Get current state
        applied_migrations = set(self.get_applied_migrations())
        available_migrations = self.discover_migrations()
        
        # Filter migrations to apply
        pending_migrations = []
        for migration in available_migrations:
            if migration['version'] not in applied_migrations:
                if target_version is None or migration['version'] <= target_version:
                    pending_migrations.append(migration)
        
        if not pending_migrations:
            logger.info("No pending migrations to apply")
            return True
        
        logger.info(f"Found {len(pending_migrations)} pending migrations")
        
        # Apply migrations in order
        applied_count = 0
        for migration in pending_migrations:
            try:
                if self.apply_migration(migration):
                    applied_count += 1
                else:
                    logger.error(f"Failed to apply migration {migration['version']}")
                    return False
            except Exception as e:
                logger.error(f"Migration failed: {e}")
                return False
        
        logger.info(f"Successfully applied {applied_count} migrations")
        return True
    
    def rollback(self, target_version: str) -> bool:
        """
        Rollback migrations down to target version.
        """
        logger.info(f"Rolling back to version {target_version}")
        
        # Get current state
        applied_migrations = self.get_applied_migrations()
        available_migrations = self.discover_migrations()
        
        # Create lookup for migration info
        migration_lookup = {m['version']: m for m in available_migrations}
        
        # Find migrations to rollback (in reverse order)
        rollback_migrations = []
        for version in reversed(applied_migrations):
            if version > target_version:
                if version in migration_lookup:
                    rollback_migrations.append(migration_lookup[version])
                else:
                    logger.warning(f"Migration file for version {version} not found")
        
        if not rollback_migrations:
            logger.info("No migrations to rollback")
            return True
        
        logger.info(f"Rolling back {len(rollback_migrations)} migrations")
        
        # Rollback migrations in reverse order
        for migration in rollback_migrations:
            try:
                if not self.rollback_migration(migration):
                    logger.error(f"Failed to rollback migration {migration['version']}")
                    return False
            except Exception as e:
                logger.error(f"Rollback failed: {e}")
                return False
        
        logger.info("Rollback completed successfully")
        return True
    
    def status(self) -> Dict[str, Any]:
        """Get migration status information."""
        applied_migrations = set(self.get_applied_migrations())
        available_migrations = self.discover_migrations()
        
        pending_migrations = []
        applied_migration_info = []
        
        for migration in available_migrations:
            if migration['version'] in applied_migrations:
                applied_migration_info.append(migration)
            else:
                pending_migrations.append(migration)
        
        return {
            'applied_count': len(applied_migration_info),
            'pending_count': len(pending_migrations),
            'applied_migrations': applied_migration_info,
            'pending_migrations': pending_migrations,
            'last_applied': applied_migration_info[-1]['version'] if applied_migration_info else None
        }


# Convenience functions for CLI usage
def migrate(target_version: Optional[str] = None):
    """Apply migrations."""
    manager = MigrationManager()
    return manager.migrate(target_version)


def rollback(target_version: str):
    """Rollback migrations."""
    manager = MigrationManager()
    return manager.rollback(target_version)


def status():
    """Show migration status."""
    manager = MigrationManager()
    return manager.status()


if __name__ == "__main__":
    import argparse
    
    logging.basicConfig(level=logging.INFO)
    
    parser = argparse.ArgumentParser(description='Database Migration Manager')
    parser.add_argument('command', choices=['migrate', 'rollback', 'status'], 
                       help='Migration command to execute')
    parser.add_argument('--target', type=str, 
                       help='Target migration version')
    
    args = parser.parse_args()
    
    manager = MigrationManager()
    
    if args.command == 'migrate':
        success = manager.migrate(args.target)
    elif args.command == 'rollback':
        if not args.target:
            print("Error: rollback requires --target version")
            sys.exit(1)
        success = manager.rollback(args.target)
    elif args.command == 'status':
        status_info = manager.status()
        print(f"Applied migrations: {status_info['applied_count']}")
        print(f"Pending migrations: {status_info['pending_count']}")
        if status_info['last_applied']:
            print(f"Last applied: {status_info['last_applied']}")
        
        if status_info['pending_migrations']:
            print("\nPending migrations:")
            for migration in status_info['pending_migrations']:
                print(f"  - {migration['version']}: {migration['name']}")
        success = True
    
    if not success:
        sys.exit(1)
