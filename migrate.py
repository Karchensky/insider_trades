"""
Database Migration CLI
Manages database schema migrations with proper versioning and tracking.
"""

import logging
import sys
from migrations.migration_manager import MigrationManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main migration CLI."""
    
    if len(sys.argv) < 2:
        print_help()
        return
    
    command = sys.argv[1]
    
    try:
        manager = MigrationManager()
        
        if command == "migrate":
            # Apply all pending migrations
            target = sys.argv[2] if len(sys.argv) > 2 else None
            print("=" * 60)
            print("DATABASE MIGRATION")
            print("=" * 60)
            
            status_info = manager.status()
            print(f"Current state: {status_info['applied_count']} applied, {status_info['pending_count']} pending")
            
            if status_info['pending_count'] == 0:
                print("✓ Database is up to date!")
                return
            
            print(f"\nApplying {status_info['pending_count']} migrations...")
            success = manager.migrate(target)
            
            if success:
                print("✓ All migrations applied successfully!")
            else:
                print("❌ Migration failed!")
                sys.exit(1)
                
        elif command == "rollback":
            # Rollback to target version
            if len(sys.argv) < 3:
                print("Error: rollback requires target version")
                print("Usage: python migrate.py rollback <version>")
                sys.exit(1)
            
            target = sys.argv[2]
            print("=" * 60)
            print("DATABASE ROLLBACK")
            print("=" * 60)
            print(f"Rolling back to version: {target}")
            
            success = manager.rollback(target)
            if success:
                print("✓ Rollback completed successfully!")
            else:
                print("❌ Rollback failed!")
                sys.exit(1)
                
        elif command == "status":
            # Show migration status
            print("=" * 60)
            print("MIGRATION STATUS")
            print("=" * 60)
            
            status_info = manager.status()
            
            print(f"Applied migrations: {status_info['applied_count']}")
            print(f"Pending migrations: {status_info['pending_count']}")
            
            if status_info['last_applied']:
                print(f"Last applied: {status_info['last_applied']}")
            
            if status_info['applied_migrations']:
                print("\nApplied migrations:")
                for migration in status_info['applied_migrations']:
                    print(f"  ✓ {migration['version']}: {migration['name']}")
            
            if status_info['pending_migrations']:
                print("\nPending migrations:")
                for migration in status_info['pending_migrations']:
                    print(f"  • {migration['version']}: {migration['name']}")
            
            if status_info['pending_count'] > 0:
                print(f"\nRun 'python migrate.py migrate' to apply {status_info['pending_count']} pending migrations")
            else:
                print("\n✓ Database is up to date!")
                
        elif command == "create":
            # Create new migration template
            if len(sys.argv) < 3:
                print("Error: create requires migration name")
                print("Usage: python migrate.py create <migration_name>")
                sys.exit(1)
            
            migration_name = "_".join(sys.argv[2:]).lower()
            create_migration_template(migration_name)
            
        else:
            print(f"Unknown command: {command}")
            print_help()
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Migration command failed: {e}")
        sys.exit(1)


def create_migration_template(name: str):
    """Create a new migration template file."""
    from datetime import datetime
    
    # Generate timestamp version
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    version = f"{timestamp}_{name}"
    filename = f"migrations/{version}.py"
    
    template = f'''"""
Migration: {name.replace("_", " ").title()}
Version: {version}
Created: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Description: Add your migration description here.
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the migration."""
    logger.info("Applying migration: {name}")
    
    # Add your migration SQL here
    sql = """
    -- Add your SQL statements here
    -- Example:
    -- CREATE TABLE example_table (
    --     id SERIAL PRIMARY KEY,
    --     name VARCHAR(255) NOT NULL
    -- );
    """
    
    try:
        db.execute_command(sql)
        logger.info("Migration applied successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {{e}}")
        raise


def down():
    """Rollback the migration."""
    logger.info("Rolling back migration: {name}")
    
    # Add your rollback SQL here
    sql = """
    -- Add your rollback SQL statements here
    -- Example:
    -- DROP TABLE IF EXISTS example_table;
    """
    
    try:
        db.execute_command(sql)
        logger.info("Migration rolled back successfully!")
        
    except Exception as e:
        logger.error(f"Rollback failed: {{e}}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    up()
'''
    
    try:
        with open(filename, 'w') as f:
            f.write(template)
        
        print(f"✓ Created migration template: {filename}")
        print(f"  Version: {version}")
        print(f"  Edit the file to add your migration logic")
        
    except Exception as e:
        print(f"❌ Failed to create migration template: {e}")
        sys.exit(1)


def print_help():
    """Print CLI help."""
    print("Database Migration CLI")
    print("")
    print("Usage:")
    print("  python migrate.py migrate [target_version]  - Apply pending migrations")
    print("  python migrate.py rollback <version>        - Rollback to specific version")
    print("  python migrate.py status                    - Show migration status")
    print("  python migrate.py create <name>             - Create new migration template")
    print("")
    print("Examples:")
    print("  python migrate.py migrate                   - Apply all pending migrations")
    print("  python migrate.py migrate 20240101_000001   - Migrate up to specific version")
    print("  python migrate.py rollback 20240101_000001  - Rollback to specific version")
    print("  python migrate.py status                    - Show current migration status")
    print("  python migrate.py create add_user_table     - Create new migration")


if __name__ == "__main__":
    main()
