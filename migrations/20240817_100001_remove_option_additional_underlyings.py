"""
Migration: Remove additional_underlyings JSONB field from option_contracts
Version: 20240817_100001
Created: 2024-08-17 10:00:01

Removes the additional_underlyings JSONB column from option_contracts table
as it's not needed for the current use case.
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the migration - remove additional_underlyings column."""
    logger.info("Removing additional_underlyings column from option_contracts table...")
    
    try:
        # Drop the column
        drop_column_sql = """
        ALTER TABLE option_contracts 
        DROP COLUMN IF EXISTS additional_underlyings;
        """
        
        db.execute_command(drop_column_sql)
        logger.info("✓ additional_underlyings column removed")
        
        logger.info("option_contracts schema update completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    """Rollback the migration - add back additional_underlyings column."""
    logger.info("Adding back additional_underlyings column to option_contracts table...")
    
    try:
        # Add the column back
        add_column_sql = """
        ALTER TABLE option_contracts 
        ADD COLUMN IF NOT EXISTS additional_underlyings JSONB;
        """
        
        db.execute_command(add_column_sql)
        logger.info("✓ additional_underlyings column restored")
        
        logger.info("option_contracts schema rollback completed successfully!")
        
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Running option_contracts schema update migration...")
    up()
    print("Migration completed successfully!")
