"""
Fix Option Contracts Field Sizes Migration
Version: 20250904_000002

Increases field sizes for option_contracts table based on actual Polygon.io API data.
"""

import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Increase field sizes for option_contracts table."""
    logger.info("Increasing field sizes for option_contracts table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Increase field sizes to accommodate actual Polygon.io data
            alterations = [
                "ALTER TABLE option_contracts ALTER COLUMN contract_type TYPE VARCHAR(20);",
                "ALTER TABLE option_contracts ALTER COLUMN exercise_style TYPE VARCHAR(50);",
                "ALTER TABLE option_contracts ALTER COLUMN primary_exchange TYPE VARCHAR(20);",
                "ALTER TABLE option_contracts ALTER COLUMN cfi TYPE VARCHAR(20);"
            ]
            
            for alter_sql in alterations:
                cur.execute(alter_sql)
                logger.info(f"Applied: {alter_sql}")
            
            conn.commit()
            logger.info("Field sizes increased successfully!")
            
    except Exception as e:
        logger.error(f"Failed to increase field sizes: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Revert field sizes for option_contracts table."""
    logger.warning("Reverting field sizes for option_contracts table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Revert field sizes (note: this might fail if data is too long)
            alterations = [
                "ALTER TABLE option_contracts ALTER COLUMN contract_type TYPE VARCHAR(10);",
                "ALTER TABLE option_contracts ALTER COLUMN exercise_style TYPE VARCHAR(20);",
                "ALTER TABLE option_contracts ALTER COLUMN primary_exchange TYPE VARCHAR(10);",
                "ALTER TABLE option_contracts ALTER COLUMN cfi TYPE VARCHAR(10);"
            ]
            
            for alter_sql in alterations:
                try:
                    cur.execute(alter_sql)
                    logger.info(f"Applied: {alter_sql}")
                except Exception as e:
                    logger.warning(f"Could not revert field size: {e}")
            
            conn.commit()
            logger.info("Field sizes reverted successfully!")
            
    except Exception as e:
        logger.error(f"Failed to revert field sizes: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
