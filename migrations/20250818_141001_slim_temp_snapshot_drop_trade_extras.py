"""
Migration: Slim TEMP_STOCK_SNAPSHOT trade columns
Version: 20250818_141001

Drops optional trade columns not requested by user: last_trade_exchange, last_trade_id, last_trade_conditions
"""

import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    logger.info("Dropping extra trade columns from temp_stock_snapshot…")
    sql = """
    ALTER TABLE temp_stock_snapshot
    DROP COLUMN IF EXISTS last_trade_exchange,
    DROP COLUMN IF EXISTS last_trade_id,
    DROP COLUMN IF EXISTS last_trade_conditions;
    """
    try:
        db.execute_command(sql)
        logger.info("✓ Columns dropped")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    logger.info("Re-adding trade columns to temp_stock_snapshot…")
    sql = """
    ALTER TABLE temp_stock_snapshot
    ADD COLUMN IF NOT EXISTS last_trade_exchange INTEGER,
    ADD COLUMN IF NOT EXISTS last_trade_id VARCHAR(64),
    ADD COLUMN IF NOT EXISTS last_trade_conditions TEXT;
    """
    try:
        db.execute_command(sql)
        logger.info("✓ Columns re-added")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    up()
    print("Migration completed successfully!")


