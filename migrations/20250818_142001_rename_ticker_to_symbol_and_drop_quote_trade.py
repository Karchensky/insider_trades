"""
Migration: Rename ticker→symbol and drop last quote/trade columns
Version: 20250818_142001

Changes:
- Rename column ticker → symbol
- Update PK and indexes accordingly
- Drop last_quote_* and last_trade_* columns
"""

import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    logger.info("Renaming ticker→symbol and dropping quote/trade columns on temp_stock_snapshot…")
    sqls = [
        # Drop constraints that depend on ticker
        """
        ALTER TABLE temp_stock_snapshot RENAME COLUMN ticker TO symbol;
        """,
        # Recreate PK if necessary
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_temp_snapshot_ticker'
            ) THEN
                DROP INDEX IF EXISTS idx_temp_snapshot_ticker;
            END IF;
        END$$;
        """,
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_symbol ON temp_stock_snapshot (symbol);",
        # Drop quote/trade columns
        """
        ALTER TABLE temp_stock_snapshot
        DROP COLUMN IF EXISTS last_quote_bid,
        DROP COLUMN IF EXISTS last_quote_bid_size,
        DROP COLUMN IF EXISTS last_quote_ask,
        DROP COLUMN IF EXISTS last_quote_ask_size,
        DROP COLUMN IF EXISTS last_quote_timestamp,
        DROP COLUMN IF EXISTS last_trade_price,
        DROP COLUMN IF EXISTS last_trade_size,
        DROP COLUMN IF EXISTS last_trade_timestamp;
        """
    ]
    try:
        for s in sqls:
            db.execute_command(s)
        logger.info("✓ temp_stock_snapshot updated (symbol + slim columns)")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    logger.info("Reverting symbol→ticker and re-adding quote/trade columns…")
    sqls = [
        "ALTER TABLE temp_stock_snapshot RENAME COLUMN symbol TO ticker;",
        "DROP INDEX IF EXISTS idx_temp_snapshot_symbol;",
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_ticker ON temp_stock_snapshot (ticker);",
        """
        ALTER TABLE temp_stock_snapshot
        ADD COLUMN IF NOT EXISTS last_quote_bid DECIMAL(12,4),
        ADD COLUMN IF NOT EXISTS last_quote_bid_size INTEGER,
        ADD COLUMN IF NOT EXISTS last_quote_ask DECIMAL(12,4),
        ADD COLUMN IF NOT EXISTS last_quote_ask_size INTEGER,
        ADD COLUMN IF NOT EXISTS last_quote_timestamp TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS last_trade_price DECIMAL(12,4),
        ADD COLUMN IF NOT EXISTS last_trade_size INTEGER,
        ADD COLUMN IF NOT EXISTS last_trade_timestamp TIMESTAMPTZ;
        """
    ]
    try:
        for s in sqls:
            db.execute_command(s)
        logger.info("✓ temp_stock_snapshot reverted")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    up()
    print("Migration completed successfully!")


