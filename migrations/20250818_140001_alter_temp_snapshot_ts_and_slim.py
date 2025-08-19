"""
Migration: Alter TEMP_STOCK_SNAPSHOT timestamp types and slim columns
Version: 20250818_140001

Changes:
- Convert as_of_timestamp, last_quote_timestamp, last_trade_timestamp from BIGINT epoch-ns to TIMESTAMPTZ
- Drop any extra columns if they exist (min_*, prev_day_*, todays_change*, fair_market_value)
"""

import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    logger.info("Altering temp_stock_snapshot timestamp columns to TIMESTAMPTZ and slimming schema…")

    sql_commands = [
        # Convert BIGINT epoch-ns to TIMESTAMPTZ
        # Use USING to_timestamp(value/1e9)
        """
        ALTER TABLE temp_stock_snapshot
        ALTER COLUMN as_of_timestamp TYPE TIMESTAMPTZ USING to_timestamp(as_of_timestamp::double precision/1e9);
        """,
        """
        ALTER TABLE temp_stock_snapshot
        ALTER COLUMN last_quote_timestamp TYPE TIMESTAMPTZ USING to_timestamp(last_quote_timestamp::double precision/1e9);
        """,
        """
        ALTER TABLE temp_stock_snapshot
        ALTER COLUMN last_trade_timestamp TYPE TIMESTAMPTZ USING to_timestamp(last_trade_timestamp::double precision/1e9);
        """,
        # Recreate index on as_of_timestamp due to type change
        "DROP INDEX IF EXISTS idx_temp_snapshot_asof;",
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_asof ON temp_stock_snapshot (as_of_timestamp);",
        # Optional slimming: drop columns if they exist
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='temp_stock_snapshot' AND column_name='min_open'
            ) THEN
                ALTER TABLE temp_stock_snapshot
                DROP COLUMN IF EXISTS min_open,
                DROP COLUMN IF EXISTS min_high,
                DROP COLUMN IF EXISTS min_low,
                DROP COLUMN IF EXISTS min_close,
                DROP COLUMN IF EXISTS min_volume,
                DROP COLUMN IF EXISTS min_vwap,
                DROP COLUMN IF EXISTS min_timestamp,
                DROP COLUMN IF EXISTS min_transactions,
                DROP COLUMN IF EXISTS prev_day_open,
                DROP COLUMN IF EXISTS prev_day_high,
                DROP COLUMN IF EXISTS prev_day_low,
                DROP COLUMN IF EXISTS prev_day_close,
                DROP COLUMN IF EXISTS prev_day_volume,
                DROP COLUMN IF EXISTS prev_day_vwap,
                DROP COLUMN IF EXISTS todays_change,
                DROP COLUMN IF EXISTS todays_change_perc,
                DROP COLUMN IF EXISTS fair_market_value;
            END IF;
        END$$;
        """
    ]

    try:
        for sql in sql_commands:
            db.execute_command(sql)
        logger.info("✓ temp_stock_snapshot schema updated")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    logger.info("Reverting TIMESTAMPTZ columns back to BIGINT epoch-ns (data loss of precision possible)…")
    sql_commands = [
        # Reverse conversions by casting to epoch seconds * 1e9
        """
        ALTER TABLE temp_stock_snapshot
        ALTER COLUMN as_of_timestamp TYPE BIGINT USING (EXTRACT(EPOCH FROM as_of_timestamp)*1e9)::BIGINT;
        """,
        """
        ALTER TABLE temp_stock_snapshot
        ALTER COLUMN last_quote_timestamp TYPE BIGINT USING (EXTRACT(EPOCH FROM last_quote_timestamp)*1e9)::BIGINT;
        """,
        """
        ALTER TABLE temp_stock_snapshot
        ALTER COLUMN last_trade_timestamp TYPE BIGINT USING (EXTRACT(EPOCH FROM last_trade_timestamp)*1e9)::BIGINT;
        """,
        "DROP INDEX IF EXISTS idx_temp_snapshot_asof;",
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_asof ON temp_stock_snapshot (as_of_timestamp);"
    ]

    try:
        for sql in sql_commands:
            db.execute_command(sql)
        logger.info("✓ temp_stock_snapshot schema reverted")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    up()
    print("Migration completed successfully!")


