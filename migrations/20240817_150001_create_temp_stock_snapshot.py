"""
Migration: Create TEMP_STOCK_SNAPSHOT table
Version: 20240817_150001
Created: 2024-08-17 15:00:01

Creates the TEMP_STOCK_SNAPSHOT table for storing intraday full-market snapshots
from Polygon Full Market Snapshot endpoint.

Primary Key: (as_of_timestamp, ticker)
API Source: /v2/snapshot/locale/us/markets/stocks/tickers (exclude OTC)
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the migration - create temp stock snapshot table and related objects."""
    logger.info("Creating TEMP_STOCK_SNAPSHOT table...")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS temp_stock_snapshot (
        as_of_timestamp BIGINT NOT NULL,
        ticker VARCHAR(10) NOT NULL,

        -- Most recent daily bar
        day_open DECIMAL(12, 4),
        day_high DECIMAL(12, 4),
        day_low DECIMAL(12, 4),
        day_close DECIMAL(12, 4),
        day_volume BIGINT,
        day_vwap DECIMAL(12, 4),

        -- Most recent quote
        last_quote_bid DECIMAL(12, 4),
        last_quote_bid_size INTEGER,
        last_quote_ask DECIMAL(12, 4),
        last_quote_ask_size INTEGER,
        last_quote_timestamp BIGINT,

        -- Most recent trade
        last_trade_price DECIMAL(12, 4),
        last_trade_size INTEGER,
        last_trade_timestamp BIGINT,
        last_trade_exchange INTEGER,
        last_trade_id VARCHAR(64),
        last_trade_conditions TEXT,

        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (as_of_timestamp, ticker)
    );
    """

    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_ticker ON temp_stock_snapshot (ticker);",
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_asof ON temp_stock_snapshot (as_of_timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_temp_snapshot_created ON temp_stock_snapshot (created_at);"
    ]

    create_trigger_sql = """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    DROP TRIGGER IF EXISTS update_temp_stock_snapshot_updated_at ON temp_stock_snapshot;

    CREATE TRIGGER update_temp_stock_snapshot_updated_at
        BEFORE UPDATE ON temp_stock_snapshot
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """

    # RLS policies
    rls_sql = [
        "ALTER TABLE temp_stock_snapshot ENABLE ROW LEVEL SECURITY;",
        """
        CREATE POLICY "Allow read access for authenticated users" ON temp_stock_snapshot
        FOR SELECT
        USING (auth.role() = 'authenticated');
        """,
        """
        CREATE POLICY "Allow insert for authenticated users" ON temp_stock_snapshot
        FOR INSERT
        WITH CHECK (auth.role() = 'authenticated');
        """,
        """
        CREATE POLICY "Allow update for authenticated users" ON temp_stock_snapshot
        FOR UPDATE
        USING (auth.role() = 'authenticated')
        WITH CHECK (auth.role() = 'authenticated');
        """,
        """
        CREATE POLICY "Allow all for service role" ON temp_stock_snapshot
        FOR ALL
        USING (auth.jwt() ->> 'role' = 'service_role');
        """
    ]

    try:
        db.execute_command(create_table_sql)
        logger.info("✓ Table created")

        for sql in create_indexes_sql:
            db.execute_command(sql)
        logger.info("✓ Indexes created")

        db.execute_command(create_trigger_sql)
        logger.info("✓ Update trigger created")

        for sql in rls_sql:
            try:
                db.execute_command(sql)
            except Exception as e:
                logger.warning(f"RLS setup warning: {e}")
        logger.info("✓ Row Level Security configured")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    """Rollback the migration - drop temp stock snapshot table."""
    logger.info("Rolling back TEMP_STOCK_SNAPSHOT table...")

    rollback_sql = [
        "DROP TRIGGER IF EXISTS update_temp_stock_snapshot_updated_at ON temp_stock_snapshot;",
        "DROP TABLE IF EXISTS temp_stock_snapshot CASCADE;"
    ]

    try:
        for sql in rollback_sql:
            db.execute_command(sql)

        logger.info("TEMP_STOCK_SNAPSHOT rollback completed successfully!")

    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Running TEMP_STOCK_SNAPSHOT migration...")
    up()
    print("Migration completed successfully!")


