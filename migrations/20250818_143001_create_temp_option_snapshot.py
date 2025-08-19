"""
Migration: Create TEMP_OPTION_SNAPSHOT table
Version: 20250818_143001

Composite Key: (as_of_timestamp, symbol, contract_ticker)
Source: Unified Snapshot /v3/snapshot (type=options)
"""

import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    logger.info("Creating TEMP_OPTION_SNAPSHOT table…")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS temp_option_snapshot (
        as_of_timestamp TIMESTAMPTZ NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        contract_ticker VARCHAR(50) NOT NULL,

        -- Key option metrics
        break_even_price DECIMAL(18,6),
        strike_price DECIMAL(18,6),
        implied_volatility DECIMAL(18,8),
        open_interest BIGINT,

        -- Greeks
        greeks_delta DECIMAL(18,8),
        greeks_gamma DECIMAL(18,8),
        greeks_theta DECIMAL(18,8),
        greeks_vega DECIMAL(18,8),

        -- Contract details
        contract_type VARCHAR(10),
        exercise_style VARCHAR(20),
        expiration_date DATE,
        shares_per_contract INTEGER,

        -- Session metrics
        session_open DECIMAL(18,6),
        session_high DECIMAL(18,6),
        session_low DECIMAL(18,6),
        session_close DECIMAL(18,6),
        session_volume BIGINT,
        session_change DECIMAL(18,6),
        session_change_percent DECIMAL(18,6),
        session_early_trading_change DECIMAL(18,6),
        session_early_trading_change_percent DECIMAL(18,6),
        session_regular_trading_change DECIMAL(18,6),
        session_regular_trading_change_percent DECIMAL(18,6),
        session_late_trading_change DECIMAL(18,6),
        session_late_trading_change_percent DECIMAL(18,6),
        session_previous_close DECIMAL(18,6),

        -- Underlying asset snapshot
        underlying_ticker VARCHAR(10),
        underlying_price DECIMAL(18,6),
        underlying_change_to_break_even DECIMAL(18,6),
        underlying_last_updated TIMESTAMPTZ,

        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (as_of_timestamp, symbol, contract_ticker)
    );
    """

    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_temp_option_symbol ON temp_option_snapshot (symbol);",
        "CREATE INDEX IF NOT EXISTS idx_temp_option_contract ON temp_option_snapshot (contract_ticker);",
        "CREATE INDEX IF NOT EXISTS idx_temp_option_asof ON temp_option_snapshot (as_of_timestamp);"
    ]

    create_trigger_sql = """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';

    DROP TRIGGER IF EXISTS update_temp_option_snapshot_updated_at ON temp_option_snapshot;

    CREATE TRIGGER update_temp_option_snapshot_updated_at
        BEFORE UPDATE ON temp_option_snapshot
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """

    rls_sql = [
        "ALTER TABLE temp_option_snapshot ENABLE ROW LEVEL SECURITY;",
        """
        CREATE POLICY IF NOT EXISTS "Allow read access for authenticated users" ON temp_option_snapshot
        FOR SELECT
        USING (auth.role() = 'authenticated');
        """,
        """
        CREATE POLICY IF NOT EXISTS "Allow insert for authenticated users" ON temp_option_snapshot
        FOR INSERT
        WITH CHECK (auth.role() = 'authenticated');
        """,
        """
        CREATE POLICY IF NOT EXISTS "Allow update for authenticated users" ON temp_option_snapshot
        FOR UPDATE
        USING (auth.role() = 'authenticated')
        WITH CHECK (auth.role() = 'authenticated');
        """,
        """
        CREATE POLICY IF NOT EXISTS "Allow all for service role" ON temp_option_snapshot
        FOR ALL
        USING (auth.jwt() ->> 'role' = 'service_role');
        """
    ]

    try:
        db.execute_command(create_table_sql)
        for s in create_indexes_sql:
            db.execute_command(s)
        db.execute_command(create_trigger_sql)
        for s in rls_sql:
            try:
                db.execute_command(s)
            except Exception as e:
                logger.warning(f"RLS setup warning: {e}")
        logger.info("✓ temp_option_snapshot created")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    logger.info("Dropping TEMP_OPTION_SNAPSHOT…")
    try:
        db.execute_command("DROP TRIGGER IF EXISTS update_temp_option_snapshot_updated_at ON temp_option_snapshot;")
        db.execute_command("DROP TABLE IF EXISTS temp_option_snapshot CASCADE;")
        logger.info("✓ temp_option_snapshot dropped")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    up()
    print("Migration completed successfully!")


