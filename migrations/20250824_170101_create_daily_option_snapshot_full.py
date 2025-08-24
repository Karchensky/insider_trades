import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Create daily_option_snapshot_full table to persist last intraday option snapshot per contract per day."""
    logger.info("Creating daily_option_snapshot_full table…")
    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_option_snapshot_full (
                snapshot_date DATE NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                contract_ticker VARCHAR(50) NOT NULL,
                as_of_timestamp TIMESTAMPTZ NOT NULL,
                break_even_price DECIMAL(18,6),
                strike_price DECIMAL(18,6),
                implied_volatility DECIMAL(18,8),
                open_interest BIGINT,
                greeks_delta DECIMAL(18,8),
                greeks_gamma DECIMAL(18,8),
                greeks_theta DECIMAL(18,8),
                greeks_vega DECIMAL(18,8),
                contract_type VARCHAR(10),
                exercise_style VARCHAR(20),
                expiration_date DATE,
                shares_per_contract INTEGER,
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
                underlying_ticker VARCHAR(10),
                underlying_price DECIMAL(18,6),
                underlying_change_to_break_even DECIMAL(18,6),
                underlying_last_updated TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_date, symbol, contract_ticker)
            );
            """
        )
        # Helpful indexes
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dosf_date ON daily_option_snapshot_full (snapshot_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dosf_contract ON daily_option_snapshot_full (contract_ticker);")
        conn.commit()
    logger.info("daily_option_snapshot_full created.")


def down():
    logger.info("Dropping daily_option_snapshot_full…")
    db.execute_command("DROP TABLE IF EXISTS daily_option_snapshot_full;")


