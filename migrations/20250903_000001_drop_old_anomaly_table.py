"""
Drop Old Anomaly Table Migration
Version: 20250127_000001

Removes the existing improperly structured temp_anomaly table in preparation for 
the new anomaly detection system focused on high-conviction insider trading patterns.
"""

import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Drop the existing temp_anomaly table and related structures."""
    logger.info("Dropping old temp_anomaly table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop the temp_anomaly table with cascade to remove dependencies
            logger.info("Dropping temp_anomaly table...")
            cur.execute("DROP TABLE IF EXISTS temp_anomaly CASCADE;")
            
            # Also remove any references to full_daily_anomaly_snapshot if it still exists
            logger.info("Ensuring full_daily_anomaly_snapshot is removed...")
            cur.execute("DROP TABLE IF EXISTS full_daily_anomaly_snapshot CASCADE;")
            
            conn.commit()
            logger.info("Old anomaly tables dropped successfully!")
            
    except Exception as e:
        logger.error(f"Failed to drop old anomaly tables: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Recreate the old temp_anomaly table structure (for rollback only)."""
    logger.warning("Recreating old temp_anomaly table structure...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Recreate the old temp_anomaly table structure
            cur.execute("""
                CREATE TABLE temp_anomaly (
                    id BIGSERIAL PRIMARY KEY,
                    event_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(10),
                    expiry_date DATE,
                    as_of_timestamp TIMESTAMPTZ,
                    kind VARCHAR(128) NOT NULL,
                    score DECIMAL(18,6) NOT NULL,
                    details JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT uq_temp_anomaly UNIQUE (event_date, symbol, direction, expiry_date, kind)
                );
            """)
            
            # Recreate indexes
            indexes = [
                "CREATE INDEX idx_temp_anomaly_date ON temp_anomaly(event_date);",
                "CREATE INDEX idx_temp_anomaly_symbol ON temp_anomaly(symbol);", 
                "CREATE INDEX idx_temp_anomaly_score ON temp_anomaly(score DESC);",
                "CREATE INDEX idx_temp_anomaly_kind ON temp_anomaly(kind);",
                "CREATE INDEX idx_temp_anomaly_timestamp ON temp_anomaly(as_of_timestamp);",
                "CREATE INDEX idx_temp_anomaly_lookup ON temp_anomaly(event_date, symbol, direction);",
                "CREATE INDEX idx_temp_anomaly_compound ON temp_anomaly(event_date, symbol, kind);"
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
            
            # Recreate trigger
            cur.execute("""
                CREATE TRIGGER update_temp_anomaly_updated_at 
                BEFORE UPDATE ON temp_anomaly 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """)
            
            conn.commit()
            logger.info("Old temp_anomaly table recreated!")
            
    except Exception as e:
        logger.error(f"Failed to recreate old temp_anomaly table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
