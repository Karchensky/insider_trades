"""
Migration: Add Actionability Columns to daily_anomaly_snapshot
Date: 2026-01-12
Purpose: Add columns to track earnings proximity and same-day price movement

These columns help identify:
1. Pre-earnings volume spikes (not insider trading, just speculation)
2. Bot-driven trades where stock already moved before notification
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add actionability columns to daily_anomaly_snapshot."""
    print("Adding actionability columns to daily_anomaly_snapshot...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Add earnings_proximity_days - days until next earnings (NULL if none within 30 days)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS earnings_proximity_days INTEGER DEFAULT NULL;
            """)
            print("[OK] Added earnings_proximity_days column")
            
            # Add intraday_price_move_pct - how much the stock moved on the detection day
            # Positive = up, negative = down
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS intraday_price_move_pct DECIMAL(8,4) DEFAULT NULL;
            """)
            print("[OK] Added intraday_price_move_pct column")
            
            # Add is_earnings_related - flag for triggers within 4 days of earnings
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS is_earnings_related BOOLEAN DEFAULT FALSE;
            """)
            print("[OK] Added is_earnings_related column")
            
            # Add is_bot_driven - flag for triggers where stock already moved significantly
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS is_bot_driven BOOLEAN DEFAULT FALSE;
            """)
            print("[OK] Added is_bot_driven column")
            
            # Add is_actionable - computed flag based on earnings and bot detection
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS is_actionable BOOLEAN DEFAULT TRUE;
            """)
            print("[OK] Added is_actionable column")
            
            # Add comments for documentation
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.earnings_proximity_days IS 
                'Days until next earnings announcement (NULL if none within 30 days)';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.intraday_price_move_pct IS 
                'Percentage price movement on detection day (positive=up, negative=down)';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.is_earnings_related IS 
                'True if trigger occurred within 4 days of an earnings announcement';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.is_bot_driven IS 
                'True if stock already moved >5% intraday (likely algorithmic/news-driven)';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.is_actionable IS 
                'True if trigger is potentially actionable (not earnings or bot driven)';
            """)
            print("[OK] Added column comments")
            
            # Create index for actionable anomalies
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_anomaly_snapshot_actionable 
                ON daily_anomaly_snapshot (event_date, total_score DESC) 
                WHERE is_actionable = TRUE AND total_score >= 7.5;
            """)
            print("[OK] Created index for actionable anomalies")
            
            # Create index for earnings-related analysis
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_anomaly_snapshot_earnings 
                ON daily_anomaly_snapshot (symbol, event_date) 
                WHERE is_earnings_related = TRUE;
            """)
            print("[OK] Created index for earnings-related anomalies")
            
            conn.commit()
            print("[OK] Migration completed successfully!")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Remove actionability columns from daily_anomaly_snapshot."""
    print("Rolling back actionability columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop indexes first
            cur.execute("DROP INDEX IF EXISTS idx_daily_anomaly_snapshot_actionable;")
            cur.execute("DROP INDEX IF EXISTS idx_daily_anomaly_snapshot_earnings;")
            
            # Drop columns
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS earnings_proximity_days;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS intraday_price_move_pct;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS is_earnings_related;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS is_bot_driven;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS is_actionable;")
            
            conn.commit()
            print("[OK] Rollback completed successfully!")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Add actionability columns')
    parser.add_argument('--down', action='store_true', help='Rollback the migration')
    
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
