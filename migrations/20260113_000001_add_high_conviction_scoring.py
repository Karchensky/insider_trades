"""
Migration: Add High Conviction Scoring System

Adds columns for the new validated high conviction scoring algorithm:
- high_conviction_score: Count of factors above 93rd percentile threshold (0-4)
- is_high_conviction: Boolean flag for score >= 3
- recommended_option: The highest volume option ticker in the triggered direction

The scoring is based on 4 Greeks-based factors at 93rd percentile thresholds:
- Theta >= 0.1624
- Gamma >= 0.4683
- Vega >= 0.1326
- OTM Score >= 1.4

Exit Strategy: +100% take profit or hold to expiration
Expected Performance: ~50% hit rate at score >= 3
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add high conviction scoring columns to daily_anomaly_snapshot."""
    print("Adding high conviction scoring columns to daily_anomaly_snapshot...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Add high_conviction_score column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS high_conviction_score INTEGER DEFAULT 0
            """)
            print("[OK] Added high_conviction_score column")
            
            # Add is_high_conviction flag
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS is_high_conviction BOOLEAN DEFAULT FALSE
            """)
            print("[OK] Added is_high_conviction column")
            
            # Add recommended_option column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS recommended_option VARCHAR(50)
            """)
            print("[OK] Added recommended_option column")
            
            # Add comments explaining the scoring system
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.high_conviction_score IS 
                'Count of Greek factors above 93rd percentile threshold (0-4). Factors: Theta>=0.1624, Gamma>=0.4683, Vega>=0.1326, OTM>=1.4'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.is_high_conviction IS 
                'True if high_conviction_score >= 3. Expected +100% TP hit rate: ~50%'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.recommended_option IS 
                'Highest volume option ticker in the triggered direction (call/put), price $0.05-$5.00'
            """)
            print("[OK] Added column comments")
            
            # Create index for efficient high conviction queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_high_conviction 
                ON daily_anomaly_snapshot (event_date, is_high_conviction) 
                WHERE is_high_conviction = TRUE
            """)
            print("[OK] Created index for high conviction alerts")
            
            conn.commit()
            print("[OK] Migration completed successfully!")
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Migration failed: {e}")
        raise
    finally:
        conn.close()


def down():
    """Remove high conviction scoring columns."""
    print("Removing high conviction scoring columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DROP INDEX IF EXISTS idx_anomaly_high_conviction
            """)
            print("[OK] Dropped index")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                DROP COLUMN IF EXISTS high_conviction_score,
                DROP COLUMN IF EXISTS is_high_conviction,
                DROP COLUMN IF EXISTS recommended_option
            """)
            print("[OK] Dropped columns")
            
            conn.commit()
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Rollback failed: {e}")
        raise
    finally:
        conn.close()
