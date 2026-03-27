"""
Migration: Add Feature Engineering Columns
Date: 2026-03-09
Purpose: Expand feature space for ML model training

New columns:
- volume_rank_percentile: Cross-sectional rank of symbol's volume z-score vs all symbols that day
- historical_tp100_rate: Symbol's historical TP100 hit rate (rolling 90 days)
- historical_signal_count: Number of historical signals for this symbol
- moneyness: Strike / underlying price for recommended option
- days_to_expiry: Days until expiration for recommended option
- iv_percentile: IV percentile vs contract's own history
- gamma_theta_ratio: Gamma / |Theta| ratio for recommended option
- sector: Stock sector (for sector-level analysis)
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add feature engineering columns to daily_anomaly_snapshot."""
    print("Adding feature engineering columns to daily_anomaly_snapshot...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS volume_rank_percentile DECIMAL(5,2) DEFAULT NULL
            """)
            print("[OK] Added volume_rank_percentile column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS historical_tp100_rate DECIMAL(5,4) DEFAULT NULL
            """)
            print("[OK] Added historical_tp100_rate column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS historical_signal_count INTEGER DEFAULT 0
            """)
            print("[OK] Added historical_signal_count column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS moneyness DECIMAL(6,4) DEFAULT NULL
            """)
            print("[OK] Added moneyness column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS days_to_expiry INTEGER DEFAULT NULL
            """)
            print("[OK] Added days_to_expiry column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS iv_percentile DECIMAL(5,2) DEFAULT NULL
            """)
            print("[OK] Added iv_percentile column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS gamma_theta_ratio DECIMAL(10,4) DEFAULT NULL
            """)
            print("[OK] Added gamma_theta_ratio column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS sector VARCHAR(50) DEFAULT NULL
            """)
            print("[OK] Added sector column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS underlying_price DECIMAL(12,4) DEFAULT NULL
            """)
            print("[OK] Added underlying_price column")
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.volume_rank_percentile IS 
                'Cross-sectional percentile rank of symbol volume z-score vs all symbols that day (0-100)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.historical_tp100_rate IS 
                'Symbol historical TP100 hit rate from past 90 days (0.0000 to 1.0000)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.historical_signal_count IS 
                'Number of high conviction signals for this symbol in past 90 days'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.moneyness IS 
                'Strike price / underlying price for recommended option'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.days_to_expiry IS 
                'Days until expiration for recommended option'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.iv_percentile IS 
                'Implied volatility percentile vs contract history (0-100)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.gamma_theta_ratio IS 
                'Gamma / |Theta| ratio for recommended option (higher = better risk/reward)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.sector IS 
                'Stock sector classification'
            """)
            print("[OK] Added column comments")
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_historical_rate 
                ON daily_anomaly_snapshot (symbol, historical_tp100_rate DESC)
                WHERE historical_tp100_rate IS NOT NULL
            """)
            print("[OK] Created index for historical hit rate")
            
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
    """Remove feature engineering columns."""
    print("Removing feature engineering columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS idx_anomaly_historical_rate")
            print("[OK] Dropped index")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                DROP COLUMN IF EXISTS volume_rank_percentile,
                DROP COLUMN IF EXISTS historical_tp100_rate,
                DROP COLUMN IF EXISTS historical_signal_count,
                DROP COLUMN IF EXISTS moneyness,
                DROP COLUMN IF EXISTS days_to_expiry,
                DROP COLUMN IF EXISTS iv_percentile,
                DROP COLUMN IF EXISTS gamma_theta_ratio,
                DROP COLUMN IF EXISTS sector,
                DROP COLUMN IF EXISTS underlying_price
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


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--down', action='store_true')
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
