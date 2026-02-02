"""
Migration: Add Greek Values and Percentiles
Date: 2026-01-31
Purpose: Store actual greek values and their percentiles for recommended options

Instead of just boolean flags (met/not met), store:
- The actual greek value from the recommended option
- The percentile ranking of that value (0-100)

This allows analysis of how close to thresholds each trigger was.
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add greek value and percentile columns."""
    print("Adding greek value and percentile tracking columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Add value columns (DECIMAL to match option greeks)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_theta_value DECIMAL(18,8) DEFAULT NULL
            """)
            print("[OK] Added greeks_theta_value column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_gamma_value DECIMAL(18,8) DEFAULT NULL
            """)
            print("[OK] Added greeks_gamma_value column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_vega_value DECIMAL(18,8) DEFAULT NULL
            """)
            print("[OK] Added greeks_vega_value column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_otm_value DECIMAL(8,4) DEFAULT NULL
            """)
            print("[OK] Added greeks_otm_value column")
            
            # Add percentile columns (0-100)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_theta_percentile DECIMAL(5,2) DEFAULT NULL
            """)
            print("[OK] Added greeks_theta_percentile column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_gamma_percentile DECIMAL(5,2) DEFAULT NULL
            """)
            print("[OK] Added greeks_gamma_percentile column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_vega_percentile DECIMAL(5,2) DEFAULT NULL
            """)
            print("[OK] Added greeks_vega_percentile column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_otm_percentile DECIMAL(5,2) DEFAULT NULL
            """)
            print("[OK] Added greeks_otm_percentile column")
            
            # Add comments
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_theta_value IS 
                'Actual theta value of recommended option (absolute value)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_gamma_value IS 
                'Actual gamma value of recommended option'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_vega_value IS 
                'Actual vega value of recommended option'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_otm_value IS 
                'Actual OTM score from anomaly detection'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_theta_percentile IS 
                'Percentile rank of theta (0-100, threshold at 93.0)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_gamma_percentile IS 
                'Percentile rank of gamma (0-100, threshold at 93.0)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_vega_percentile IS 
                'Percentile rank of vega (0-100, threshold at 93.0)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_otm_percentile IS 
                'Percentile rank of OTM score (0-100, threshold at 93.0)'
            """)
            print("[OK] Added column comments")
            
            conn.commit()
            print("[OK] Migration completed - greek values and percentiles added")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Remove greek value and percentile columns."""
    print("Rolling back - removing greek value and percentile columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                DROP COLUMN IF EXISTS greeks_theta_value,
                DROP COLUMN IF EXISTS greeks_gamma_value,
                DROP COLUMN IF EXISTS greeks_vega_value,
                DROP COLUMN IF EXISTS greeks_otm_value,
                DROP COLUMN IF EXISTS greeks_theta_percentile,
                DROP COLUMN IF EXISTS greeks_gamma_percentile,
                DROP COLUMN IF EXISTS greeks_vega_percentile,
                DROP COLUMN IF EXISTS greeks_otm_percentile
            """)
            conn.commit()
            print("[OK] Rollback completed")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        conn.rollback()
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
