"""
Migration: Add Individual Greek Component Tracking
Date: 2026-01-31
Purpose: Track which specific greek thresholds were met for high conviction scores

Adds boolean columns to track each of the 4 high conviction factors:
- greeks_theta_met: Theta >= 0.1624 (93rd percentile)
- greeks_gamma_met: Gamma >= 0.4683 (93rd percentile)
- greeks_vega_met: Vega >= 0.1326 (93rd percentile)
- greeks_otm_met: OTM score >= 1.4 (from anomaly detection)

These allow analysis of which specific factors drive successful predictions.
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add greek component tracking columns."""
    print("Adding individual greek component tracking columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Add boolean columns for each greek threshold
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_theta_met BOOLEAN DEFAULT FALSE
            """)
            print("[OK] Added greeks_theta_met column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_gamma_met BOOLEAN DEFAULT FALSE
            """)
            print("[OK] Added greeks_gamma_met column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_vega_met BOOLEAN DEFAULT FALSE
            """)
            print("[OK] Added greeks_vega_met column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS greeks_otm_met BOOLEAN DEFAULT FALSE
            """)
            print("[OK] Added greeks_otm_met column")
            
            # Add comments
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_theta_met IS 
                'True if recommended option theta >= 0.1624 (93rd percentile threshold)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_gamma_met IS 
                'True if recommended option gamma >= 0.4683 (93rd percentile threshold)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_vega_met IS 
                'True if recommended option vega >= 0.1326 (93rd percentile threshold)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.greeks_otm_met IS 
                'True if OTM score >= 1.4 threshold'
            """)
            print("[OK] Added column comments")
            
            conn.commit()
            print("[OK] Migration completed - greek component columns added")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Remove greek component tracking columns."""
    print("Rolling back - removing greek component columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                DROP COLUMN IF EXISTS greeks_theta_met,
                DROP COLUMN IF EXISTS greeks_gamma_met,
                DROP COLUMN IF EXISTS greeks_vega_met,
                DROP COLUMN IF EXISTS greeks_otm_met
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
