#!/usr/bin/env python3
"""
Migration: Update anomaly scoring schema for volume:open interest ratio scoring

This migration updates the daily_anomaly_snapshot table to replace the old
open interest change scoring with the new volume:open interest ratio scoring.

Changes:
- Rename open_interest_score to volume_oi_ratio_score
- Remove open_interest_change, prior_open_interest columns
- Keep open_interest column (total open interest)
- Add call_open_interest, put_open_interest columns
- Add call_volume_oi_ratio, put_volume_oi_ratio columns (current ratios)
- Add call_volume_oi_z_score, put_volume_oi_z_score columns (z-scores)
- Add call_volume_oi_avg, put_volume_oi_avg columns (historical averages)
"""

import os
import sys
import logging
sys.path.append('.')
from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Update the anomaly scoring schema."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Rename open_interest_score to volume_oi_ratio_score
            logger.info("Renaming open_interest_score to volume_oi_ratio_score...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                RENAME COLUMN open_interest_score TO volume_oi_ratio_score;
            """)
            
            # Remove old open interest columns (keep open_interest for total)
            logger.info("Removing old open interest columns...")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS open_interest_change;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS prior_open_interest;")
            
            # Add new open interest columns
            logger.info("Adding new call/put open interest columns...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS call_open_interest BIGINT DEFAULT 0;
            """)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS put_open_interest BIGINT DEFAULT 0;
            """)
            
            # Add volume:OI ratio columns
            logger.info("Adding volume:OI ratio columns...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS call_volume_oi_ratio DECIMAL(8,4) DEFAULT 0.0;
            """)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS put_volume_oi_ratio DECIMAL(8,4) DEFAULT 0.0;
            """)
            
            # Add z-score columns
            logger.info("Adding z-score columns...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS call_volume_oi_z_score DECIMAL(6,3) DEFAULT 0.0;
            """)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS put_volume_oi_z_score DECIMAL(6,3) DEFAULT 0.0;
            """)
            
            # Add historical average columns
            logger.info("Adding historical average columns...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS call_volume_oi_avg DECIMAL(8,4) DEFAULT 0.0;
            """)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS put_volume_oi_avg DECIMAL(8,4) DEFAULT 0.0;
            """)
            
            # Update column comments
            logger.info("Updating column comments...")
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.volume_oi_ratio_score IS 
                'Volume:Open Interest ratio anomaly score (0-2 points) - z-score vs historical baseline';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.call_open_interest IS 
                'Total call open interest for the symbol on this date';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.put_open_interest IS 
                'Total put open interest for the symbol on this date';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.call_volume_oi_ratio IS 
                'Current call volume:open interest ratio for this date';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.put_volume_oi_ratio IS 
                'Current put volume:open interest ratio for this date';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.call_volume_oi_z_score IS 
                'Z-score of call volume:OI ratio vs historical baseline';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.put_volume_oi_z_score IS 
                'Z-score of put volume:OI ratio vs historical baseline';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.call_volume_oi_avg IS 
                'Historical average call volume:OI ratio from baseline period';
            """)
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.put_volume_oi_avg IS 
                'Historical average put volume:OI ratio from baseline period';
            """)
            
            conn.commit()
            logger.info("✓ Successfully updated anomaly scoring schema")
            
    except Exception as e:
        logger.error(f"✗ Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Rollback the schema changes."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Rename volume_oi_ratio_score back to open_interest_score
            logger.info("Renaming volume_oi_ratio_score back to open_interest_score...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                RENAME COLUMN volume_oi_ratio_score TO open_interest_score;
            """)
            
            # Remove new open interest columns
            logger.info("Removing new call/put open interest columns...")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS call_open_interest;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS put_open_interest;")
            
            # Remove volume:OI ratio columns
            logger.info("Removing volume:OI ratio columns...")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS call_volume_oi_ratio;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS put_volume_oi_ratio;")
            
            # Remove z-score columns
            logger.info("Removing z-score columns...")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS call_volume_oi_z_score;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS put_volume_oi_z_score;")
            
            # Remove historical average columns
            logger.info("Removing historical average columns...")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS call_volume_oi_avg;")
            cur.execute("ALTER TABLE daily_anomaly_snapshot DROP COLUMN IF EXISTS put_volume_oi_avg;")
            
            # Add back old open interest columns
            logger.info("Adding back old open interest columns...")
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS open_interest_change DECIMAL(8,4) DEFAULT 0.0;
            """)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS prior_open_interest BIGINT DEFAULT 0;
            """)
            
            conn.commit()
            logger.info("✓ Successfully rolled back anomaly scoring schema")
            
    except Exception as e:
        logger.error(f"✗ Rollback failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Update anomaly scoring schema')
    parser.add_argument('--rollback', action='store_true', help='Rollback the migration')
    args = parser.parse_args()
    
    if args.rollback:
        down()
    else:
        up()
