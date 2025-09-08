#!/usr/bin/env python3
"""
Add Open Interest Volume Columns Migration

Adds columns to daily_anomaly_snapshot table to show the actual volumes used in open interest calculations:
- open_interest: Current day's total open interest
- prior_open_interest: Most recent trading day's total open interest
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Add open interest volume columns to daily_anomaly_snapshot table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            logger.info("Adding open interest volume columns to daily_anomaly_snapshot...")
            
            # Add open_interest column (current day's total)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS open_interest INTEGER DEFAULT 0
            """)
            logger.info("Added open_interest column")
            
            # Add prior_open_interest column (most recent trading day's total)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS prior_open_interest INTEGER DEFAULT 0
            """)
            logger.info("Added prior_open_interest column")
            
            # Add comments to document the columns
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.open_interest IS 
                'Current day total open interest across all contracts for this symbol'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.prior_open_interest IS 
                'Most recent trading day total open interest (handles weekends/holidays)'
            """)
            
            conn.commit()
            logger.info("Open interest volume columns added successfully!")
            
    except Exception as e:
        logger.error(f"Failed to add open interest volume columns: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Remove open interest volume columns from daily_anomaly_snapshot table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            logger.info("Removing open interest volume columns from daily_anomaly_snapshot...")
            
            # Remove prior_open_interest column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                DROP COLUMN IF EXISTS prior_open_interest
            """)
            logger.info("Removed prior_open_interest column")
            
            # Remove open_interest column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                DROP COLUMN IF EXISTS open_interest
            """)
            logger.info("Removed open_interest column")
            
            conn.commit()
            logger.info("Open interest volume columns removed successfully!")
            
    except Exception as e:
        logger.error(f"Failed to remove open interest volume columns: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Add Open Interest Volume Columns Migration')
    parser.add_argument('--down', action='store_true', help='Run rollback')
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
