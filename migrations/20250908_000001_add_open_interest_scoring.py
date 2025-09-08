#!/usr/bin/env python3
"""
Add Open Interest Scoring Migration

Adds columns to daily_anomaly_snapshot table to support open interest change scoring:
- open_interest_score: Score for open interest change (0-2 points)
- open_interest_change: Multiplier of open interest vs prior day
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Add open interest scoring columns to daily_anomaly_snapshot table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            logger.info("Adding open interest scoring columns to daily_anomaly_snapshot...")
            
            # Add open_interest_score column (0-2 points)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS open_interest_score DECIMAL(3,1) DEFAULT 0.0
            """)
            logger.info("Added open_interest_score column")
            
            # Add open_interest_change column (multiplier vs prior day)
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN IF NOT EXISTS open_interest_change DECIMAL(8,4) DEFAULT 0.0000
            """)
            logger.info("Added open_interest_change column")
            
            # Add comment to document the new scoring system
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.open_interest_score IS 
                'Open interest change score (0-2 points): >=5.0x change = 2.0 points'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.open_interest_change IS 
                'Open interest multiplier vs prior day (e.g., 5.0 = 5x increase)'
            """)
            
            conn.commit()
            logger.info("Open interest scoring columns added successfully!")
            
    except Exception as e:
        logger.error(f"Failed to add open interest scoring columns: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Remove open interest scoring columns from daily_anomaly_snapshot table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            logger.info("Removing open interest scoring columns from daily_anomaly_snapshot...")
            
            # Remove open_interest_change column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                DROP COLUMN IF EXISTS open_interest_change
            """)
            logger.info("Removed open_interest_change column")
            
            # Remove open_interest_score column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                DROP COLUMN IF EXISTS open_interest_score
            """)
            logger.info("Removed open_interest_score column")
            
            conn.commit()
            logger.info("Open interest scoring columns removed successfully!")
            
    except Exception as e:
        logger.error(f"Failed to remove open interest scoring columns: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Add Open Interest Scoring Migration')
    parser.add_argument('--down', action='store_true', help='Run rollback')
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
