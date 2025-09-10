#!/usr/bin/env python3
"""
Migration: Add MAX_TOTAL_SCORE column to daily_anomaly_snapshot table

This column will track the highest total score achieved for each symbol on each day,
allowing us to maintain the peak score even if subsequent runs have lower scores.
"""

import os
import sys
sys.path.append('.')
from database.core.connection import db

def up():
    """Add MAX_TOTAL_SCORE column to daily_anomaly_snapshot table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Add MAX_TOTAL_SCORE column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                ADD COLUMN max_total_score DECIMAL(4,1) DEFAULT 0.0
            """)
            
            # Add comment to document the column
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.max_total_score IS 
                'Highest total score achieved for this symbol on this day across all runs'
            """)
            
            # Initialize existing records with their current total_score as max_total_score
            cur.execute("""
                UPDATE daily_anomaly_snapshot 
                SET max_total_score = total_score 
                WHERE max_total_score = 0.0
            """)
            
            print("✓ Added MAX_TOTAL_SCORE column to daily_anomaly_snapshot table")
            print("✓ Initialized existing records with current total_score as max_total_score")
            
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        raise
    finally:
        conn.close()

def down():
    """Remove MAX_TOTAL_SCORE column from daily_anomaly_snapshot table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Remove MAX_TOTAL_SCORE column
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot 
                DROP COLUMN IF EXISTS max_total_score
            """)
            
            print("✓ Removed MAX_TOTAL_SCORE column from daily_anomaly_snapshot table")
            
    except Exception as e:
        print(f"✗ Rollback failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "down":
        down()
    else:
        up()
