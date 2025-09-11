#!/usr/bin/env python3
"""
Migration: Fix cleanup_old_anomalies function

The function is still referencing temp_anomaly table which no longer exists.
This migration fixes the function to properly reference daily_anomaly_snapshot.
"""

import os
import sys
sys.path.append('.')
from database.core.connection import db

def up():
    """Fix the cleanup_old_anomalies function to reference the correct table."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop the old function
            cur.execute("DROP FUNCTION IF EXISTS cleanup_old_anomalies(INTEGER) CASCADE;")
            
            # Create the corrected function
            cur.execute("""
                CREATE OR REPLACE FUNCTION cleanup_old_anomalies(retention_days INTEGER DEFAULT 7)
                RETURNS INTEGER 
                LANGUAGE plpgsql
                SECURITY DEFINER
                SET search_path = public
                AS $$
                DECLARE
                    deleted_count INTEGER := 0;
                    cutoff_date DATE;
                BEGIN
                    cutoff_date := CURRENT_DATE - INTERVAL '1 day' * retention_days;
                    
                    DELETE FROM daily_anomaly_snapshot 
                    WHERE event_date < cutoff_date;
                    
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RETURN deleted_count;
                END;
                $$;
            """)
            
            print("✓ Fixed cleanup_old_anomalies function to reference daily_anomaly_snapshot")
            
    except Exception as e:
        print(f"✗ Migration failed: {e}")
        raise
    finally:
        conn.close()

def down():
    """Rollback the function fix."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop the function
            cur.execute("DROP FUNCTION IF EXISTS cleanup_old_anomalies(INTEGER) CASCADE;")
            
            print("✓ Removed cleanup_old_anomalies function")
            
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
