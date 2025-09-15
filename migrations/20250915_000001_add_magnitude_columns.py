"""
Migration: Add magnitude columns to daily_anomaly_snapshot table
Date: 2025-09-15
Purpose: Add call_magnitude, put_magnitude, and total_magnitude columns for filtering anomalies by financial impact
"""

import os
import sys
from datetime import datetime

# Add the parent directory to the path so we can import from database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db

def up():
    """Add magnitude columns to daily_anomaly_snapshot table."""
    print("Adding magnitude columns to daily_anomaly_snapshot table...")
    
    # Add call_magnitude column
    db.execute_command("""
        ALTER TABLE daily_anomaly_snapshot 
        ADD COLUMN call_magnitude DECIMAL(15,2) DEFAULT 0.00
    """)
    print("✓ Added call_magnitude column")
    
    # Add put_magnitude column  
    db.execute_command("""
        ALTER TABLE daily_anomaly_snapshot 
        ADD COLUMN put_magnitude DECIMAL(15,2) DEFAULT 0.00
    """)
    print("✓ Added put_magnitude column")
    
    # Add total_magnitude column
    db.execute_command("""
        ALTER TABLE daily_anomaly_snapshot 
        ADD COLUMN total_magnitude DECIMAL(15,2) DEFAULT 0.00
    """)
    print("✓ Added total_magnitude column")
    
    # Add comments for documentation
    db.execute_command("""
        COMMENT ON COLUMN daily_anomaly_snapshot.call_magnitude IS 
        'Total dollar value of call option volume (volume * price * shares_per_contract)'
    """)
    
    db.execute_command("""
        COMMENT ON COLUMN daily_anomaly_snapshot.put_magnitude IS 
        'Total dollar value of put option volume (volume * price * shares_per_contract)'
    """)
    
    db.execute_command("""
        COMMENT ON COLUMN daily_anomaly_snapshot.total_magnitude IS 
        'Total dollar value of all option volume (call_magnitude + put_magnitude)'
    """)
    
    print("✓ Added column comments")
    
    # Create index on total_magnitude for efficient filtering
    db.execute_command("""
        CREATE INDEX idx_daily_anomaly_snapshot_total_magnitude 
        ON daily_anomaly_snapshot(total_magnitude)
    """)
    print("✓ Created index on total_magnitude")
    
    print("Migration completed successfully!")

def down():
    """Remove magnitude columns from daily_anomaly_snapshot table."""
    print("Removing magnitude columns from daily_anomaly_snapshot table...")
    
    # Drop index first
    db.execute_command("""
        DROP INDEX IF EXISTS idx_daily_anomaly_snapshot_total_magnitude
    """)
    print("✓ Dropped total_magnitude index")
    
    # Remove columns
    db.execute_command("""
        ALTER TABLE daily_anomaly_snapshot 
        DROP COLUMN IF EXISTS total_magnitude
    """)
    print("✓ Removed total_magnitude column")
    
    db.execute_command("""
        ALTER TABLE daily_anomaly_snapshot 
        DROP COLUMN IF EXISTS put_magnitude
    """)
    print("✓ Removed put_magnitude column")
    
    db.execute_command("""
        ALTER TABLE daily_anomaly_snapshot 
        DROP COLUMN IF EXISTS call_magnitude
    """)
    print("✓ Removed call_magnitude column")
    
    print("Rollback completed successfully!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Add magnitude columns to daily_anomaly_snapshot')
    parser.add_argument('--down', action='store_true', help='Rollback the migration')
    
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
