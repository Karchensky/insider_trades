"""
Migration: Fix Earnings View Security
Date: 2026-01-31
Purpose: Change upcoming_earnings view from SECURITY DEFINER to SECURITY INVOKER

This fixes the Supabase security warning about views with SECURITY DEFINER property.
SECURITY INVOKER ensures RLS policies of the querying user are applied, not the view creator.
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Recreate view with security_invoker = true."""
    print("Fixing upcoming_earnings view security...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE OR REPLACE VIEW upcoming_earnings
                WITH (security_invoker = true) AS
                SELECT 
                    symbol,
                    earnings_date,
                    time_of_day,
                    date_status,
                    earnings_date - CURRENT_DATE as days_until_earnings
                FROM earnings_calendar
                WHERE earnings_date >= CURRENT_DATE
                  AND earnings_date <= CURRENT_DATE + INTERVAL '14 days'
                ORDER BY earnings_date, symbol;
            """)
            conn.commit()
            print("[OK] View security fixed - now uses SECURITY INVOKER")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Revert to SECURITY DEFINER (default)."""
    print("Reverting view security to SECURITY DEFINER...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE OR REPLACE VIEW upcoming_earnings AS
                SELECT 
                    symbol,
                    earnings_date,
                    time_of_day,
                    date_status,
                    earnings_date - CURRENT_DATE as days_until_earnings
                FROM earnings_calendar
                WHERE earnings_date >= CURRENT_DATE
                  AND earnings_date <= CURRENT_DATE + INTERVAL '14 days'
                ORDER BY earnings_date, symbol;
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
