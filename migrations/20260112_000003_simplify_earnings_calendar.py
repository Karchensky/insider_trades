"""
Migration: Simplify Earnings Calendar Table
Date: 2026-01-12
Purpose: Remove unused columns from earnings_calendar - keep only what we need

We only need:
- symbol: Stock ticker
- earnings_date: Date of earnings announcement
- time_of_day: Before/after market (optional)
- date_status: confirmed/projected (optional)
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Simplify earnings_calendar table by removing unused columns."""
    print("Simplifying earnings_calendar table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop dependent view first
            cur.execute("DROP VIEW IF EXISTS upcoming_earnings CASCADE;")
            print("[OK] Dropped upcoming_earnings view")
            
            # Drop unused columns
            columns_to_drop = [
                'fiscal_year',
                'fiscal_period', 
                'importance',
                'estimated_eps',
                'actual_eps',
                'eps_surprise_percent',
                'estimated_revenue',
                'actual_revenue',
                'revenue_surprise_percent',
                'company_name',
                'benzinga_id',
                'last_updated',
            ]
            
            for col in columns_to_drop:
                cur.execute(f"ALTER TABLE earnings_calendar DROP COLUMN IF EXISTS {col};")
                print(f"[OK] Dropped column: {col}")
            
            # Recreate simplified view
            # SECURITY INVOKER ensures RLS policies apply to querying user, not view creator
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
            print("[OK] Recreated upcoming_earnings view (simplified)")
            
            conn.commit()
            print("[OK] Migration completed - earnings_calendar simplified")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Re-add columns (with NULL values)."""
    print("Rolling back - re-adding columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS fiscal_year INTEGER;")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS fiscal_period VARCHAR(10);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS importance INTEGER;")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS estimated_eps DECIMAL(12,4);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS actual_eps DECIMAL(12,4);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS eps_surprise_percent DECIMAL(8,4);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS estimated_revenue DECIMAL(18,2);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS actual_revenue DECIMAL(18,2);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS revenue_surprise_percent DECIMAL(8,4);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS company_name VARCHAR(255);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS benzinga_id VARCHAR(50);")
            cur.execute("ALTER TABLE earnings_calendar ADD COLUMN IF NOT EXISTS last_updated TIMESTAMPTZ;")
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
