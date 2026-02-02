"""
Migration: Create Earnings Calendar Table
Date: 2026-01-12
Purpose: Store earnings announcement dates to filter false positive anomalies

This table stores upcoming and historical earnings dates fetched from the Benzinga API
via Massive. Used to identify and flag anomalies that occur 1-4 days before earnings,
which are likely to be pre-earnings positioning rather than insider trading.
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Create the earnings_calendar table."""
    print("Creating earnings_calendar table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Create the earnings_calendar table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS earnings_calendar (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    earnings_date DATE NOT NULL,
                    time_of_day VARCHAR(20),  -- 'before_market', 'after_market', 'during_market', 'unknown'
                    fiscal_year INTEGER,
                    fiscal_period VARCHAR(10),  -- Q1, Q2, Q3, Q4, FY
                    date_status VARCHAR(20),  -- 'projected', 'confirmed'
                    importance INTEGER,  -- 0-5 scale from Benzinga
                    estimated_eps DECIMAL(12,4),
                    actual_eps DECIMAL(12,4),
                    eps_surprise_percent DECIMAL(8,4),
                    estimated_revenue DECIMAL(18,2),
                    actual_revenue DECIMAL(18,2),
                    revenue_surprise_percent DECIMAL(8,4),
                    company_name VARCHAR(255),
                    benzinga_id VARCHAR(50),
                    last_updated TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT earnings_calendar_unique UNIQUE (symbol, earnings_date)
                );
            """)
            print("[OK] Created earnings_calendar table")
            
            # Create indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_earnings_calendar_symbol ON earnings_calendar (symbol);",
                "CREATE INDEX IF NOT EXISTS idx_earnings_calendar_date ON earnings_calendar (earnings_date);",
                "CREATE INDEX IF NOT EXISTS idx_earnings_calendar_symbol_date ON earnings_calendar (symbol, earnings_date);",
                # Index for confirmed dates (partial index with immutable predicate)
                "CREATE INDEX IF NOT EXISTS idx_earnings_calendar_confirmed ON earnings_calendar (symbol, earnings_date) WHERE date_status = 'confirmed';",
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
            print("[OK] Created indexes for earnings_calendar")
            
            # Create trigger for updated_at
            cur.execute("""
                DROP TRIGGER IF EXISTS update_earnings_calendar_updated_at ON earnings_calendar;
                CREATE TRIGGER update_earnings_calendar_updated_at
                    BEFORE UPDATE ON earnings_calendar
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
            """)
            print("[OK] Created update trigger")
            
            # Create helper function to get earnings proximity for a symbol/date
            cur.execute("""
                CREATE OR REPLACE FUNCTION get_earnings_proximity(p_symbol VARCHAR, p_date DATE)
                RETURNS INTEGER AS $$
                DECLARE
                    days_to_earnings INTEGER;
                BEGIN
                    -- Find the nearest upcoming earnings date
                    SELECT MIN(earnings_date - p_date) INTO days_to_earnings
                    FROM earnings_calendar
                    WHERE symbol = p_symbol
                      AND earnings_date >= p_date
                      AND earnings_date <= p_date + INTERVAL '30 days';
                    
                    -- If no upcoming earnings found, return NULL
                    RETURN days_to_earnings;
                END;
                $$ LANGUAGE plpgsql SECURITY INVOKER;
            """)
            print("[OK] Created get_earnings_proximity function")
            
            # Create view for symbols with upcoming earnings
            # SECURITY INVOKER ensures RLS policies apply to querying user, not view creator
            cur.execute("""
                CREATE OR REPLACE VIEW upcoming_earnings 
                WITH (security_invoker = true) AS
                SELECT 
                    symbol,
                    earnings_date,
                    time_of_day,
                    fiscal_period,
                    date_status,
                    importance,
                    estimated_eps,
                    company_name,
                    earnings_date - CURRENT_DATE as days_until_earnings
                FROM earnings_calendar
                WHERE earnings_date >= CURRENT_DATE
                  AND earnings_date <= CURRENT_DATE + INTERVAL '14 days'
                ORDER BY earnings_date, symbol;
            """)
            print("[OK] Created upcoming_earnings view")
            
            conn.commit()
            print("[OK] Migration completed successfully!")
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Drop the earnings_calendar table and related objects."""
    print("Rolling back earnings_calendar table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS upcoming_earnings CASCADE;")
            cur.execute("DROP FUNCTION IF EXISTS get_earnings_proximity(VARCHAR, DATE) CASCADE;")
            cur.execute("DROP TABLE IF EXISTS earnings_calendar CASCADE;")
            conn.commit()
            print("[OK] Rollback completed successfully!")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Create earnings_calendar table')
    parser.add_argument('--down', action='store_true', help='Rollback the migration')
    
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
