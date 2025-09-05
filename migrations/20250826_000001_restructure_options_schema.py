"""
Options Schema Restructuring Migration
Version: 20250826_000001

Changes:
1. Remove full_daily_anomaly_snapshot table
2. Remove full_daily_option_snapshot table  
3. Remove after_hours_price and pre_market_price columns from daily_option_snapshot
4. Add implied_volatility, open_interest, greeks_delta, greeks_gamma, greeks_theta, greeks_vega columns to daily_option_snapshot
5. Update process flow to populate new columns instead of copying to removed tables
"""

import logging
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the schema restructuring changes."""
    logger.info("Starting options schema restructuring...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # 1. Remove full_daily_anomaly_snapshot table
            logger.info("Removing full_daily_anomaly_snapshot table...")
            cur.execute("DROP TABLE IF EXISTS full_daily_anomaly_snapshot CASCADE;")
            
            # 2. Remove full_daily_option_snapshot table
            logger.info("Removing full_daily_option_snapshot table...")
            cur.execute("DROP TABLE IF EXISTS full_daily_option_snapshot CASCADE;")
            
            # 3. Remove after_hours_price and pre_market_price columns from daily_option_snapshot
            logger.info("Removing after_hours_price and pre_market_price columns...")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS after_hours_price;")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS pre_market_price;")
            
            # 4. Add new columns to daily_option_snapshot
            logger.info("Adding new analytics columns to daily_option_snapshot...")
            
            # Add implied_volatility column
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS implied_volatility DECIMAL(18,8);
            """)
            
            # Add open_interest column
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS open_interest BIGINT;
            """)
            
            # Add greeks_delta column
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS greeks_delta DECIMAL(18,8);
            """)
            
            # Add greeks_gamma column
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS greeks_gamma DECIMAL(18,8);
            """)
            
            # Add greeks_theta column
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS greeks_theta DECIMAL(18,8);
            """)
            
            # Add greeks_vega column
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS greeks_vega DECIMAL(18,8);
            """)
            
            # 5. Create indexes for the new columns to improve query performance
            logger.info("Creating indexes for new columns...")
            
            # Index for implied_volatility
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_iv 
                ON daily_option_snapshot (implied_volatility);
            """)
            
            # Index for open_interest
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_oi 
                ON daily_option_snapshot (open_interest);
            """)
            
            # Index for greeks_delta
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_delta 
                ON daily_option_snapshot (greeks_delta);
            """)
            
            # Index for greeks_gamma
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_gamma 
                ON daily_option_snapshot (greeks_gamma);
            """)
            
            # Index for greeks_theta
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_theta 
                ON daily_option_snapshot (greeks_theta);
            """)
            
            # Index for greeks_vega
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_vega 
                ON daily_option_snapshot (greeks_vega);
            """)
            
            # 6. Create a function to update daily_option_snapshot with latest temp_option data
            logger.info("Creating function to update daily_option_snapshot with temp data...")
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_daily_option_snapshot_from_temp(target_date DATE)
                RETURNS INTEGER AS $$
                DECLARE
                    updated_count INTEGER := 0;
                BEGIN
                    -- Update existing records with latest temp_option data
                    UPDATE daily_option_snapshot dos
                    SET 
                        implied_volatility = temp.implied_volatility,
                        open_interest = temp.open_interest,
                        greeks_delta = temp.greeks_delta,
                        greeks_gamma = temp.greeks_gamma,
                        greeks_theta = temp.greeks_theta,
                        greeks_vega = temp.greeks_vega,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT DISTINCT ON (symbol, contract_ticker)
                            symbol,
                            contract_ticker,
                            implied_volatility,
                            open_interest,
                            greeks_delta,
                            greeks_gamma,
                            greeks_theta,
                            greeks_vega
                        FROM temp_option
                        WHERE DATE(as_of_timestamp) = target_date
                        ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                    ) temp
                    WHERE dos.date = target_date
                      AND dos.symbol = temp.symbol
                      AND dos.contract_ticker = temp.contract_ticker;
                    
                    GET DIAGNOSTICS updated_count = ROW_COUNT;
                    RETURN updated_count;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            conn.commit()
            logger.info("Options schema restructuring completed successfully!")
            
    except Exception as e:
        logger.error(f"Schema restructuring failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Rollback the schema restructuring changes."""
    logger.warning("Rolling back options schema restructuring...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # 1. Drop the update function
            cur.execute("DROP FUNCTION IF EXISTS update_daily_option_snapshot_from_temp(DATE);")
            
            # 2. Remove the new columns from daily_option_snapshot
            logger.info("Removing new analytics columns...")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS implied_volatility;")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS open_interest;")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS greeks_delta;")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS greeks_gamma;")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS greeks_theta;")
            cur.execute("ALTER TABLE daily_option_snapshot DROP COLUMN IF EXISTS greeks_vega;")
            
            # 3. Re-add the removed columns
            logger.info("Re-adding removed columns...")
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS after_hours_price DECIMAL(12, 4);
            """)
            cur.execute("""
                ALTER TABLE daily_option_snapshot 
                ADD COLUMN IF NOT EXISTS pre_market_price DECIMAL(12, 4);
            """)
            
            # 4. Recreate the full tables (this would require the original migration to be run again)
            logger.warning("Note: Full tables recreation requires running the original migration script")
            
            conn.commit()
            logger.info("Schema rollback completed!")
            
    except Exception as e:
        logger.error(f"Schema rollback failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
