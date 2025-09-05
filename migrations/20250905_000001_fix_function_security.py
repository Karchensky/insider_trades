"""
Fix Function Security - Search Path Mutable
Version: 20250905_000001

Fixes security warnings by setting proper search_path for PostgreSQL functions:
- update_updated_at_column
- update_daily_option_snapshot_from_temp  
- cleanup_old_anomalies

This prevents search path injection attacks.
"""

import logging
from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Fix function security by setting proper search_path."""
    logger.info("Fixing function security - setting search_path...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # 1. Fix update_updated_at_column function
            logger.info("Fixing update_updated_at_column function...")
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER 
                LANGUAGE plpgsql
                SECURITY DEFINER
                SET search_path = public
                AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$;
            """)
            
            # 2. Fix update_daily_option_snapshot_from_temp function
            logger.info("Fixing update_daily_option_snapshot_from_temp function...")
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_daily_option_snapshot_from_temp(target_date DATE)
                RETURNS INTEGER 
                LANGUAGE plpgsql
                SECURITY DEFINER
                SET search_path = public
                AS $$
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
                $$;
            """)
            
            # 3. Fix cleanup_old_anomalies function
            logger.info("Fixing cleanup_old_anomalies function...")
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
                    
                    DELETE FROM temp_anomaly 
                    WHERE event_date < cutoff_date;
                    
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RETURN deleted_count;
                END;
                $$;
            """)
            
            conn.commit()
            logger.info("Function security fixes completed successfully!")
            
    except Exception as e:
        logger.error(f"Failed to fix function security: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Rollback function security fixes (restore original functions)."""
    logger.info("Rolling back function security fixes...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # 1. Restore original update_updated_at_column
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
            """)
            
            # 2. Restore original update_daily_option_snapshot_from_temp
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_daily_option_snapshot_from_temp(target_date DATE)
                RETURNS INTEGER AS $$
                DECLARE
                    updated_count INTEGER := 0;
                BEGIN
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
                            symbol, contract_ticker, implied_volatility,
                            open_interest, greeks_delta, greeks_gamma,
                            greeks_theta, greeks_vega
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
            
            # 3. Restore original cleanup_old_anomalies
            cur.execute("""
                CREATE OR REPLACE FUNCTION cleanup_old_anomalies(retention_days INTEGER DEFAULT 7)
                RETURNS INTEGER AS $$
                DECLARE
                    deleted_count INTEGER := 0;
                    cutoff_date DATE;
                BEGIN
                    cutoff_date := CURRENT_DATE - INTERVAL '1 day' * retention_days;
                    
                    DELETE FROM temp_anomaly 
                    WHERE event_date < cutoff_date;
                    
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RETURN deleted_count;
                END;
                $$ LANGUAGE plpgsql;
            """)
            
            conn.commit()
            logger.info("Function security rollback completed!")
            
    except Exception as e:
        logger.error(f"Failed to rollback function security: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    up()
