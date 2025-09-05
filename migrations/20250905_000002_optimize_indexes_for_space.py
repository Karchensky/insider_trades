"""
Optimize Database Indexes for Space Efficiency
Version: 20250905_000002

Removes redundant indexes and optimizes remaining ones to reclaim ~2-3GB of disk space
while maintaining query performance for daily and intraday processes.

Target: Reduce index size from 4,103 MB to ~1,500 MB
"""

import logging
from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Optimize indexes for space efficiency while maintaining performance."""
    logger.info("Optimizing database indexes for space efficiency...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # ===== OPTIMIZE daily_option_snapshot indexes =====
            logger.info("Optimizing daily_option_snapshot indexes...")
            
            # Drop redundant indexes (PRIMARY KEY already covers these)
            redundant_dos_indexes = [
                "idx_daily_option_snapshot_compound",  # Duplicates PRIMARY KEY
                "idx_daily_option_snapshot_date",      # Covered by PRIMARY KEY prefix
                "idx_daily_option_snapshot_created_at", # Rarely used
                "idx_daily_option_snapshot_close",     # Single column, low selectivity
                
                # Drop individual Greeks indexes (create one composite instead)
                "idx_daily_option_snapshot_delta",
                "idx_daily_option_snapshot_gamma", 
                "idx_daily_option_snapshot_theta",
                "idx_daily_option_snapshot_vega"
            ]
            
            for index_name in redundant_dos_indexes:
                try:
                    cur.execute(f"DROP INDEX IF EXISTS {index_name};")
                    logger.info(f"Dropped redundant index: {index_name}")
                except Exception as e:
                    logger.warning(f"Could not drop {index_name}: {e}")
            
            # Create optimized composite indexes for actual query patterns
            logger.info("Creating optimized composite indexes...")
            
            # For anomaly detection queries (symbol + date range)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dos_symbol_date_volume 
                ON daily_option_snapshot (symbol, date, volume) 
                WHERE volume > 0;
            """)
            
            # For Greeks analysis (only if needed - most queries filter by symbol first)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_dos_analytics 
                ON daily_option_snapshot (symbol, implied_volatility, open_interest) 
                WHERE implied_volatility IS NOT NULL;
            """)
            
            # ===== OPTIMIZE option_contracts indexes =====
            logger.info("Optimizing option_contracts indexes...")
            
            # Drop redundant single-column indexes
            redundant_oc_indexes = [
                "idx_option_contracts_symbol",          # Covered by PRIMARY KEY prefix
                "idx_option_contracts_contract_ticker", # Covered by PRIMARY KEY suffix
                "idx_option_contracts_underlying",      # Usually same as symbol
                "idx_option_contracts_type",           # Low selectivity alone
                "idx_option_contracts_strike",         # Low selectivity alone
                "idx_option_contracts_expiration",     # Better in composite
                
                # Drop some composite indexes with limited use
                "idx_option_contracts_type_expiry",    # Rarely used pattern
                "idx_option_contracts_strike_expiry",  # Rarely used pattern
            ]
            
            for index_name in redundant_oc_indexes:
                try:
                    cur.execute(f"DROP INDEX IF EXISTS {index_name};")
                    logger.info(f"Dropped redundant index: {index_name}")
                except Exception as e:
                    logger.warning(f"Could not drop {index_name}: {e}")
            
            # Keep only essential composite indexes for actual query patterns
            logger.info("Creating essential composite indexes...")
            
            # For anomaly detection: symbol + contract_type + expiration filtering
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_oc_symbol_type_expiry_strike 
                ON option_contracts (symbol, contract_type, expiration_date, strike_price);
            """)
            
            # For OTM analysis: calls above strike, puts below strike
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_oc_calls_otm 
                ON option_contracts (symbol, strike_price, expiration_date) 
                WHERE contract_type = 'call';
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_oc_puts_otm 
                ON option_contracts (symbol, strike_price, expiration_date) 
                WHERE contract_type = 'put';
            """)
            
            # ===== OPTIMIZE temp_option indexes (if any exist) =====
            logger.info("Checking temp_option indexes...")
            
            # temp_option should have minimal indexes since it's cleared frequently
            cur.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE tablename = 'temp_option' AND indexname != 'temp_option_pkey';
            """)
            temp_indexes = cur.fetchall()
            
            for row in temp_indexes:
                index_name = row[0] if isinstance(row, tuple) else row['indexname']
                try:
                    cur.execute(f"DROP INDEX IF EXISTS {index_name};")
                    logger.info(f"Dropped temp index: {index_name}")
                except Exception as e:
                    logger.warning(f"Could not drop {index_name}: {e}")
            
            # Create only essential temp_option index for intraday queries
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_temp_option_symbol_timestamp 
                ON temp_option (symbol, as_of_timestamp) 
                WHERE session_volume > 0;
            """)
            
            conn.commit()
            logger.info("Index optimization completed successfully!")
            logger.info("Expected space savings: ~2-3 GB")
            
    except Exception as e:
        logger.error(f"Failed to optimize indexes: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Restore original indexes (not recommended due to space usage)."""
    logger.warning("Rolling back index optimization (will restore high space usage)...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # Restore daily_option_snapshot indexes
            logger.info("Restoring daily_option_snapshot indexes...")
            
            daily_option_indexes = [
                "CREATE INDEX idx_daily_option_snapshot_date ON daily_option_snapshot (date);",
                "CREATE INDEX idx_daily_option_snapshot_contract ON daily_option_snapshot (contract_ticker);",
                "CREATE INDEX idx_daily_option_snapshot_close ON daily_option_snapshot (close_price);",
                "CREATE INDEX idx_daily_option_snapshot_volume ON daily_option_snapshot (volume);",
                "CREATE INDEX idx_daily_option_snapshot_created_at ON daily_option_snapshot (created_at);",
                "CREATE INDEX idx_daily_option_snapshot_compound ON daily_option_snapshot (date, symbol, contract_ticker);",
                "CREATE INDEX idx_dos_ct_date ON daily_option_snapshot(contract_ticker, date);",
                "CREATE INDEX idx_dos_sym_date ON daily_option_snapshot(symbol, date);",
                "CREATE INDEX idx_daily_option_snapshot_iv ON daily_option_snapshot (implied_volatility);",
                "CREATE INDEX idx_daily_option_snapshot_oi ON daily_option_snapshot (open_interest);",
                "CREATE INDEX idx_daily_option_snapshot_delta ON daily_option_snapshot (greeks_delta);",
                "CREATE INDEX idx_daily_option_snapshot_gamma ON daily_option_snapshot (greeks_gamma);",
                "CREATE INDEX idx_daily_option_snapshot_theta ON daily_option_snapshot (greeks_theta);",
                "CREATE INDEX idx_daily_option_snapshot_vega ON daily_option_snapshot (greeks_vega);"
            ]
            
            for idx_sql in daily_option_indexes:
                try:
                    cur.execute(idx_sql)
                except Exception as e:
                    logger.warning(f"Could not restore index: {e}")
            
            # Restore option_contracts indexes
            logger.info("Restoring option_contracts indexes...")
            
            option_contract_indexes = [
                "CREATE INDEX idx_option_contracts_symbol ON option_contracts (symbol);",
                "CREATE INDEX idx_option_contracts_contract_ticker ON option_contracts (contract_ticker);",
                "CREATE INDEX idx_option_contracts_underlying ON option_contracts (underlying_ticker);",
                "CREATE INDEX idx_option_contracts_expiration ON option_contracts (expiration_date);",
                "CREATE INDEX idx_option_contracts_strike ON option_contracts (strike_price);",
                "CREATE INDEX idx_option_contracts_type ON option_contracts (contract_type);",
                "CREATE INDEX idx_option_contracts_symbol_expiry ON option_contracts (symbol, expiration_date);",
                "CREATE INDEX idx_option_contracts_symbol_type ON option_contracts (symbol, contract_type);",
                "CREATE INDEX idx_option_contracts_type_expiry ON option_contracts (contract_type, expiration_date);",
                "CREATE INDEX idx_option_contracts_strike_expiry ON option_contracts (strike_price, expiration_date);",
                "CREATE INDEX idx_option_contracts_calls ON option_contracts (symbol, expiration_date, strike_price) WHERE contract_type = 'call';",
                "CREATE INDEX idx_option_contracts_puts ON option_contracts (symbol, expiration_date, strike_price) WHERE contract_type = 'put';"
            ]
            
            for idx_sql in option_contract_indexes:
                try:
                    cur.execute(idx_sql)
                except Exception as e:
                    logger.warning(f"Could not restore index: {e}")
            
            conn.commit()
            logger.warning("Index optimization rollback completed (high space usage restored)")
            
    except Exception as e:
        logger.error(f"Failed to rollback index optimization: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    up()
