"""
Create Option Contracts Table Migration
Version: 20250904_000001

Creates option_contracts table to store contract metadata from Polygon.io API.
This table contains static contract information (type, expiration, strike, etc.)
separate from the pricing/volume data in daily_option_snapshot.
"""

import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Create the option_contracts table."""
    logger.info("Creating option_contracts table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Create the option_contracts table
            logger.info("Creating option_contracts table...")
            cur.execute("""
                CREATE TABLE option_contracts (
                    symbol VARCHAR(10) NOT NULL,
                    contract_ticker VARCHAR(50) NOT NULL,
                    cfi VARCHAR(10),
                    contract_type VARCHAR(10) NOT NULL,
                    exercise_style VARCHAR(20),
                    expiration_date DATE NOT NULL,
                    primary_exchange VARCHAR(10),
                    shares_per_contract INTEGER DEFAULT 100,
                    strike_price DECIMAL(18,6) NOT NULL,
                    underlying_ticker VARCHAR(10) NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    
                    PRIMARY KEY (symbol, contract_ticker)
                );
            """)
            
            # Create indexes for optimal query performance
            logger.info("Creating indexes for option_contracts table...")
            
            indexes = [
                # Core lookup indexes
                "CREATE INDEX idx_option_contracts_symbol ON option_contracts (symbol);",
                "CREATE INDEX idx_option_contracts_contract_ticker ON option_contracts (contract_ticker);",
                "CREATE INDEX idx_option_contracts_underlying ON option_contracts (underlying_ticker);",
                
                # Query optimization indexes
                "CREATE INDEX idx_option_contracts_expiration ON option_contracts (expiration_date);",
                "CREATE INDEX idx_option_contracts_strike ON option_contracts (strike_price);",
                "CREATE INDEX idx_option_contracts_type ON option_contracts (contract_type);",
                
                # Composite indexes for common queries
                "CREATE INDEX idx_option_contracts_symbol_expiry ON option_contracts (symbol, expiration_date);",
                "CREATE INDEX idx_option_contracts_symbol_type ON option_contracts (symbol, contract_type);",
                "CREATE INDEX idx_option_contracts_type_expiry ON option_contracts (contract_type, expiration_date);",
                "CREATE INDEX idx_option_contracts_strike_expiry ON option_contracts (strike_price, expiration_date);",
                
                # Advanced filtering (removed CURRENT_DATE predicate due to immutability requirement)
                "CREATE INDEX idx_option_contracts_calls ON option_contracts (symbol, expiration_date, strike_price) WHERE contract_type = 'call';",
                "CREATE INDEX idx_option_contracts_puts ON option_contracts (symbol, expiration_date, strike_price) WHERE contract_type = 'put';"
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
                logger.info(f"Created index: {idx_sql.split()[2]}")
            
            # Create trigger for updated_at
            logger.info("Creating update trigger...")
            cur.execute("""
                CREATE TRIGGER update_option_contracts_updated_at
                    BEFORE UPDATE ON option_contracts
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
            """)
            
            # Set up RLS (Row Level Security) policies
            logger.info("Setting up RLS policies...")
            cur.execute("ALTER TABLE option_contracts ENABLE ROW LEVEL SECURITY;")
            
            # RLS policies matching existing database security patterns
            rls_policies = [
                # Service role full access
                (
                    "option_contracts_service_full",
                    """DO $$ BEGIN
                        CREATE POLICY option_contracts_service_full ON option_contracts
                        FOR ALL TO service_role USING (true) WITH CHECK (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;"""
                ),
                # Postgres full access
                (
                    "option_contracts_postgres_full", 
                    """DO $$ BEGIN
                        CREATE POLICY option_contracts_postgres_full ON option_contracts
                        FOR ALL TO postgres USING (true) WITH CHECK (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;"""
                ),
                # Authenticated users read access
                (
                    "option_contracts_authenticated_read",
                    """DO $$ BEGIN
                        CREATE POLICY option_contracts_authenticated_read ON option_contracts
                        FOR SELECT USING (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;"""
                ),
                # Authenticated users insert access
                (
                    "option_contracts_authenticated_insert",
                    """DO $$ BEGIN
                        CREATE POLICY option_contracts_authenticated_insert ON option_contracts
                        FOR INSERT WITH CHECK (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;"""
                ),
                # Authenticated users update access
                (
                    "option_contracts_authenticated_update",
                    """DO $$ BEGIN
                        CREATE POLICY option_contracts_authenticated_update ON option_contracts
                        FOR UPDATE USING (auth.role() = 'authenticated') WITH CHECK (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;"""
                )
            ]
            
            for policy_name, policy_sql in rls_policies:
                try:
                    cur.execute(policy_sql)
                    logger.info(f"Created RLS policy: {policy_name}")
                except Exception as e:
                    logger.warning(f"RLS policy creation failed for {policy_name}: {e}")
            
            conn.commit()
            logger.info("option_contracts table created successfully!")
            
    except Exception as e:
        logger.error(f"Failed to create option_contracts table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Drop the option_contracts table and related objects."""
    logger.warning("Dropping option_contracts table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop the table (this will cascade drop all indexes and triggers)
            cur.execute("DROP TABLE IF EXISTS option_contracts CASCADE;")
            
            conn.commit()
            logger.info("option_contracts table dropped successfully!")
            
    except Exception as e:
        logger.error(f"Failed to drop option_contracts table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
