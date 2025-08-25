"""
Complete Database Setup - Single Migration
Version: 20250825_000001

Creates all tables with updated naming convention:
- daily_option_snapshot
- daily_stock_snapshot  
- full_daily_option_snapshot (formerly daily_option_snapshot_full)
- full_daily_anomaly_snapshot (formerly anomaly_event_full)
- temp_anomaly (formerly temp_anomaly_event)
- temp_option (formerly temp_option_snapshot)
- temp_stock (formerly temp_stock_snapshot)
"""

import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Create complete database structure with updated table names."""
    logger.info("Creating complete database structure...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # 1. Create trigger function (used by multiple tables)
            logger.info("Creating update trigger function...")
            cur.execute("""
                CREATE OR REPLACE FUNCTION update_updated_at_column()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.updated_at = CURRENT_TIMESTAMP;
                    RETURN NEW;
                END;
                $$ language 'plpgsql';
            """)
            
            # 2. Create daily_stock_snapshot table
            logger.info("Creating daily_stock_snapshot table...")
            cur.execute("""
                CREATE TABLE daily_stock_snapshot (
                    date DATE NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    close DECIMAL(12, 4) NOT NULL,
                    high DECIMAL(12, 4) NOT NULL,
                    low DECIMAL(12, 4) NOT NULL,
                    transaction_volume INTEGER NOT NULL,
                    open DECIMAL(12, 4) NOT NULL,
                    trading_volume BIGINT NOT NULL,
                    weighted_average_price DECIMAL(12, 4) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    
                    PRIMARY KEY (date, symbol)
                );
            """)
            
            # Indexes for daily_stock_snapshot
            daily_stock_indexes = [
                "CREATE INDEX idx_daily_stock_symbol ON daily_stock_snapshot (symbol);",
                "CREATE INDEX idx_daily_stock_date ON daily_stock_snapshot (date);",
                "CREATE INDEX idx_daily_stock_created_at ON daily_stock_snapshot (created_at);"
            ]
            
            for idx_sql in daily_stock_indexes:
                cur.execute(idx_sql)
            
            # Trigger for daily_stock_snapshot
            cur.execute("""
                CREATE TRIGGER update_daily_stock_snapshot_updated_at
                    BEFORE UPDATE ON daily_stock_snapshot
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
            """)
            
            # 3. Create daily_option_snapshot table
            logger.info("Creating daily_option_snapshot table...")
            cur.execute("""
                CREATE TABLE daily_option_snapshot (
                    date DATE NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    contract_ticker VARCHAR(50) NOT NULL,
                    open_price DECIMAL(12, 4),
                    high_price DECIMAL(12, 4),
                    low_price DECIMAL(12, 4),
                    close_price DECIMAL(12, 4),
                    volume BIGINT DEFAULT 0,
                    pre_market_price DECIMAL(12, 4),
                    after_hours_price DECIMAL(12, 4),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    
                    PRIMARY KEY (date, symbol, contract_ticker)
                );
            """)
            
            # Indexes for daily_option_snapshot
            daily_option_indexes = [
                "CREATE INDEX idx_daily_option_snapshot_date ON daily_option_snapshot (date);",
                "CREATE INDEX idx_daily_option_snapshot_contract ON daily_option_snapshot (contract_ticker);",
                "CREATE INDEX idx_daily_option_snapshot_close ON daily_option_snapshot (close_price);",
                "CREATE INDEX idx_daily_option_snapshot_volume ON daily_option_snapshot (volume);",
                "CREATE INDEX idx_daily_option_snapshot_created_at ON daily_option_snapshot (created_at);",
                "CREATE INDEX idx_daily_option_snapshot_compound ON daily_option_snapshot (date, symbol, contract_ticker);",
                "CREATE INDEX idx_dos_ct_date ON daily_option_snapshot(contract_ticker, date);",
                "CREATE INDEX idx_dos_sym_date ON daily_option_snapshot(symbol, date);"
            ]
            
            for idx_sql in daily_option_indexes:
                cur.execute(idx_sql)
            
            # 4. Create temp_stock table (current structure after migrations)
            logger.info("Creating temp_stock table...")
            cur.execute("""
                CREATE TABLE temp_stock (
                    as_of_timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    
                    day_open DECIMAL(12, 4),
                    day_high DECIMAL(12, 4),
                    day_low DECIMAL(12, 4),
                    day_close DECIMAL(12, 4),
                    day_volume BIGINT,
                    day_vwap DECIMAL(12, 4),
                    
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    
                    PRIMARY KEY (as_of_timestamp, symbol)
                );
            """)
            
            # Indexes for temp_stock
            temp_stock_indexes = [
                "CREATE INDEX idx_temp_stock_symbol ON temp_stock (symbol);",
                "CREATE INDEX idx_temp_stock_asof ON temp_stock (as_of_timestamp);",
                "CREATE INDEX idx_temp_stock_symbol_ts_enh ON temp_stock(symbol, as_of_timestamp);"
            ]
            
            for idx_sql in temp_stock_indexes:
                cur.execute(idx_sql)
            
            # 5. Create temp_option table (current structure)
            logger.info("Creating temp_option table...")
            cur.execute("""
                CREATE TABLE temp_option (
                    as_of_timestamp TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    contract_ticker VARCHAR(50) NOT NULL,
                    
                    break_even_price DECIMAL(18,6),
                    strike_price DECIMAL(18,6),
                    implied_volatility DECIMAL(18,8),
                    open_interest BIGINT,
                    
                    greeks_delta DECIMAL(18,8),
                    greeks_gamma DECIMAL(18,8),
                    greeks_theta DECIMAL(18,8),
                    greeks_vega DECIMAL(18,8),
                    
                    contract_type VARCHAR(10),
                    exercise_style VARCHAR(20),
                    expiration_date DATE,
                    shares_per_contract INTEGER,
                    
                    session_open DECIMAL(18,6),
                    session_high DECIMAL(18,6),
                    session_low DECIMAL(18,6),
                    session_close DECIMAL(18,6),
                    session_volume BIGINT,
                    session_change DECIMAL(18,6),
                    session_change_percent DECIMAL(18,6),
                    session_early_trading_change DECIMAL(18,6),
                    session_early_trading_change_percent DECIMAL(18,6),
                    session_regular_trading_change DECIMAL(18,6),
                    session_regular_trading_change_percent DECIMAL(18,6),
                    session_late_trading_change DECIMAL(18,6),
                    session_late_trading_change_percent DECIMAL(18,6),
                    session_previous_close DECIMAL(18,6),
                    
                    underlying_ticker VARCHAR(10),
                    underlying_price DECIMAL(18,6),
                    underlying_change_to_break_even DECIMAL(18,6),
                    underlying_last_updated TIMESTAMPTZ,
                    
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    
                    PRIMARY KEY (as_of_timestamp, symbol, contract_ticker)
                );
            """)
            
            # Indexes for temp_option
            temp_option_indexes = [
                "CREATE INDEX idx_temp_option_symbol ON temp_option(symbol);",
                "CREATE INDEX idx_temp_option_contract_ticker ON temp_option(contract_ticker);",
                "CREATE INDEX idx_temp_option_timestamp ON temp_option(as_of_timestamp);",
                "CREATE INDEX idx_temp_option_expiry ON temp_option(expiration_date);",
                "CREATE INDEX idx_temp_option_volume ON temp_option(session_volume);",
                "CREATE INDEX idx_temp_option_oi ON temp_option(open_interest);",
                "CREATE INDEX idx_temp_option_volume_enh ON temp_option(session_volume);",
                "CREATE INDEX idx_temp_option_symbol_type_enh ON temp_option(symbol, contract_type);",
                "CREATE INDEX idx_temp_option_expiry_enh ON temp_option(expiration_date);",
                "CREATE INDEX idx_temp_option_oi_enh ON temp_option(open_interest);"
            ]
            
            for idx_sql in temp_option_indexes:
                cur.execute(idx_sql)
            
            # 6. Create full_daily_option_snapshot table
            logger.info("Creating full_daily_option_snapshot table...")
            cur.execute("""
                CREATE TABLE full_daily_option_snapshot (
                    snapshot_date DATE NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    contract_ticker VARCHAR(50) NOT NULL,
                    as_of_timestamp TIMESTAMPTZ NOT NULL,
                    break_even_price DECIMAL(18,6),
                    strike_price DECIMAL(18,6),
                    implied_volatility DECIMAL(18,8),
                    open_interest BIGINT,
                    greeks_delta DECIMAL(18,8),
                    greeks_gamma DECIMAL(18,8),
                    greeks_theta DECIMAL(18,8),
                    greeks_vega DECIMAL(18,8),
                    contract_type VARCHAR(10),
                    exercise_style VARCHAR(20),
                    expiration_date DATE,
                    shares_per_contract INTEGER,
                    session_open DECIMAL(18,6),
                    session_high DECIMAL(18,6),
                    session_low DECIMAL(18,6),
                    session_close DECIMAL(18,6),
                    session_volume BIGINT,
                    session_change DECIMAL(18,6),
                    session_change_percent DECIMAL(18,6),
                    session_early_trading_change DECIMAL(18,6),
                    session_early_trading_change_percent DECIMAL(18,6),
                    session_regular_trading_change DECIMAL(18,6),
                    session_regular_trading_change_percent DECIMAL(18,6),
                    session_late_trading_change DECIMAL(18,6),
                    session_late_trading_change_percent DECIMAL(18,6),
                    session_previous_close DECIMAL(18,6),
                    underlying_ticker VARCHAR(10),
                    underlying_price DECIMAL(18,6),
                    underlying_change_to_break_even DECIMAL(18,6),
                    underlying_last_updated TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (snapshot_date, symbol, contract_ticker)
                );
            """)
            
            # Indexes for full_daily_option_snapshot
            full_daily_option_indexes = [
                "CREATE INDEX idx_fdos_sym_date ON full_daily_option_snapshot(symbol, snapshot_date);",
                "CREATE INDEX idx_fdos_ct_date ON full_daily_option_snapshot(contract_ticker, snapshot_date);",
                "CREATE INDEX idx_full_daily_option_snapshot_contract_date_enh ON full_daily_option_snapshot(contract_ticker, snapshot_date);"
            ]
            
            for idx_sql in full_daily_option_indexes:
                cur.execute(idx_sql)
            
            # 7. Create temp_anomaly table (intraday, mutable)
            logger.info("Creating temp_anomaly table...")
            cur.execute("""
                CREATE TABLE temp_anomaly (
                    id BIGSERIAL PRIMARY KEY,
                    event_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(10),
                    expiry_date DATE,
                    as_of_timestamp TIMESTAMPTZ,
                    kind VARCHAR(128) NOT NULL,
                    score DECIMAL(18,6) NOT NULL,
                    details JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT uq_temp_anomaly UNIQUE (event_date, symbol, direction, expiry_date, kind)
                );
            """)
            
            # 8. Create full_daily_anomaly_snapshot table (end-of-day, permanent)
            logger.info("Creating full_daily_anomaly_snapshot table...")
            cur.execute("""
                CREATE TABLE full_daily_anomaly_snapshot (
                    id BIGSERIAL PRIMARY KEY,
                    event_date DATE NOT NULL,
                    symbol VARCHAR(32) NOT NULL,
                    direction VARCHAR(10),
                    expiry_date DATE,
                    as_of_timestamp TIMESTAMPTZ,
                    kind VARCHAR(128) NOT NULL,
                    score DECIMAL(18,6) NOT NULL,
                    details JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT uq_full_daily_anomaly_snapshot UNIQUE (event_date, symbol, direction, expiry_date, kind)
                );
            """)
            
            # Indexes for anomaly tables
            anomaly_indexes = [
                # temp_anomaly indexes
                "CREATE INDEX idx_temp_anomaly_date ON temp_anomaly(event_date);",
                "CREATE INDEX idx_temp_anomaly_symbol ON temp_anomaly(symbol);", 
                "CREATE INDEX idx_temp_anomaly_score ON temp_anomaly(score DESC);",
                "CREATE INDEX idx_temp_anomaly_kind ON temp_anomaly(kind);",
                "CREATE INDEX idx_temp_anomaly_timestamp ON temp_anomaly(as_of_timestamp);",
                "CREATE INDEX idx_temp_anomaly_lookup ON temp_anomaly(event_date, symbol, direction);",
                "CREATE INDEX idx_temp_anomaly_compound ON temp_anomaly(event_date, symbol, kind);",
                
                # full_daily_anomaly_snapshot indexes
                "CREATE INDEX idx_full_daily_anomaly_snapshot_date ON full_daily_anomaly_snapshot(event_date);",
                "CREATE INDEX idx_full_daily_anomaly_snapshot_symbol ON full_daily_anomaly_snapshot(symbol);",
                "CREATE INDEX idx_full_daily_anomaly_snapshot_score ON full_daily_anomaly_snapshot(score DESC);", 
                "CREATE INDEX idx_full_daily_anomaly_snapshot_kind ON full_daily_anomaly_snapshot(kind);",
                "CREATE INDEX idx_full_daily_anomaly_snapshot_timestamp ON full_daily_anomaly_snapshot(as_of_timestamp);",
                "CREATE INDEX idx_full_daily_anomaly_snapshot_lookup ON full_daily_anomaly_snapshot(event_date, symbol, direction);",
                "CREATE INDEX idx_full_daily_anomaly_snapshot_compound ON full_daily_anomaly_snapshot(event_date, symbol, kind);"
            ]
            
            for idx_sql in anomaly_indexes:
                cur.execute(idx_sql)
            
            # Triggers for anomaly tables
            cur.execute("""
                CREATE TRIGGER update_temp_anomaly_updated_at 
                BEFORE UPDATE ON temp_anomaly 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """)
            
            cur.execute("""
                CREATE TRIGGER update_full_daily_anomaly_snapshot_updated_at 
                BEFORE UPDATE ON full_daily_anomaly_snapshot 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """)
            
            # 9. Set up RLS policies for all tables
            logger.info("Setting up RLS policies...")
            
            # Enable RLS on all tables
            tables_with_rls = [
                'daily_stock_snapshot', 'daily_option_snapshot', 'full_daily_option_snapshot',
                'temp_stock', 'temp_option', 'temp_anomaly', 'full_daily_anomaly_snapshot'
            ]
            
            for table in tables_with_rls:
                cur.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
            
            # RLS policies for daily tables (authenticated + service + postgres)
            daily_tables = ['daily_stock_snapshot', 'daily_option_snapshot']
            for table in daily_tables:
                policies = [
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY "Allow read access for authenticated users" ON {table}
                        FOR SELECT USING (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY "Allow insert for authenticated users" ON {table}
                        FOR INSERT WITH CHECK (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY "Allow update for authenticated users" ON {table}
                        FOR UPDATE USING (auth.role() = 'authenticated') WITH CHECK (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY "Allow all for service role" ON {table}
                        FOR ALL USING (auth.jwt() ->> 'role' = 'service_role');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY "Allow all for postgres role" ON {table}
                        FOR ALL USING (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """
                ]
                
                for policy_sql in policies:
                    try:
                        cur.execute(policy_sql)
                    except Exception as e:
                        logger.warning(f"RLS policy creation failed for {table}: {e}")
            
            # RLS for full_daily_option_snapshot
            fdos_policies = [
                """
                DO $$ BEGIN
                    CREATE POLICY "fdos_select_authenticated" ON full_daily_option_snapshot
                    FOR SELECT USING (auth.role() = 'authenticated');
                EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                """,
                """
                DO $$ BEGIN
                    CREATE POLICY "fdos_insert_authenticated" ON full_daily_option_snapshot
                    FOR INSERT WITH CHECK (auth.role() = 'authenticated');
                EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                """,
                """
                DO $$ BEGIN
                    CREATE POLICY "fdos_update_authenticated" ON full_daily_option_snapshot
                    FOR UPDATE USING (auth.role() = 'authenticated') WITH CHECK (auth.role() = 'authenticated');
                EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                """,
                """
                DO $$ BEGIN
                    CREATE POLICY "fdos_service_role_all" ON full_daily_option_snapshot
                    FOR ALL USING ((auth.jwt() ->> 'role') = 'service_role');
                EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                """,
                """
                DO $$ BEGIN
                    CREATE POLICY "fdos_postgres_all" ON full_daily_option_snapshot
                    FOR ALL USING (true);
                EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                """
            ]
            
            for policy_sql in fdos_policies:
                try:
                    cur.execute(policy_sql)
                except Exception as e:
                    logger.warning(f"FDOS RLS policy creation failed: {e}")
            
            # RLS for temp tables (restrictive - only service/postgres)
            temp_tables = ['temp_option', 'temp_stock']
            for table in temp_tables:
                temp_policies = [
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY {table}_all_postgres ON {table}
                        FOR ALL TO postgres USING (true) WITH CHECK (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY {table}_all_service ON {table}
                        FOR ALL TO service_role USING (true) WITH CHECK (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """
                ]
                
                for policy_sql in temp_policies:
                    try:
                        cur.execute(policy_sql)
                    except Exception as e:
                        logger.warning(f"Temp table RLS policy creation failed for {table}: {e}")
            
            # RLS for anomaly tables
            anomaly_tables = ['temp_anomaly', 'full_daily_anomaly_snapshot']
            for table in anomaly_tables:
                anomaly_policies = [
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY {table}_service_full ON {table}
                        FOR ALL TO service_role USING (true) WITH CHECK (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY {table}_postgres_full ON {table}
                        FOR ALL TO postgres USING (true) WITH CHECK (true);
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """,
                    f"""
                    DO $$ BEGIN
                        CREATE POLICY {table}_authenticated_read ON {table}
                        FOR SELECT USING (auth.role() = 'authenticated');
                    EXCEPTION WHEN duplicate_object THEN NULL; END $$;
                    """
                ]
                
                for policy_sql in anomaly_policies:
                    try:
                        cur.execute(policy_sql)
                    except Exception as e:
                        logger.warning(f"Anomaly RLS policy creation failed for {table}: {e}")
            
            conn.commit()
            logger.info("Complete database structure created successfully!")
            
    except Exception as e:
        logger.error(f"❌ Database setup failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Drop all tables (DANGEROUS - only for development)."""
    logger.warning("DROPPING ALL TABLES - THIS WILL DELETE ALL DATA!")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop all tables in reverse dependency order
            tables_to_drop = [
                'temp_anomaly',
                'full_daily_anomaly_snapshot',
                'full_daily_option_snapshot',
                'temp_option',
                'temp_stock',
                'daily_option_snapshot',
                'daily_stock_snapshot'
            ]
            
            for table in tables_to_drop:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                    logger.info(f"Dropped table: {table}")
                except Exception as e:
                    logger.warning(f"Could not drop {table}: {e}")
            
            # Drop the trigger function
            try:
                cur.execute("DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;")
                logger.info("Dropped trigger function")
            except Exception as e:
                logger.warning(f"Could not drop trigger function: {e}")
            
            conn.commit()
            logger.info("All tables dropped successfully!")
            
    except Exception as e:
        logger.error(f"Table dropping failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
