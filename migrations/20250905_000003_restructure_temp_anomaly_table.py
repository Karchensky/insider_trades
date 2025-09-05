"""
Restructure temp_anomaly Table for Enhanced Anomaly Details
Version: 20250905_000003

Replaces temp_anomaly table with comprehensive scoring details and baseline comparisons.
Captures all score components, baseline data, and multipliers for better analysis.
"""

import logging
from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Restructure temp_anomaly table with enhanced anomaly details."""
    logger.info("Restructuring temp_anomaly table for enhanced anomaly details...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # Drop the old temp_anomaly table entirely (clean slate as requested)
            logger.info("Dropping existing temp_anomaly table...")
            cur.execute("DROP TABLE IF EXISTS temp_anomaly CASCADE;")
            
            # Drop the anomaly_summary view (no longer necessary)
            logger.info("Dropping anomaly_summary view...")
            cur.execute("DROP VIEW IF EXISTS anomaly_summary CASCADE;")
            
            # Create the new enhanced temp_anomaly table
            logger.info("Creating enhanced temp_anomaly table...")
            cur.execute("""
                CREATE TABLE temp_anomaly (
                    id BIGSERIAL PRIMARY KEY,
                    event_date DATE NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    
                    -- Composite Score (1-10 scale)
                    total_score DECIMAL(4,1) NOT NULL,
                    
                    -- Individual Score Components (matching email format)
                    volume_score DECIMAL(3,1) NOT NULL,
                    otm_score DECIMAL(3,1) NOT NULL, 
                    directional_score DECIMAL(3,1) NOT NULL,
                    time_score DECIMAL(3,1) NOT NULL,
                    
                    -- Trading Activity Data
                    call_volume BIGINT NOT NULL DEFAULT 0,
                    put_volume BIGINT NOT NULL DEFAULT 0,
                    total_volume BIGINT NOT NULL DEFAULT 0,
                    
                    -- Baseline Comparison Data
                    call_baseline_avg DECIMAL(12,2) DEFAULT 0,
                    put_baseline_avg DECIMAL(12,2) DEFAULT 0,
                    call_multiplier DECIMAL(8,2) DEFAULT 0,
                    put_multiplier DECIMAL(8,2) DEFAULT 0,
                    
                    -- Pattern Analysis
                    direction VARCHAR(20) NOT NULL DEFAULT 'mixed',
                    pattern_description TEXT,
                    z_score DECIMAL(8,2),
                    
                    -- Additional Metrics
                    otm_call_percentage DECIMAL(5,2),
                    short_term_percentage DECIMAL(5,2),
                    call_put_ratio DECIMAL(8,4),
                    
                    -- Timestamps
                    as_of_timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    
                    -- Constraints
                    CONSTRAINT temp_anomaly_unique_symbol_date UNIQUE (event_date, symbol)
                );
            """)
            
            # Create indexes for optimal query performance
            logger.info("Creating indexes for enhanced temp_anomaly table...")
            
            indexes = [
                # Core lookup indexes
                "CREATE INDEX idx_temp_anomaly_symbol ON temp_anomaly (symbol);",
                "CREATE INDEX idx_temp_anomaly_date ON temp_anomaly (event_date);",
                "CREATE INDEX idx_temp_anomaly_score ON temp_anomaly (total_score DESC);",
                "CREATE INDEX idx_temp_anomaly_timestamp ON temp_anomaly (as_of_timestamp DESC);",
                
                # Analysis indexes
                "CREATE INDEX idx_temp_anomaly_direction ON temp_anomaly (direction);",
                "CREATE INDEX idx_temp_anomaly_volume_score ON temp_anomaly (volume_score DESC);",
                "CREATE INDEX idx_temp_anomaly_multiplier ON temp_anomaly (call_multiplier DESC);",
                
                # Composite indexes for common queries
                "CREATE INDEX idx_temp_anomaly_date_score ON temp_anomaly (event_date, total_score DESC);",
                "CREATE INDEX idx_temp_anomaly_symbol_date ON temp_anomaly (symbol, event_date);"
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
                logger.info(f"Created index: {idx_sql.split()[2]}")
            
            # Create trigger for updated_at
            logger.info("Creating update trigger...")
            cur.execute("""
                CREATE TRIGGER update_temp_anomaly_updated_at 
                BEFORE UPDATE ON temp_anomaly 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """)
            
            # Enable RLS for security
            logger.info("Enabling RLS on temp_anomaly...")
            cur.execute("ALTER TABLE temp_anomaly ENABLE ROW LEVEL SECURITY;")
            
            # Create RLS policies
            policies = [
                """
                CREATE POLICY temp_anomaly_policy_authenticated 
                ON temp_anomaly FOR ALL 
                TO authenticated 
                USING (true) WITH CHECK (true);
                """,
                """
                CREATE POLICY temp_anomaly_policy_service_role 
                ON temp_anomaly FOR ALL 
                TO service_role 
                USING (true) WITH CHECK (true);
                """,
                """
                CREATE POLICY temp_anomaly_policy_postgres 
                ON temp_anomaly FOR ALL 
                TO postgres 
                USING (true) WITH CHECK (true);
                """
            ]
            
            for policy_sql in policies:
                try:
                    cur.execute(policy_sql)
                except Exception as e:
                    logger.warning(f"RLS policy creation failed: {e}")
            
            conn.commit()
            logger.info("Enhanced temp_anomaly table created successfully!")
            
    except Exception as e:
        logger.error(f"Failed to restructure temp_anomaly table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Rollback the temp_anomaly table restructuring."""
    logger.warning("Rolling back temp_anomaly table restructuring...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # Drop the enhanced table
            cur.execute("DROP TABLE IF EXISTS temp_anomaly CASCADE;")
            
            # Recreate the original temp_anomaly table structure
            logger.info("Recreating original temp_anomaly table...")
            cur.execute("""
                CREATE TABLE temp_anomaly (
                    id BIGSERIAL PRIMARY KEY,
                    event_date DATE NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    direction VARCHAR(10) DEFAULT 'mixed',
                    score DECIMAL(12,4) NOT NULL,
                    anomaly_types TEXT[] NOT NULL,
                    total_individual_anomalies INTEGER DEFAULT 0,
                    max_individual_score DECIMAL(12,4) DEFAULT 0,
                    details JSONB,
                    as_of_timestamp TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                    
                    CONSTRAINT temp_anomaly_unique_symbol_date UNIQUE (event_date, symbol)
                );
            """)
            
            # Recreate basic indexes
            cur.execute("CREATE INDEX idx_temp_anomaly_symbol ON temp_anomaly (symbol);")
            cur.execute("CREATE INDEX idx_temp_anomaly_date ON temp_anomaly (event_date);")
            cur.execute("CREATE INDEX idx_temp_anomaly_score ON temp_anomaly (score DESC);")
            
            # Recreate trigger
            cur.execute("""
                CREATE TRIGGER update_temp_anomaly_updated_at 
                BEFORE UPDATE ON temp_anomaly 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """)
            
            conn.commit()
            logger.info("Original temp_anomaly table structure restored!")
            
    except Exception as e:
        logger.error(f"Failed to rollback temp_anomaly restructuring: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    up()
