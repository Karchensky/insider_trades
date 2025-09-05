"""
Create New Anomaly Detection Table Migration  
Version: 20250127_000002

Creates a new temp_anomaly table optimized for insider trading pattern detection.
This table stores symbol-level anomalies with composite scoring and detailed
pattern information for high-conviction trading opportunities.
"""

import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Create the new temp_anomaly table structure."""
    logger.info("Creating new temp_anomaly table for insider trading detection...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Create the new temp_anomaly table
            logger.info("Creating temp_anomaly table...")
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
            
            # Create indexes for optimal query performance
            logger.info("Creating indexes for temp_anomaly table...")
            
            indexes = [
                # Core lookup indexes
                "CREATE INDEX idx_temp_anomaly_event_date ON temp_anomaly (event_date);",
                "CREATE INDEX idx_temp_anomaly_symbol ON temp_anomaly (symbol);",
                "CREATE INDEX idx_temp_anomaly_score_desc ON temp_anomaly (score DESC);",
                
                # Composite indexes for common queries
                "CREATE INDEX idx_temp_anomaly_date_score ON temp_anomaly (event_date, score DESC);",
                "CREATE INDEX idx_temp_anomaly_symbol_date ON temp_anomaly (symbol, event_date);",
                "CREATE INDEX idx_temp_anomaly_direction_score ON temp_anomaly (direction, score DESC);",
                
                # Specialized indexes
                "CREATE INDEX idx_temp_anomaly_timestamp ON temp_anomaly (as_of_timestamp);",
                "CREATE INDEX idx_temp_anomaly_anomaly_types ON temp_anomaly USING GIN (anomaly_types);",
                "CREATE INDEX idx_temp_anomaly_details ON temp_anomaly USING GIN (details);",
                
                # High-score filtering
                "CREATE INDEX idx_temp_anomaly_high_scores ON temp_anomaly (event_date, score DESC) WHERE score >= 3.0;"
            ]
            
            for idx_sql in indexes:
                cur.execute(idx_sql)
                logger.info(f"Created index: {idx_sql.split()[2]}")
            
            # Create trigger for updated_at
            logger.info("Creating update trigger...")
            cur.execute("""
                CREATE TRIGGER update_temp_anomaly_updated_at
                    BEFORE UPDATE ON temp_anomaly
                    FOR EACH ROW
                    EXECUTE FUNCTION update_updated_at_column();
            """)
            
            # Create a view for easy anomaly analysis
            logger.info("Creating anomaly analysis view...")
            cur.execute("""
                CREATE VIEW anomaly_summary AS
                SELECT 
                    event_date,
                    symbol,
                    direction,
                    score,
                    anomaly_types,
                    total_individual_anomalies,
                    max_individual_score,
                    CASE 
                        WHEN score >= 7.0 THEN 'CRITICAL'
                        WHEN score >= 5.0 THEN 'HIGH'
                        WHEN score >= 3.0 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as risk_level,
                    CASE
                        WHEN 'volume_concentration' = ANY(anomaly_types) THEN true
                        ELSE false
                    END as has_volume_anomaly,
                    CASE
                        WHEN 'directional_bias' = ANY(anomaly_types) THEN true
                        ELSE false
                    END as has_directional_bias,
                    CASE
                        WHEN 'expiration_clustering' = ANY(anomaly_types) THEN true
                        ELSE false
                    END as has_short_term_focus,
                    as_of_timestamp,
                    created_at
                FROM temp_anomaly
                ORDER BY event_date DESC, score DESC;
            """)
            
            # Set up RLS (Row Level Security) policies
            logger.info("Setting up RLS policies...")
            cur.execute("ALTER TABLE temp_anomaly ENABLE ROW LEVEL SECURITY;")
            
            # RLS policies for different user types
            rls_policies = [
                """
                CREATE POLICY temp_anomaly_service_full ON temp_anomaly
                FOR ALL TO service_role USING (true) WITH CHECK (true);
                """,
                """
                CREATE POLICY temp_anomaly_postgres_full ON temp_anomaly
                FOR ALL TO postgres USING (true) WITH CHECK (true);
                """,
                """
                CREATE POLICY temp_anomaly_authenticated_read ON temp_anomaly
                FOR SELECT USING (auth.role() = 'authenticated');
                """
            ]
            
            for policy_sql in rls_policies:
                try:
                    cur.execute(policy_sql)
                except Exception as e:
                    logger.warning(f"RLS policy creation failed: {e}")
            
            # Create a function for cleaning up old anomalies
            logger.info("Creating cleanup function...")
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
            logger.info("New temp_anomaly table created successfully!")
            
    except Exception as e:
        logger.error(f"Failed to create new temp_anomaly table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Drop the new temp_anomaly table and related objects."""
    logger.warning("Dropping new temp_anomaly table...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop the view first
            cur.execute("DROP VIEW IF EXISTS anomaly_summary CASCADE;")
            
            # Drop the cleanup function
            cur.execute("DROP FUNCTION IF EXISTS cleanup_old_anomalies(INTEGER) CASCADE;")
            
            # Drop the table (this will cascade drop all indexes and triggers)
            cur.execute("DROP TABLE IF EXISTS temp_anomaly CASCADE;")
            
            conn.commit()
            logger.info("New temp_anomaly table dropped successfully!")
            
    except Exception as e:
        logger.error(f"Failed to drop new temp_anomaly table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
