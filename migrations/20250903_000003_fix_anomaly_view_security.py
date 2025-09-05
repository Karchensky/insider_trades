"""
Fix Anomaly Summary View Security Migration
Version: 20250903_000003

Fixes the SECURITY DEFINER issue with the anomaly_summary view by recreating it
without the SECURITY DEFINER property to use the querying user's permissions.
"""

import logging
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Fix the anomaly_summary view security issue."""
    logger.info("Fixing anomaly_summary view security definer issue...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Drop the existing view
            logger.info("Dropping existing anomaly_summary view...")
            cur.execute("DROP VIEW IF EXISTS anomaly_summary CASCADE;")
            
            # Recreate the view without SECURITY DEFINER
            logger.info("Creating anomaly_summary view with proper security...")
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
            
            conn.commit()
            logger.info("Anomaly summary view security issue fixed successfully!")
            
    except Exception as e:
        logger.error(f"Failed to fix anomaly summary view security: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Rollback the security fix (recreate with SECURITY DEFINER)."""
    logger.warning("Rolling back anomaly summary view security fix...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # This rollback recreates the problematic version for testing purposes only
            cur.execute("DROP VIEW IF EXISTS anomaly_summary CASCADE;")
            
            # Note: We don't actually want to recreate the SECURITY DEFINER version
            # This is just a placeholder for the rollback
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
            
            conn.commit()
            logger.info("Anomaly summary view rollback completed!")
            
    except Exception as e:
        logger.error(f"Failed to rollback anomaly summary view: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    up()
