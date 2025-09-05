"""
Rename temp_anomaly to daily_anomaly_snapshot
Version: 20250905_000004

Renames temp_anomaly table to daily_anomaly_snapshot to better reflect its purpose
as a persistent storage for daily anomaly snapshots rather than temporary data.
"""

import logging
from database.core.connection import db

logger = logging.getLogger(__name__)

def up():
    """Rename temp_anomaly table to daily_anomaly_snapshot."""
    logger.info("Renaming temp_anomaly table to daily_anomaly_snapshot...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # Rename the table
            logger.info("Renaming table...")
            cur.execute("ALTER TABLE temp_anomaly RENAME TO daily_anomaly_snapshot;")
            
            # Update all indexes to reflect new table name
            logger.info("Renaming indexes...")
            index_renames = [
                ("idx_temp_anomaly_symbol", "idx_daily_anomaly_snapshot_symbol"),
                ("idx_temp_anomaly_date", "idx_daily_anomaly_snapshot_date"),
                ("idx_temp_anomaly_score", "idx_daily_anomaly_snapshot_score"),
                ("idx_temp_anomaly_timestamp", "idx_daily_anomaly_snapshot_timestamp"),
                ("idx_temp_anomaly_direction", "idx_daily_anomaly_snapshot_direction"),
                ("idx_temp_anomaly_volume_score", "idx_daily_anomaly_snapshot_volume_score"),
                ("idx_temp_anomaly_multiplier", "idx_daily_anomaly_snapshot_multiplier"),
                ("idx_temp_anomaly_date_score", "idx_daily_anomaly_snapshot_date_score"),
                ("idx_temp_anomaly_symbol_date", "idx_daily_anomaly_snapshot_symbol_date")
            ]
            
            for old_name, new_name in index_renames:
                try:
                    cur.execute(f"ALTER INDEX {old_name} RENAME TO {new_name};")
                    logger.info(f"Renamed index: {old_name} -> {new_name}")
                except Exception as e:
                    logger.warning(f"Could not rename index {old_name}: {e}")
            
            # Update constraint name
            logger.info("Renaming constraints...")
            try:
                cur.execute("ALTER TABLE daily_anomaly_snapshot RENAME CONSTRAINT temp_anomaly_unique_symbol_date TO daily_anomaly_snapshot_unique_symbol_date;")
                logger.info("Renamed unique constraint")
            except Exception as e:
                logger.warning(f"Could not rename constraint: {e}")
            
            # Update trigger name
            logger.info("Renaming triggers...")
            try:
                cur.execute("DROP TRIGGER IF EXISTS update_temp_anomaly_updated_at ON daily_anomaly_snapshot;")
                cur.execute("""
                    CREATE TRIGGER update_daily_anomaly_snapshot_updated_at 
                    BEFORE UPDATE ON daily_anomaly_snapshot 
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                """)
                logger.info("Updated trigger name")
            except Exception as e:
                logger.warning(f"Could not update trigger: {e}")
            
            # Update RLS policies
            logger.info("Updating RLS policies...")
            try:
                cur.execute("DROP POLICY IF EXISTS temp_anomaly_policy_authenticated ON daily_anomaly_snapshot;")
                cur.execute("DROP POLICY IF EXISTS temp_anomaly_policy_service_role ON daily_anomaly_snapshot;")
                cur.execute("DROP POLICY IF EXISTS temp_anomaly_policy_postgres ON daily_anomaly_snapshot;")
                
                policies = [
                    """
                    CREATE POLICY daily_anomaly_snapshot_policy_authenticated 
                    ON daily_anomaly_snapshot FOR ALL 
                    TO authenticated 
                    USING (true) WITH CHECK (true);
                    """,
                    """
                    CREATE POLICY daily_anomaly_snapshot_policy_service_role 
                    ON daily_anomaly_snapshot FOR ALL 
                    TO service_role 
                    USING (true) WITH CHECK (true);
                    """,
                    """
                    CREATE POLICY daily_anomaly_snapshot_policy_postgres 
                    ON daily_anomaly_snapshot FOR ALL 
                    TO postgres 
                    USING (true) WITH CHECK (true);
                    """
                ]
                
                for policy_sql in policies:
                    cur.execute(policy_sql)
                
                logger.info("Updated RLS policies")
            except Exception as e:
                logger.warning(f"Could not update RLS policies: {e}")
            
            conn.commit()
            logger.info("Successfully renamed temp_anomaly to daily_anomaly_snapshot!")
            
    except Exception as e:
        logger.error(f"Failed to rename table: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

def down():
    """Rollback: Rename daily_anomaly_snapshot back to temp_anomaly."""
    logger.warning("Rolling back table rename...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            
            # Rename the table back
            cur.execute("ALTER TABLE daily_anomaly_snapshot RENAME TO temp_anomaly;")
            
            # Rename indexes back
            index_renames = [
                ("idx_daily_anomaly_snapshot_symbol", "idx_temp_anomaly_symbol"),
                ("idx_daily_anomaly_snapshot_date", "idx_temp_anomaly_date"),
                ("idx_daily_anomaly_snapshot_score", "idx_temp_anomaly_score"),
                ("idx_daily_anomaly_snapshot_timestamp", "idx_temp_anomaly_timestamp"),
                ("idx_daily_anomaly_snapshot_direction", "idx_temp_anomaly_direction"),
                ("idx_daily_anomaly_snapshot_volume_score", "idx_temp_anomaly_volume_score"),
                ("idx_daily_anomaly_snapshot_multiplier", "idx_temp_anomaly_multiplier"),
                ("idx_daily_anomaly_snapshot_date_score", "idx_temp_anomaly_date_score"),
                ("idx_daily_anomaly_snapshot_symbol_date", "idx_temp_anomaly_symbol_date")
            ]
            
            for old_name, new_name in index_renames:
                try:
                    cur.execute(f"ALTER INDEX {old_name} RENAME TO {new_name};")
                except Exception as e:
                    logger.warning(f"Could not rename index {old_name}: {e}")
            
            # Rename constraint back
            try:
                cur.execute("ALTER TABLE temp_anomaly RENAME CONSTRAINT daily_anomaly_snapshot_unique_symbol_date TO temp_anomaly_unique_symbol_date;")
            except Exception as e:
                logger.warning(f"Could not rename constraint: {e}")
            
            # Update trigger back
            try:
                cur.execute("DROP TRIGGER IF EXISTS update_daily_anomaly_snapshot_updated_at ON temp_anomaly;")
                cur.execute("""
                    CREATE TRIGGER update_temp_anomaly_updated_at 
                    BEFORE UPDATE ON temp_anomaly 
                    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                """)
            except Exception as e:
                logger.warning(f"Could not update trigger: {e}")
            
            conn.commit()
            logger.info("Table rename rollback completed!")
            
    except Exception as e:
        logger.error(f"Failed to rollback table rename: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    up()
