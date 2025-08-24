import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Drop FK from daily_option_snapshot to option_contracts and drop option_contracts."""
    logger.info("Dropping FK constraint(s) from daily_option_snapshot → option_contracts, then dropping option_contracts…")
    conn = db.connect()
    with conn.cursor() as cur:
        # Drop FK constraints that reference option_contracts
        cur.execute(
            """
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = 'daily_option_snapshot'::regclass
                      AND confrelid = 'option_contracts'::regclass
                ) LOOP
                    EXECUTE format('ALTER TABLE daily_option_snapshot DROP CONSTRAINT %I;', r.conname);
                END LOOP;
            END $$;
            """
        )
        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS option_contracts CASCADE;")
        conn.commit()
    logger.info("option_contracts dropped and constraints removed.")


def down():
    """No automatic down migration (requires full table definition)."""
    logger.info("No down migration for dropping option_contracts.")


