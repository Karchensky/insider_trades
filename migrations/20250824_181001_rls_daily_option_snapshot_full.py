import logging
from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Enable RLS and add policies for daily_option_snapshot_full."""
    logger.info("Enabling RLS and creating policies on daily_option_snapshot_full…")
    sql_statements = [
        # Enable RLS
        "ALTER TABLE daily_option_snapshot_full ENABLE ROW LEVEL SECURITY;",
        # Policies with duplicate-safe blocks
        """
        DO $$ BEGIN
            CREATE POLICY "dosf_select_authenticated" ON daily_option_snapshot_full
            FOR SELECT USING (auth.role() = 'authenticated');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
        """
        DO $$ BEGIN
            CREATE POLICY "dosf_insert_authenticated" ON daily_option_snapshot_full
            FOR INSERT WITH CHECK (auth.role() = 'authenticated');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
        """
        DO $$ BEGIN
            CREATE POLICY "dosf_update_authenticated" ON daily_option_snapshot_full
            FOR UPDATE USING (auth.role() = 'authenticated') WITH CHECK (auth.role() = 'authenticated');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
        """
        DO $$ BEGIN
            CREATE POLICY "dosf_service_role_all" ON daily_option_snapshot_full
            FOR ALL USING ((auth.jwt() ->> 'role') = 'service_role');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """,
    ]
    with db.connect().cursor() as cur:
        for stmt in sql_statements:
            cur.execute(stmt)
        cur.connection.commit()
    logger.info("RLS enabled and policies ensured for daily_option_snapshot_full.")


def down():
    logger.info("Disabling RLS and dropping policies on daily_option_snapshot_full…")
    with db.connect().cursor() as cur:
        cur.execute("""
            DO $$ BEGIN
                DROP POLICY IF EXISTS "dosf_select_authenticated" ON daily_option_snapshot_full;
                DROP POLICY IF EXISTS "dosf_insert_authenticated" ON daily_option_snapshot_full;
                DROP POLICY IF EXISTS "dosf_update_authenticated" ON daily_option_snapshot_full;
                DROP POLICY IF EXISTS "dosf_service_role_all" ON daily_option_snapshot_full;
            END $$;
        """)
        cur.execute("ALTER TABLE daily_option_snapshot_full DISABLE ROW LEVEL SECURITY;")
        cur.connection.commit()
    logger.info("RLS disabled and policies dropped for daily_option_snapshot_full.")


