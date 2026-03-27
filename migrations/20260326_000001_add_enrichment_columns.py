"""
Migration: Add Enrichment Columns to daily_anomaly_snapshot
Date: 2026-03-26
Purpose: Persist signal enrichment data (novelty, news, EDGAR, conviction modifier)
         so Streamlit and other consumers can display enrichment context.

Columns added:
- enrichment_novelty_is_first: First-time trigger for this symbol
- enrichment_novelty_count_30d: How many times symbol triggered in last 30 days
- enrichment_novelty_score: 0.0 (frequent) to 1.0 (very novel)
- enrichment_news_has_news: Whether recent news articles exist
- enrichment_news_count: Number of recent articles
- enrichment_news_has_catalyst: Whether catalyst keywords found in news
- enrichment_edgar_has_filings: Whether recent SEC Form 4 filings exist
- enrichment_edgar_filing_count: Number of Form 4 filings
- enrichment_edgar_alignment: 'aligned' or 'contradictory' with anomaly direction
- enrichment_conviction_modifier: Net conviction modifier (-3 to +3)
- enrichment_enriched_at: When enrichment was computed
- enrichment_raw_json: Full enrichment dict for debugging/future use

All columns default to NULL (not yet enriched), distinct from checked-and-found-nothing.
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add enrichment columns to daily_anomaly_snapshot."""
    print("Adding enrichment columns to daily_anomaly_snapshot...")

    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Novelty columns
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_novelty_is_first BOOLEAN
            """)
            print("[OK] Added enrichment_novelty_is_first")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_novelty_count_30d INTEGER
            """)
            print("[OK] Added enrichment_novelty_count_30d")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_novelty_score NUMERIC(5,4)
            """)
            print("[OK] Added enrichment_novelty_score")

            # News columns
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_news_has_news BOOLEAN
            """)
            print("[OK] Added enrichment_news_has_news")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_news_count INTEGER
            """)
            print("[OK] Added enrichment_news_count")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_news_has_catalyst BOOLEAN
            """)
            print("[OK] Added enrichment_news_has_catalyst")

            # EDGAR columns
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_edgar_has_filings BOOLEAN
            """)
            print("[OK] Added enrichment_edgar_has_filings")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_edgar_filing_count INTEGER
            """)
            print("[OK] Added enrichment_edgar_filing_count")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_edgar_alignment VARCHAR(20)
            """)
            print("[OK] Added enrichment_edgar_alignment")

            # Conviction modifier
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_conviction_modifier INTEGER
            """)
            print("[OK] Added enrichment_conviction_modifier")

            # Metadata
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_enriched_at TIMESTAMP WITH TIME ZONE
            """)
            print("[OK] Added enrichment_enriched_at")

            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS enrichment_raw_json JSONB
            """)
            print("[OK] Added enrichment_raw_json")

            # Column comments
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_novelty_is_first IS 'True if this is the first-ever trigger for this symbol'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_novelty_count_30d IS 'Number of times symbol triggered in the last 30 days'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_novelty_score IS 'Novelty score 0.0 (frequent/common) to 1.0 (very novel/first-time)'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_news_has_news IS 'Whether recent news articles exist for this symbol around event date'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_news_count IS 'Number of recent news articles found'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_news_has_catalyst IS 'Whether catalyst keywords (FDA, merger, earnings, etc.) found in news'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_edgar_has_filings IS 'Whether recent SEC Form 4 insider filings exist'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_edgar_filing_count IS 'Number of Form 4 filings in the lookback window'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_edgar_alignment IS 'Whether insider filing direction aligns with anomaly: aligned, contradictory, or NULL'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_conviction_modifier IS 'Net conviction modifier from enrichment sources (-3 to +3)'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_enriched_at IS 'Timestamp when enrichment was computed for this row'""")
            cur.execute("""COMMENT ON COLUMN daily_anomaly_snapshot.enrichment_raw_json IS 'Full enrichment result dict as JSONB for debugging and forward compatibility'""")
            print("[OK] Added column comments")

            conn.commit()
            print("[OK] Migration completed - enrichment columns added")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def down():
    """Remove enrichment columns."""
    print("Rolling back - removing enrichment columns...")

    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                DROP COLUMN IF EXISTS enrichment_novelty_is_first,
                DROP COLUMN IF EXISTS enrichment_novelty_count_30d,
                DROP COLUMN IF EXISTS enrichment_novelty_score,
                DROP COLUMN IF EXISTS enrichment_news_has_news,
                DROP COLUMN IF EXISTS enrichment_news_count,
                DROP COLUMN IF EXISTS enrichment_news_has_catalyst,
                DROP COLUMN IF EXISTS enrichment_edgar_has_filings,
                DROP COLUMN IF EXISTS enrichment_edgar_filing_count,
                DROP COLUMN IF EXISTS enrichment_edgar_alignment,
                DROP COLUMN IF EXISTS enrichment_conviction_modifier,
                DROP COLUMN IF EXISTS enrichment_enriched_at,
                DROP COLUMN IF EXISTS enrichment_raw_json
            """)
            conn.commit()
            print("[OK] Rollback completed")
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--down', action='store_true')
    args = parser.parse_args()

    if args.down:
        down()
    else:
        up()
