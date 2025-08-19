"""
Intraday Snapshots Orchestrator

- Runs temp_stock_snapshot (full-market stock snapshot)
- Runs temp_option_snapshot (unified options snapshot)
- Applies retention for both temp tables

Usage:
  python intraday_schedule.py --retention 1 --delay-seconds 0

Notes:
- No --recent; these are current snapshots
- Default retention = 1 business day
"""

import os
import sys
import logging
import argparse
import time
from typing import Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.polygon_full_market_snapshot_scraper import FullMarketSnapshotScraper
from scrapers.polygon_unified_options_snapshot_scraper import UnifiedOptionsSnapshotScraper
from database.bulk_operations import BulkStockDataLoader
from maintenance.data_retention import DataRetentionManager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_once(include_otc: bool, options_limit: int, options_max_pages: Optional[int]) -> None:
    loader = BulkStockDataLoader()

    # Stocks: full market snapshot (one call)
    try:
        stock_scraper = FullMarketSnapshotScraper()
        stock_snapshot = stock_scraper.fetch_full_market_snapshot(include_otc=include_otc)
        if stock_snapshot:
            out = loader.bulk_upsert_temp_snapshots_copy(stock_snapshot)
            if out.get('success'):
                logger.info("Stocks snapshot loaded: %s rows", out['records_processed'])
            else:
                logger.error("Stocks snapshot failed: %s", out.get('error'))
        else:
            logger.warning("No stock snapshot data")
    except Exception as e:
        logger.error(f"Stocks snapshot error: {e}")

    # Options: unified snapshot (page through)
    try:
        opt_scraper = UnifiedOptionsSnapshotScraper()
        total = 0
        # Strategy: paginate all (no tickers param) OR fall back to batching tickers by 200 → 100
        try:
            next_url: Optional[str] = None
            pages = 0
            while True:
                data = opt_scraper.fetch_page(ticker=None, limit=options_limit, sort=None, order=None, next_url=next_url)
                results = data.get('results') or []
                if not results:
                    break
                out = loader.bulk_upsert_temp_option_snapshot_copy({'results': results})
                if out.get('success'):
                    total += out['records_processed']
                    logger.info("Options snapshot page %d loaded: %s rows", pages + 1, out['records_processed'])
                else:
                    logger.error("Options snapshot page %d failed: %s", pages + 1, out.get('error'))
                    break
                pages += 1
                next_url = data.get('next_url')
                if not next_url:
                    break
                if options_max_pages is not None and pages >= options_max_pages:
                    break
        except Exception as e:
            logger.error(f"Unified snapshot pagination failed, falling back to ticker batching: {e}")
            # Ensure DB connection is clean after error
            try:
                from database.connection import db as _db
                _db.connect().rollback()
            except Exception:
                pass
            # Fetch latest contracts from DB and batch-request via 'ticker' param
            from database.connection import db
            latest_date_rows = db.execute_query("SELECT MAX(date) AS d FROM option_contracts")
            latest_date = latest_date_rows[0]['d'] if latest_date_rows and latest_date_rows[0]['d'] else None
            if not latest_date:
                logger.warning("No latest date found in option_contracts; skipping options snapshot")
            else:
                rows = db.execute_query("SELECT contract_ticker FROM option_contracts WHERE date = %s", (latest_date,))
                tickers = [r['contract_ticker'] for r in rows]
                logger.info("Batching %d contract tickers for unified snapshot…", len(tickers))
                for size in (200, 100):
                    idx = 0
                    loaded = 0
                    while idx < len(tickers):
                        batch = tickers[idx:idx+size]
                        try:
                            data = opt_scraper.fetch_by_tickers(batch)
                            results = data.get('results') or []
                            if results:
                                out = loader.bulk_upsert_temp_option_snapshot_copy({'results': results})
                                if out.get('success'):
                                    loaded += out['records_processed']
                                else:
                                    logger.error("Ticker batch upsert failed at idx=%d size=%d: %s", idx, size, out.get('error'))
                                    break
                        except Exception as be:
                            logger.error("Ticker batch request failed at idx=%d size=%d: %s", idx, size, be)
                            break
                        idx += size
                    logger.info("Loaded %d rows via ticker batches of size %d", loaded, size)
                    if loaded > 0:
                        total += loaded
                        break
        logger.info("Options snapshot total loaded: %d rows", total)
    except Exception as e:
        logger.error(f"Options snapshot error: {e}")


def apply_retention(retention_days: int) -> None:
    manager = DataRetentionManager()
    # temp_stock_snapshot: use created_at (timestamp) for age
    try:
        res = manager.delete_old_records('temp_stock_snapshot', 'created_at', retention_days, dry_run=False)
        logger.info("Retention temp_stock_snapshot: cutoff=%s deleted=%s", res['cutoff_date'], res['records_deleted'])
    except Exception as e:
        logger.error(f"Retention failed for temp_stock_snapshot: {e}")
    # temp_option_snapshot: use as_of_timestamp
    try:
        res = manager.delete_old_records('temp_option_snapshot', 'as_of_timestamp', retention_days, dry_run=False)
        logger.info("Retention temp_option_snapshot: cutoff=%s deleted=%s", res['cutoff_date'], res['records_deleted'])
    except Exception as e:
        logger.error(f"Retention failed for temp_option_snapshot: {e}")


def main():
    parser = argparse.ArgumentParser(description='Intraday snapshots runner (single iteration)')
    parser.add_argument('--retention', type=int, default=int(os.getenv('INTRADAY_RETENTION_DAYS', '1')),
                        help='Retention in business days for temp tables (default: 1)')
    parser.add_argument('--include-otc', action='store_true', help='Include OTC in stocks snapshot')
    parser.add_argument('--options-limit', type=int, default=int(os.getenv('INTRADAY_OPTIONS_LIMIT', '250')),
                        help='Unified snapshot page size (default: 250)')
    parser.add_argument('--options-max-pages', type=int, default=None,
                        help='Max pages to pull for options per iteration (default: unlimited)')
    parser.add_argument('--delay-seconds', type=int, default=0, help='Optional sleep after completion')
    args = parser.parse_args()

    run_once(include_otc=args.include_otc, options_limit=args.options_limit, options_max_pages=args.options_max_pages)
    apply_retention(retention_days=args.retention)
    if args.delay_seconds > 0:
        time.sleep(args.delay_seconds)


if __name__ == '__main__':
    main()


