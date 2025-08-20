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
from typing import Optional, List

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.polygon_full_market_snapshot_scraper import FullMarketSnapshotScraper
from scrapers.polygon_unified_options_snapshot_scraper import UnifiedOptionsSnapshotScraper
from database.bulk_operations import BulkStockDataLoader
from maintenance.data_retention import DataRetentionManager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_once(include_otc: bool,
             options_limit: int,
             options_max_pages: Optional[int],
             options_batch_calls: int,
             options_workers: int) -> None:
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

    # Options: high-throughput concurrent by explicit tickers (fast path)
    try:
        opt_scraper = UnifiedOptionsSnapshotScraper()
        from database.connection import db
        latest_date_rows = db.execute_query("SELECT MAX(date) AS d FROM option_contracts")
        latest_date = latest_date_rows[0]['d'] if latest_date_rows and latest_date_rows[0]['d'] else None
        if not latest_date:
            logger.warning("No latest date found in option_contracts; skipping options snapshot")
        else:
            count_row = db.execute_query("SELECT COUNT(*) AS c FROM option_contracts WHERE date = %s", (latest_date,))
            total_contracts = count_row[0]['c'] if count_row else 0
            page_size = 250
            import math
            total_pages = math.ceil(total_contracts / page_size) if total_contracts else 0
            bulk_calls = options_batch_calls
            total_bulk_loads = math.ceil(total_pages / bulk_calls) if total_pages else 0
            logger.info("[temp_option_snapshot] Planning: total_contracts=%d, pages(@250)=%d, bulk_loads(@%d calls)=%d", total_contracts, total_pages, bulk_calls, total_bulk_loads)

            if total_contracts == 0:
                return

            rows = db.execute_query("SELECT contract_ticker FROM option_contracts WHERE date = %s ORDER BY contract_ticker", (latest_date,))
            tickers: List[str] = [r['contract_ticker'] for r in rows]
            # Build list of 250-ticker request batches
            request_batches = [tickers[i:i+page_size] for i in range(0, len(tickers), page_size)]

            # Group into super-batches of N calls (e.g., 100)
            total_loaded = 0
            from concurrent.futures import ThreadPoolExecutor, as_completed
            for start in range(0, len(request_batches), bulk_calls):
                group = request_batches[start:start+bulk_calls]
                logger.info("[temp_option_snapshot] Fetching super-batch %d/%d (%d calls)", (start//bulk_calls)+1, total_bulk_loads, len(group))
                combined_results = []
                with ThreadPoolExecutor(max_workers=options_workers) as ex:
                    future_map = {ex.submit(opt_scraper.fetch_by_tickers, batch): idx for idx, batch in enumerate(group)}
                    for fut in as_completed(future_map):
                        try:
                            data = fut.result()
                            res = data.get('results') or []
                            if res:
                                combined_results.extend(res)
                        except Exception as fe:
                            logger.error("fetch_by_tickers failed in super-batch at request %d: %s", future_map[fut], fe)
                if combined_results:
                    out = loader.bulk_upsert_temp_option_snapshot_copy({'results': combined_results})
                    if out.get('success'):
                        total_loaded += out['records_processed']
                        logger.info("[temp_option_snapshot] Super-batch loaded: %s rows (total=%s)", out['records_processed'], total_loaded)
                    else:
                        logger.error("[temp_option_snapshot] Super-batch load failed: %s", out.get('error'))
            logger.info("[temp_option_snapshot] Total loaded: %d rows", total_loaded)
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
    parser.add_argument('--options-batch-calls', type=int, default=int(os.getenv('INTRADAY_OPTIONS_BATCH_CALLS', '100')),
                        help='Number of concurrent API calls to combine per bulk load (default: 100)')
    parser.add_argument('--options-workers', type=int, default=int(os.getenv('INTRADAY_OPTIONS_WORKERS', '20')),
                        help='Max concurrent workers for options calls (default: 20)')
    args = parser.parse_args()

    run_once(include_otc=args.include_otc,
             options_limit=args.options_limit,
             options_max_pages=args.options_max_pages,
             options_batch_calls=args.options_batch_calls,
             options_workers=args.options_workers)
    apply_retention(retention_days=args.retention)
    if args.delay_seconds > 0:
        time.sleep(args.delay_seconds)


if __name__ == '__main__':
    main()


