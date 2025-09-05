"""
Intraday Snapshots Orchestrator

- Runs temp_stock (full-market stock snapshot)
- Runs temp_option (unified options snapshot)
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
from analysis.insider_anomaly_detection import run_insider_anomaly_detection


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_once(include_otc: bool,
             options_limit: int,
             options_max_pages: Optional[int],
             options_batch_calls: int,
             options_workers: int,
             options_test_contracts: Optional[int] = None) -> None:
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
        latest_date_rows = db.execute_query("SELECT MAX(date) AS d FROM daily_option_snapshot")
        latest_date = latest_date_rows[0]['d'] if latest_date_rows and latest_date_rows[0]['d'] else None
        if not latest_date:
            logger.warning("No latest date found in daily_option_snapshot; skipping options snapshot")
        else:
            count_row = db.execute_query("SELECT COUNT(DISTINCT contract_ticker) AS c FROM daily_option_snapshot WHERE date = %s", (latest_date,))
            total_contracts = count_row[0]['c'] if count_row else 0
            # Use CLI-provided options_limit as the per-request ticker count
            page_size = options_limit or 250
            import math
            total_pages = math.ceil(total_contracts / page_size) if total_contracts else 0
            bulk_calls = options_batch_calls
            total_bulk_loads = math.ceil(total_pages / bulk_calls) if total_pages else 0
            logger.info("[temp_option] Planning: total_contracts=%d, pages(@%d)=%d, bulk_loads(@%d calls)=%d", total_contracts, page_size, total_pages, bulk_calls, total_bulk_loads)

            if total_contracts == 0:
                return

            rows = db.execute_query("SELECT DISTINCT contract_ticker FROM daily_option_snapshot WHERE date = %s ORDER BY contract_ticker", (latest_date,))
            tickers: List[str] = [r['contract_ticker'] for r in rows]
            if options_test_contracts and options_test_contracts > 0:
                tickers = tickers[:options_test_contracts]
            # Build URL-length-aware request batches using ticker.any_of, capped by page_size
            import requests
            try:
                max_url_len = int(os.getenv('POLYGON_MAX_URL_LEN', '7500'))
            except Exception:
                max_url_len = 7500
            base_url = "https://api.polygon.io/v3/snapshot"

            request_batches: List[List[str]] = []
            current: List[str] = []
            for t in tickers:
                candidate = current + [t]
                if len(candidate) > page_size:
                    if current:
                        request_batches.append(current)
                    current = [t]
                    continue
                params = {
                    'type': 'options',
                    'ticker.any_of': ','.join(candidate),
                    'apikey': 'X'
                }
                preq = requests.PreparedRequest()
                preq.prepare_url(base_url, params)
                if len(preq.url) > max_url_len:
                    if current:
                        request_batches.append(current)
                        current = [t]
                    else:
                        request_batches.append([t])
                        current = []
                else:
                    current = candidate
            if current:
                request_batches.append(current)

            # Group into super-batches of N calls (e.g., 100)
            total_loaded = 0
            from concurrent.futures import ThreadPoolExecutor, as_completed
            for start in range(0, len(request_batches), bulk_calls):
                group = request_batches[start:start+bulk_calls]
                logger.info("[temp_option] Fetching super-batch %d/%d (%d calls)", (start//bulk_calls)+1, total_bulk_loads, len(group))
                combined_results = []
                # Track which tickers were requested and returned
                requested_in_group = set(t for batch in group for t in batch)
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
                # Deduplicate across calls by ticker, keep latest by last_updated
                if combined_results:
                    by_ticker = {}
                    for r in combined_results:
                        tk = r.get('ticker')
                        if not tk:
                            continue
                        lu = r.get('last_updated') or 0
                        if tk not in by_ticker or (by_ticker[tk].get('last_updated') or 0) < lu:
                            by_ticker[tk] = r
                    combined_results = list(by_ticker.values())
                # Compute missing tickers and attempt one retry for them
                returned_tickers = {r.get('ticker') for r in combined_results if r.get('ticker')}
                missing = list(requested_in_group - returned_tickers)
                if missing:
                    logger.info("[temp_option] Retrying %d missing tickers in smaller batches", len(missing))
                    # Retry in batches capped at 50 to avoid URL length and pool pressure
                    retry_batches = [missing[i:i+50] for i in range(0, len(missing), 50)]
                    from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
                    with _TPE(max_workers=min(options_workers, 10)) as ex2:
                        futs = {ex2.submit(opt_scraper.fetch_by_tickers, b): b for b in retry_batches}
                        for fut in _ac(futs):
                            try:
                                data = fut.result()
                                res = data.get('results') or []
                                if res:
                                    combined_results.extend(res)
                            except Exception as re:
                                logger.warning("Retry batch failed: %s", re)
                if combined_results:
                    out = loader.bulk_upsert_temp_option_copy({'results': combined_results})
                    if out.get('success'):
                        total_loaded += out['records_processed']
                        logger.info("[temp_option] Super-batch loaded: %s rows (total=%s)", out['records_processed'], total_loaded)
                    else:
                        logger.error("[temp_option] Super-batch load failed: %s", out.get('error'))
            logger.info("[temp_option] Total loaded: %d rows", total_loaded)
            
            # Run insider trading anomaly detection
            logger.info("[anomaly_detection] Starting insider trading anomaly detection...")
            try:
                anomaly_results = run_insider_anomaly_detection(baseline_days=30)
                if anomaly_results.get('success'):
                    logger.info(f"[anomaly_detection] Detected {anomaly_results.get('anomalies_detected', 0)} symbol-level anomalies from {anomaly_results.get('contracts_analyzed', 0)} contracts")
                else:
                    logger.error(f"[anomaly_detection] Detection failed: {anomaly_results.get('error')}")
            except Exception as ee:
                logger.error(f"[anomaly_detection] Detection error: {ee}")
    except Exception as e:
        logger.error(f"Options snapshot error: {e}")


def apply_retention(retention_days: int) -> None:
    manager = DataRetentionManager()
    # temp_stock: use created_at (timestamp) for age
    try:
        res = manager.delete_old_records('temp_stock', 'created_at', retention_days, dry_run=False)
        logger.info("Retention temp_stock: cutoff=%s deleted=%s", res['cutoff_date'], res['records_deleted'])
    except Exception as e:
        logger.error(f"Retention failed for temp_stock: {e}")
    # temp_option: use as_of_timestamp
    try:
        res = manager.delete_old_records('temp_option', 'as_of_timestamp', retention_days, dry_run=False)
        logger.info("Retention temp_option: cutoff=%s deleted=%s", res['cutoff_date'], res['records_deleted'])
    except Exception as e:
        logger.error(f"Retention failed for temp_option: {e}")
    # temp_anomaly: use the built-in cleanup function (7 day retention)
    try:
        from database.connection import db
        conn = db.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT cleanup_old_anomalies(7);")
            deleted_count = cur.fetchone()[0]
            conn.commit()
        logger.info(f"Retention temp_anomaly: cleaned up {deleted_count} old anomaly records (7+ days)")
    except Exception as e:
        logger.error(f"Retention failed for temp_anomaly: {e}")


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
    parser.add_argument('--options-test-contracts', type=int, default=None,
                        help='Optional: limit total number of option contracts (for testing)')
    args = parser.parse_args()

    run_once(include_otc=args.include_otc,
             options_limit=args.options_limit,
             options_max_pages=args.options_max_pages,
             options_batch_calls=args.options_batch_calls,
             options_workers=args.options_workers,
             options_test_contracts=args.options_test_contracts)
    apply_retention(retention_days=args.retention)
    if args.delay_seconds > 0:
        time.sleep(args.delay_seconds)


if __name__ == '__main__':
    main()


