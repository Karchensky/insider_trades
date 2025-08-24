"""
Daily Orchestrator

Populates:
- daily_stock_snapshot (stocks OHLC)
- option_contracts (contracts metadata)
- daily_option_snapshot (option OHLC)

Usage example:
  python daily_schedule.py --recent 3 --retention 60

Options for quick/local verification:
  --ticker-limit N (limit number of underlying symbols for contracts step)
  --contract-limit N (limit number of contracts for snapshots step)
  --dry-run-retention (show retention stats without deleting)
"""

import os
import sys
import logging
import argparse
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scrapers.polygon_daily_scraper import PolygonDailyScraper
from scrapers.polygon_option_flatfile_loader import PolygonOptionFlatFileLoader
from maintenance.data_retention import DataRetentionManager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_daily_pipeline(recent_days: int, retention_days: int, include_otc: bool,
                       force: bool, ticker_limit: int | None, contract_limit: int | None,
                       dry_run_retention: bool) -> int:
    start_ts = datetime.now()

    # 1) Determine recent trading day range
    stock_scraper = PolygonDailyScraper()
    start_date, end_date = stock_scraper.get_recent_trading_days(recent_days) if recent_days > 1 else (
        stock_scraper.get_most_recent_trading_day(), stock_scraper.get_most_recent_trading_day()
    )
    logger.info(f"Processing trading days: {start_date} → {end_date}")

    # 2) Populate daily_stock_snapshot
    logger.info("[daily_stock_snapshot] Step 1/2: loading…")
    stock_results = stock_scraper.scrape_date_range(
        start_date, end_date, include_otc=include_otc, skip_existing=not force
    )
    logger.info(f"[daily_stock_snapshot] inserted={stock_results['total_records_inserted']:,} dates_ok={stock_results['successful_scrapes']} skipped={stock_results['skipped_dates']}")

    # 3) Populate daily_option_snapshot via flat files
    logger.info("[daily_option_snapshot] Step 2/2: loading from flat files…")
    ff_loader = PolygonOptionFlatFileLoader()
    from datetime import datetime as _dt, timedelta as _td
    s = _dt.strptime(start_date, '%Y-%m-%d').date()
    e = _dt.strptime(end_date, '%Y-%m-%d').date()
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        try:
            res = ff_loader.load_for_date(ds, skip_existing=not force)
            logger.info(f"[daily_option_snapshot] {ds} loaded: success={res.get('success')}")
        except Exception as fe:
            logger.error(f"[daily_option_snapshot] {ds} failed: {fe}")
        cur += _td(days=1)

    # Copy latest temp_option_snapshot rows into daily_option_snapshot_full for the processed dates
    from database.bulk_operations import BulkStockDataLoader as _Loader
    _l = _Loader()
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        try:
            out = _l.upsert_daily_option_snapshot_full_from_temp(ds)
            logger.info(f"[daily_option_snapshot_full] {ds} upserted={out['records_upserted']} in {out['execution_time']:.2f}s")
        except Exception as ce:
            logger.error(f"[daily_option_snapshot_full] {ds} copy failed: {ce}")
        cur += _td(days=1)

    # Truncate temp_option_snapshot and temp_stock_snapshot to keep only fresh intraday going forward
    try:
        from database.connection import db as _db
        _db.execute_command("TRUNCATE TABLE temp_option_snapshot;")
        logger.info("[temp_option_snapshot] truncated after daily snapshot capture")
        try:
            _db.execute_command("TRUNCATE TABLE temp_stock_snapshot;")
            logger.info("[temp_stock_snapshot] truncated after daily snapshot capture")
        except Exception as te2:
            logger.error(f"[temp_stock_snapshot] truncate failed: {te2}")
    except Exception as te:
        logger.error(f"[temp_option_snapshot] truncate failed: {te}")

    # 5) Retention cleanup for core tables
    logger.info("Applying retention policy…")
    retention = DataRetentionManager()
    for table, date_col in (
        ('daily_stock_snapshot', 'date'),
        ('daily_option_snapshot', 'date'),
    ):
        try:
            res = retention.delete_old_records(
                table, date_col, retention_days, dry_run=dry_run_retention
            )
            logger.info(f"Retention {table}: cutoff={res['cutoff_date']} identified={res['records_identified']:,} deleted={res['records_deleted']:,}")
        except Exception as e:
            logger.error(f"Retention failed for {table}: {e}")

    elapsed = (datetime.now() - start_ts).total_seconds()
    logger.info(f"Pipeline finished in {elapsed:.1f}s")
    return 0


def main():
    parser = argparse.ArgumentParser(description='Daily ETL Orchestrator')
    parser.add_argument('--recent', type=int, default=int(os.getenv('RECENT_DAYS', '3')), help='Number of recent business days to load (default: 3)')
    parser.add_argument('--retention', type=int, default=int(os.getenv('RETENTION_DAYS', '30')), help='Retention in business days (default: 30)')
    parser.add_argument('--include-otc', action='store_true', help='Include OTC for stocks step (default: false)')
    parser.add_argument('--force', action='store_true', help='Force re-scrape even if data exists')
    parser.add_argument('--ticker-limit', type=int, help='Limit underlying tickers for contracts step (testing)')
    parser.add_argument('--contract-limit', type=int, help='Limit contracts for snapshots step (testing)')
    parser.add_argument('--dry-run-retention', action='store_true', help='Do not delete rows; report only')

    args = parser.parse_args()

    try:
        sys.exit(run_daily_pipeline(
            recent_days=args.recent,
            retention_days=args.retention,
            include_otc=args.include_otc,
            force=args.force,
            ticker_limit=args.ticker_limit,
            contract_limit=args.contract_limit,
            dry_run_retention=args.dry_run_retention,
        ))
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


