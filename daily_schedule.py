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

    # 3) Populate daily_option_snapshot via flat files (trading days only)
    logger.info("[daily_option_snapshot] Step 2/2: loading from flat files…")
    ff_loader = PolygonOptionFlatFileLoader()
    from datetime import datetime as _dt, timedelta as _td
    s = _dt.strptime(start_date, '%Y-%m-%d').date()
    e = _dt.strptime(end_date, '%Y-%m-%d').date()
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        # Skip non-trading days to avoid 404/empty flat files
        try:
            if not stock_scraper.is_trading_day(cur):
                logger.info(f"[daily_option_snapshot] Skipping {ds} - not a trading day")
                cur += _td(days=1)
                continue
        except Exception:
            pass
        try:
            res = ff_loader.load_for_date(ds, skip_existing=not force)
            logger.info(f"[daily_option_snapshot] {ds} loaded: success={res.get('success')}")
        except Exception as fe:
            logger.error(f"[daily_option_snapshot] {ds} failed: {fe}")
        cur += _td(days=1)

    # Copy latest temp_option rows into full_daily_option_snapshot for the processed dates
    from database.bulk_operations import BulkStockDataLoader as _Loader
    _l = _Loader()
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        try:
            out = _l.upsert_full_daily_option_snapshot_from_temp(ds)
            logger.info(f"[full_daily_option_snapshot] {ds} upserted={out['records_upserted']} in {out['execution_time']:.2f}s")
        except Exception as ce:
            logger.error(f"[full_daily_option_snapshot] {ds} copy failed: {ce}")
        cur += _td(days=1)

    # Capture FINAL anomaly events from temp table to permanent storage
    logger.info("[anomaly_event_capture] Capturing final anomaly events from temp table...")
    try:
        from database.connection import db as _db
        
        # First, get the latest as_of_timestamp for the processed dates to ensure we only capture final states
        latest_timestamp_sql = """
        SELECT MAX(as_of_timestamp) as latest_ts 
        FROM temp_anomaly 
        WHERE event_date BETWEEN %s AND %s
        """
        latest_result = _db.execute_query(latest_timestamp_sql, (s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')))
        latest_timestamp = latest_result[0]['latest_ts'] if latest_result and latest_result[0]['latest_ts'] else None
        
        if latest_timestamp:
            # Only capture anomalies from the final run of the day (latest timestamp)
            # This ensures we get the final state, not every intermediate update
            capture_sql = """
            INSERT INTO full_daily_anomaly_snapshot (event_date, symbol, direction, expiry_date, as_of_timestamp, kind, score, details)
            SELECT DISTINCT ON (event_date, symbol, direction, expiry_date, kind)
                event_date, symbol, direction, expiry_date, as_of_timestamp, kind, score, details
            FROM temp_anomaly
            WHERE event_date BETWEEN %s AND %s
                AND DATE(as_of_timestamp) BETWEEN %s AND %s
            ORDER BY event_date, symbol, direction, expiry_date, kind, as_of_timestamp DESC
            ON CONFLICT (event_date, symbol, direction, expiry_date, kind)
            DO UPDATE SET 
                score = EXCLUDED.score,
                details = EXCLUDED.details,
                as_of_timestamp = EXCLUDED.as_of_timestamp,
                updated_at = CURRENT_TIMESTAMP
            """
            _db.execute_command(capture_sql, (
                s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d'),
                s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')
            ))
            
            # Get count of captured events
            count_sql = """
            SELECT COUNT(*) as count 
            FROM full_daily_anomaly_snapshot 
            WHERE event_date BETWEEN %s AND %s
            """
            count_result = _db.execute_query(count_sql, (s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')))
            captured_count = count_result[0]['count'] if count_result else 0
            
            logger.info(f"[anomaly_event_capture] Captured {captured_count} final anomaly events to permanent storage")
            
            # Clean up temp_anomaly for processed dates (keep only current day for ongoing intraday)
            cleanup_sql = """
            DELETE FROM temp_anomaly 
            WHERE event_date BETWEEN %s AND %s 
                AND event_date < CURRENT_DATE
            """
            _db.execute_command(cleanup_sql, (s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')))
            logger.info("[anomaly_event_capture] Cleaned up processed temp anomaly events")
        else:
            logger.info("[anomaly_event_capture] No anomaly events found for the processed date range")
        
    except Exception as ae:
        logger.error(f"[anomaly_event_capture] Failed to capture anomaly events: {ae}")

    # Truncate temp_option and temp_stock to keep only fresh intraday going forward
    try:
        from database.connection import db as _db
        _db.execute_command("TRUNCATE TABLE temp_option;")
        logger.info("[temp_option] truncated after daily snapshot capture")
        try:
            _db.execute_command("TRUNCATE TABLE temp_stock;")
            logger.info("[temp_stock] truncated after daily snapshot capture")
        except Exception as te2:
            logger.error(f"[temp_stock] truncate failed: {te2}")
    except Exception as te:
        logger.error(f"[temp_option] truncate failed: {te}")

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


