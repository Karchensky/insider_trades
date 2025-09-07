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
from scrapers.polygon_option_contracts_scraper import PolygonOptionContractsScraper
from database.maintenance.data_retention import DataRetentionManager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_daily_pipeline(recent_days: int, retention_days: int, include_otc: bool,
                       force: bool, ticker_limit: int | None, contract_limit: int | None,
                       dry_run_retention: bool, anomaly_retention: int = 30) -> int:
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

    # 4) Populate option_contracts (contract metadata)
    logger.info("[option_contracts] Step 3/3: loading contract metadata...")
    try:
        contracts_scraper = PolygonOptionContractsScraper()
        
        # Use smart incremental approach - only process symbols with new contracts
        contracts_results = contracts_scraper.scrape_incremental_smart(symbol_limit=ticker_limit)
        
        if contracts_results.get('success'):
            if contracts_results.get('symbols_processed', 0) > 0:
                logger.info(f"[option_contracts] ✓ {contracts_results['total_contracts']} contracts loaded for {contracts_results['successful_symbols']} symbols")
                logger.info(f"[option_contracts] ✓ {contracts_results['api_calls_made']} API calls in {contracts_results['duration']:.1f}s")
            else:
                logger.info("[option_contracts] ✓ All contracts up to date - no new contracts found")
        else:
            logger.warning(f"[option_contracts] ⚠ Partial success: {contracts_results['successful_symbols']}/{contracts_results['symbols_processed']} symbols")
            if contracts_results.get('failed_symbols'):
                failed_list = [item[0] if isinstance(item, tuple) else str(item) for item in contracts_results['failed_symbols'][:3]]
                logger.warning(f"[option_contracts] ⚠ Failed symbols: {', '.join(failed_list)}" + 
                             ("..." if len(contracts_results['failed_symbols']) > 3 else ""))
            # Don't fail the entire pipeline for contracts metadata
    except Exception as contracts_error:
        logger.error(f"[option_contracts] Failed to load contract metadata: {contracts_error}")
        # Continue with pipeline even if contracts fail

    # Update daily_option_snapshot with latest analytics data from temp_option for the processed dates
    logger.info("[daily_option_snapshot] Updating analytics columns with latest temp data...")
    from database.core.bulk_operations import BulkStockDataLoader as _Loader
    _l = _Loader()
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        try:
            out = _l.update_daily_option_snapshot_analytics_from_temp(ds)
            logger.info(f"[daily_option_snapshot] {ds} updated={out['records_updated']} records with latest analytics in {out['execution_time']:.2f}s")
        except Exception as ce:
            logger.error(f"[daily_option_snapshot] {ds} analytics update failed: {ce}")
        cur += _td(days=1)

    logger.info("[anomaly_retention] Keeping daily_anomaly_snapshot data for ongoing analysis")
    
    # Clean up old daily_anomaly_snapshot data using retention
    try:
        from database.core.connection import db as _db
        conn = _db.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT cleanup_old_anomalies(%s);", (anomaly_retention,))
            deleted_count = cur.fetchone()[0]
            conn.commit()
        logger.info(f"[anomaly_retention] Cleaned up {deleted_count} old anomaly records ({anomaly_retention}+ days)")
    except Exception as ae:
        logger.error(f"[anomaly_retention] Failed to cleanup old anomaly data: {ae}")

    # Truncate temp_option and temp_stock to keep only fresh intraday going forward
    # Note: daily_anomaly_snapshot is NOT truncated to preserve ongoing anomaly analysis
    try:
        from database.core.connection import db as _db
        _db.execute_command("TRUNCATE TABLE temp_option;")
        logger.info("[temp_option] truncated after daily snapshot capture")
        try:
            _db.execute_command("TRUNCATE TABLE temp_stock;")
            logger.info("[temp_stock] truncated after daily snapshot capture")
        except Exception as te2:
            logger.error(f"[temp_stock] truncate failed: {te2}")
    except Exception as te:
        logger.error(f"[temp_option] truncate failed: {te}")

    # 5) Retention cleanup for core tables (using bulk deletion for efficiency)
    logger.info("Applying retention policy with bulk deletion…")
    retention = DataRetentionManager()
    # Step 5: Truncate temp tables (intraday data no longer needed after daily processing)
    logger.info("Step 5: Truncating temp tables...")
    try:
        from database.core.connection import db as _db
        _conn = _db.connect()
        with _conn.cursor() as _cur:
            _cur.execute("TRUNCATE temp_stock, temp_option")
            _conn.commit()
        _conn.close()
        logger.info("✓ Truncated temp_stock and temp_option tables")
    except Exception as e:
        logger.error(f"✗ Failed to truncate temp tables: {e}")
    
    # Step 6: Retention cleanup for historical and anomaly data
    tables_to_clean = [
        ('daily_stock_snapshot', 'date', False),
        ('daily_option_snapshot', 'date', False),
        ('option_contracts', 'expiration_date', True),  # Expiration table - clean expired contracts
        ('daily_anomaly_snapshot', 'event_date', False),  # Clean old anomaly records using retention days
    ]
    
    for table, date_col, is_expiration in tables_to_clean:
        try:
            # Use bulk deletion for better performance
            res = retention.bulk_delete_old_records(
                table, date_col, retention_days, dry_run=dry_run_retention, 
                is_expiration_table=is_expiration
            )
            duration = res.get('duration_seconds', 0)
            deleted = res.get('records_deleted', 0)
            logger.info(f"Retention {table}: cutoff={res['cutoff_date']} deleted={deleted:,} records in {duration:.2f}s")
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
    parser.add_argument('--anomaly-retention', type=int, default=int(os.getenv('ANOMALY_RETENTION_DAYS', '30')), 
                        help='Anomaly retention in days (default: 30)')

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
            anomaly_retention=args.anomaly_retention,
        ))
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


