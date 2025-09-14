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
                       dry_run_retention: bool, anomaly_retention: int = 30, 
                       no_expired_contracts: bool = False) -> int:
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
        contract_retention_days = 0 if no_expired_contracts else retention_days
        contracts_results = contracts_scraper.scrape_incremental_smart(symbol_limit=ticker_limit, retention_days=contract_retention_days)
        
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

    # Step 4: Check and fix symbol mismatches between daily_option_snapshot and option_contracts
    logger.info("[symbol_mismatch_check] Step 4: Checking for symbol mismatches...")
    try:
        from database.core.connection import db
        
        # Check for mismatches
        mismatch_query = """
            SELECT a.symbol, a.contract_ticker, b.symbol as contract_symbol, b.underlying_ticker
            FROM (SELECT DISTINCT symbol, contract_ticker FROM daily_option_snapshot) a
            INNER JOIN option_contracts b ON a.contract_ticker = b.contract_ticker
            WHERE a.symbol <> b.symbol
        """
        
        mismatches = db.execute_query(mismatch_query)
        
        if mismatches:
            logger.warning(f"[symbol_mismatch_check] Found {len(mismatches)} symbol mismatches")
            
            # Fix mismatches by updating daily_option_snapshot to match option_contracts
            conn = db.connect()
            try:
                with conn.cursor() as cur:
                    total_updated = 0
                    
                    for mismatch in mismatches:
                        contract_ticker = mismatch['contract_ticker']
                        correct_symbol = mismatch['contract_symbol']
                        
                        # Update all records for this contract_ticker
                        cur.execute("""
                            UPDATE daily_option_snapshot 
                            SET symbol = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE contract_ticker = %s
                        """, (correct_symbol, contract_ticker))
                        
                        records_updated = cur.rowcount
                        total_updated += records_updated
                        logger.info(f"[symbol_mismatch_check] Fixed {contract_ticker}: {records_updated} records -> '{correct_symbol}'")
                    
                    conn.commit()
                    logger.info(f"[symbol_mismatch_check] ✓ Fixed {total_updated} total records across {len(mismatches)} contracts")
                    
            finally:
                conn.close()
        else:
            logger.info("[symbol_mismatch_check] ✓ No symbol mismatches found")
            
    except Exception as mismatch_error:
        logger.error(f"[symbol_mismatch_check] Failed to check/fix symbol mismatches: {mismatch_error}")
        # Continue with pipeline even if mismatch check fails

    # Step 5: Update daily_option_snapshot with Greeks and IV from prior day's temp data
    logger.info("[daily_option_snapshot] Step 5a: Updating Greeks and IV with prior day's temp data...")
    from database.core.bulk_operations import BulkStockDataLoader as _Loader
    _l = _Loader()
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        try:
            out = _l.update_daily_option_snapshot_greeks_and_iv_from_temp(ds)
            logger.info(f"[daily_option_snapshot] {ds} updated={out['records_updated']} records with Greeks/IV in {out['execution_time']:.2f}s")
        except Exception as ce:
            logger.error(f"[daily_option_snapshot] {ds} Greeks/IV update failed: {ce}")
        cur += _td(days=1)

    # Step 6: Run fresh temp_option data to get correct open_interest for the dates being processed
    logger.info("[temp_option] Step 5b: Running fresh intraday snapshot to get correct open_interest...")
    try:
        from scrapers.polygon_unified_options_snapshot_scraper import UnifiedOptionsSnapshotScraper
        from database.core.bulk_operations import BulkStockDataLoader
        
        opt_scraper = UnifiedOptionsSnapshotScraper()
        loader = BulkStockDataLoader()
        
        # Get all contract tickers from the dates we're processing
        from database.core.connection import db
        contract_tickers = set()
        cur = s
        while cur <= e:
            ds = cur.strftime('%Y-%m-%d')
            try:
                if stock_scraper.is_trading_day(cur):
                    rows = db.execute_query("SELECT DISTINCT contract_ticker FROM daily_option_snapshot WHERE date = %s", (ds,))
                    contract_tickers.update([r['contract_ticker'] for r in rows])
            except Exception:
                pass
            cur += _td(days=1)
        
        if contract_tickers:
            logger.info(f"[temp_option] Fetching fresh data for {len(contract_tickers)} contracts...")
            
            # Use super-batch approach like intraday schedule
            ticker_list = list(contract_tickers)
            page_size = 250  # Per-request ticker count
            bulk_calls = 100  # Calls per super-batch
            options_workers = 20  # Concurrent workers
            
            import math
            total_pages = math.ceil(len(ticker_list) / page_size)
            total_bulk_loads = math.ceil(total_pages / bulk_calls)
            
            logger.info(f"[temp_option] Planning: {len(ticker_list)} contracts, pages(@{page_size})={total_pages}, bulk_loads(@{bulk_calls} calls)={total_bulk_loads}")
            
            # Create request batches
            request_batches = []
            for i in range(0, len(ticker_list), page_size):
                batch = ticker_list[i:i + page_size]
                request_batches.append(batch)
            
            # Process in super-batches
            total_loaded = 0
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            for start in range(0, len(request_batches), bulk_calls):
                group = request_batches[start:start+bulk_calls]
                logger.info(f"[temp_option] Fetching super-batch {(start//bulk_calls)+1}/{total_bulk_loads} ({len(group)} calls)")
                
                combined_results = []
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
                            logger.error(f"fetch_by_tickers failed in super-batch at request {future_map[fut]}: {fe}")
                
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
                
                # Load super-batch
                if combined_results:
                    out = loader.bulk_upsert_temp_option_copy({'results': combined_results})
                    if out.get('success'):
                        total_loaded += out['records_processed']
                        logger.info(f"[temp_option] Super-batch loaded: {out['records_processed']} rows (total={total_loaded})")
                    else:
                        logger.error(f"[temp_option] Super-batch load failed: {out.get('error')}")
            
            logger.info(f"[temp_option] ✓ Fresh intraday data loaded: {total_loaded} total rows - open_interest now reflects correct dates")
        else:
            logger.warning("[temp_option] No contract tickers found for processing dates")
            
    except Exception as temp_error:
        logger.error(f"[temp_option] Failed to load fresh intraday data: {temp_error}")
        logger.warning("[temp_option] Continuing with stale temp data (open_interest may be incorrect)")

    # Step 7: Update daily_option_snapshot with fresh open_interest data only
    logger.info("[daily_option_snapshot] Step 5c: Updating open_interest with fresh temp data...")
    cur = s
    while cur <= e:
        ds = cur.strftime('%Y-%m-%d')
        try:
            out = _l.update_daily_option_snapshot_open_interest_from_temp(ds)
            logger.info(f"[daily_option_snapshot] {ds} updated={out['records_updated']} records with fresh open_interest in {out['execution_time']:.2f}s")
        except Exception as ce:
            logger.error(f"[daily_option_snapshot] {ds} open_interest update failed: {ce}")
        cur += _td(days=1)

    logger.info("[anomaly_retention] Keeping daily_anomaly_snapshot data for ongoing analysis")

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

    # Step 8: Truncate temp tables (intraday data no longer needed after daily processing)
    logger.info("Step 8: Truncating temp tables...")
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
    
    # Step 9: Retention cleanup for all historical data
    logger.info("Applying retention policy with bulk deletion…")
    retention = DataRetentionManager()
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
    parser.add_argument('--no-expired-contracts', action='store_true', 
                        help='Skip fetching expired contracts (active only)')

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
            no_expired_contracts=args.no_expired_contracts,
        ))
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()


