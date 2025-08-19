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
from scrapers.polygon_options_scraper_optimized import OptimizedPolygonOptionsContractsScraper
from scrapers.polygon_option_snapshots_scraper import PolygonOptionSnapshotsScraper
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
    logger.info("[daily_stock_snapshot] Step 1/3: loading…")
    stock_results = stock_scraper.scrape_date_range(
        start_date, end_date, include_otc=include_otc, skip_existing=not force
    )
    logger.info(f"[daily_stock_snapshot] inserted={stock_results['total_records_inserted']:,} dates_ok={stock_results['successful_scrapes']} skipped={stock_results['skipped_dates']}")

    # 3) Populate option_contracts
    logger.info("[option_contracts] Step 2/3: loading…")
    contracts_scraper = OptimizedPolygonOptionsContractsScraper()
    contracts_results = contracts_scraper.scrape_options_date_range_optimized(
        start_date, end_date, ticker_limit=ticker_limit, skip_existing=not force
    )
    logger.info(f"[option_contracts] inserted={contracts_results['total_contracts_inserted']:,} success_dates={contracts_results['successful_dates']} skipped={contracts_results['skipped_dates']}")

    # 4) Populate daily_option_snapshot
    logger.info("[daily_option_snapshot] Step 3/3: loading…")
    snapshots_scraper = PolygonOptionSnapshotsScraper()
    snapshots_results = snapshots_scraper.scrape_snapshots_date_range(
        start_date, end_date, contract_limit=contract_limit, skip_existing=not force
    )
    logger.info(f"[daily_option_snapshot] inserted={snapshots_results['total_snapshots_inserted']:,} success_dates={snapshots_results['successful_dates']} skipped={snapshots_results['skipped_dates']}")

    # 5) Retention cleanup for core tables
    logger.info("Applying retention policy…")
    retention = DataRetentionManager()
    for table, date_col in (
        ('daily_stock_snapshot', 'date'),
        ('option_contracts', 'date'),
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


