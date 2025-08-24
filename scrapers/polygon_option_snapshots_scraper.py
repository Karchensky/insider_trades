"""
Polygon.io Option Stock Snapshots Scraper
Fetches daily OHLC data for option contracts using the Daily Ticker Summary endpoint.

Based on: https://polygon.io/docs/rest/options/aggregates/daily-ticker-summary
Endpoint: GET /v1/open-close/{optionsTicker}/{date}
"""

import os
import sys
import logging
import requests
import argparse
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.bulk_operations import BulkStockDataLoader
from scrapers.polygon_options_scraper_optimized import OptimizedPolygonOptionsContractsScraper
from scrapers.polygon_option_flatfile_loader import PolygonOptionFlatFileLoader
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PolygonOptionSnapshotsScraper(OptimizedPolygonOptionsContractsScraper):
    """
    High-performance scraper for Polygon.io Daily Ticker Summary API.
    Fetches OHLC data for option contracts using concurrent processing.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the option snapshots scraper."""
        super().__init__(api_key)
        
        # Override batch size for snapshots (smaller since we need individual contract calls)
        self.batch_size = 500  # Optimal for snapshot API calls
        
        logger.info("Option snapshots scraper initialized")
    
    def get_option_contracts_for_date(self, target_date: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get list of option contracts from the option_contracts table for a specific date.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            limit: Limit number of contracts (for testing)
            
        Returns:
            List of option contract records
        """
        try:
            if limit is None:
                # Get all contracts for the date
                contracts_sql = """
                SELECT date, symbol, contract_ticker 
                FROM option_contracts 
                WHERE date = %s
                ORDER BY symbol, contract_ticker
                """
                params = (target_date,)
            else:
                # With limit
                contracts_sql = """
                SELECT date, symbol, contract_ticker 
                FROM option_contracts 
                WHERE date = %s
                ORDER BY symbol, contract_ticker
                LIMIT %s
                """
                params = (target_date, limit)
            
            contracts = db.execute_query(contracts_sql, params)
            
            if not contracts:
                logger.warning(f"No option contracts found for {target_date}")
                return []
            
            logger.info(f"Found {len(contracts)} option contracts for {target_date}")
            return contracts
            
        except Exception as e:
            logger.error(f"Failed to get option contracts for {target_date}: {e}")
            return []
    
    def get_option_snapshot(self, contract_ticker: str, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Fetch daily snapshot for a specific option contract from Polygon API.
        
        Args:
            contract_ticker: The option contract ticker (e.g., O:AAPL211119C00085000)
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            API response dict or None if failed
        """
        url = f"{self.base_url}/v1/open-close/{contract_ticker}/{date_str}"
        params = {
            'adjusted': 'true',
            'apikey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)

                # Rate limiting with backoff
                if response.status_code == 429:
                    self.current_delay = min(self.current_delay * self.backoff_multiplier, 1.0)
                    self.success_streak = 0
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited for {contract_ticker}, increasing delay to {self.current_delay:.3f}s, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                # Treat 404 as NOT_FOUND and skip without retrying further
                if response.status_code == 404:
                    logger.debug(f"{contract_ticker} snapshot NOT_FOUND (404) for {date_str}, skipping")
                    return None

                # Parse JSON even if non-200 where possible
                response.raise_for_status()
                data = response.json()

                status = data.get('status')
                if status == 'OK':
                    self.success_streak += 1
                    if self.dynamic_rate_limiting and self.success_streak >= 10:
                        self.current_delay = max(self.current_delay * 0.95, self.request_delay)
                        self.success_streak = 0

                    # Annotate
                    data['contract_ticker'] = contract_ticker
                    data['requested_date'] = date_str
                    data['fetch_timestamp'] = datetime.now().isoformat()
                    return data

                if status == 'NOT_FOUND':
                    logger.debug(f"{contract_ticker} snapshot NOT_FOUND for {date_str}, skipping")
                    return None

                logger.warning(f"Unexpected status '{status}' for {contract_ticker} {date_str}, skipping")
                return None

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.debug(f"Request failed for {contract_ticker} (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    # If it was a 404 it would have returned above; other errors after retries -> skip
                    logger.warning(f"Skipping {contract_ticker} after {self.max_retries} attempts: {e}")
                continue
            except Exception as e:
                logger.warning(f"Skipping {contract_ticker} due to unexpected error: {e}")
                return None
        
        return None
    
    def get_option_snapshots_batch(self, contracts: List[Dict[str, Any]], date_str: str) -> List[Dict[str, Any]]:
        """
        Fetch option snapshots for multiple contracts concurrently.
        
        Args:
            contracts: List of contract records from database
            date_str: Date in YYYY-MM-DD format
            
        Returns:
            List of API responses (successful ones)
        """
        logger.info(f"Fetching snapshots for {len(contracts)} contracts concurrently...")
        
        def fetch_single_snapshot(contract: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            """Fetch snapshot for a single contract."""
            contract_ticker = contract['contract_ticker']
            return self.get_option_snapshot(contract_ticker, date_str)
        
        # Use ThreadPoolExecutor for concurrent requests
        successful_responses = []
        failed_contracts = []
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
            # Submit all requests
            future_to_contract = {
                executor.submit(fetch_single_snapshot, contract): contract 
                for contract in contracts
            }
            
            # Collect results with rate limiting
            for i, future in enumerate(as_completed(future_to_contract)):
                contract = future_to_contract[future]
                
                try:
                    result = future.result()
                    if result:
                        successful_responses.append(result)
                    else:
                        failed_contracts.append(contract['contract_ticker'])
                except Exception as e:
                    logger.error(f"Error processing {contract['contract_ticker']}: {e}")
                    failed_contracts.append(contract['contract_ticker'])
                
                # Optimized rate limiting: more aggressive approach to 20/sec
                if (i + 1) % self.max_concurrent_requests == 0:
                    time.sleep(1.0)  # Wait 1 second after every 20 requests
                elif (i + 1) % 5 == 0:
                    time.sleep(0.25)  # Brief pause every 5 requests
                else:
                    time.sleep(self.current_delay)  # Minimal delay between requests
        
        logger.info(f"Completed batch: {len(successful_responses)} successful, {len(failed_contracts)} failed")
        
        if failed_contracts:
            logger.warning(f"Failed contracts: {failed_contracts[:10]}{'...' if len(failed_contracts) > 10 else ''}")
        
        return successful_responses
    
    def scrape_snapshots_for_date(self, target_date: str, contract_limit: Optional[int] = None,
                                 skip_existing: bool = True) -> Dict[str, Any]:
        """
        Scrape option snapshots for all contracts on a specific date.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            contract_limit: Limit number of contracts (for testing)
            skip_existing: Skip if snapshot data already exists for date
            
        Returns:
            Dictionary with scraping results and statistics
        """
        logger.info(f"Starting option snapshots scraping for {target_date}")
        start_time = time.time()
        
        results = {
            'date': target_date,
            'total_contracts_requested': 0,
            'successful_api_calls': 0,
            'failed_api_calls': 0,
            'skipped_contracts': 0,
            'total_snapshots_inserted': 0,
            'processing_time_seconds': 0,
            'api_calls_per_second': 0,
            'snapshots_per_second': 0
        }
        
        # If configured, load from flat-file day aggregates instead of per-contract API calls
        if os.getenv('OPTIONS_SNAPSHOT_SOURCE', 'api').lower() in ('flatfile', 'flat-file', 'flat'):
            try:
                logger.info("OPTIONS_SNAPSHOT_SOURCE=flatfile - using Polygon flat-file day aggregates")
                ff_loader = PolygonOptionFlatFileLoader(api_key=self.api_key)
                ff_res = ff_loader.load_for_date(target_date, skip_existing=skip_existing)
                results['total_snapshots_inserted'] = 0  # unknown without extra query
                total_time = time.time() - start_time
                results['processing_time_seconds'] = round(total_time, 2)
                if ff_res.get('success'):
                    logger.info("✓ Flat-file load completed")
                else:
                    logger.error("Flat-file load failed")
                return results
            except Exception as e:
                logger.error(f"Flat-file load failed, falling back to API path: {e}")

        # Check if we should skip this date entirely (API path)
        if skip_existing:
            try:
                existing_check_sql = """
                SELECT COUNT(*) as count 
                FROM daily_option_snapshot 
                WHERE date = %s
                """
                existing_count = db.execute_query(existing_check_sql, (target_date,))
                if existing_count and existing_count[0]['count'] > 0:
                    logger.info(f"Skipping {target_date} - snapshot data already exists for {existing_count[0]['count']} contracts")
                    results['skipped_contracts'] = existing_count[0]['count']
                    return results
            except Exception as e:
                logger.warning(f"Could not check existing snapshot data for {target_date}: {e}")
        
        # Get option contracts for the date
        contracts = self.get_option_contracts_for_date(target_date, limit=contract_limit)
        
        if not contracts:
            logger.error(f"No option contracts found for {target_date}")
            return results
        
        results['total_contracts_requested'] = len(contracts)
        
        logger.info(f"Processing {len(contracts)} contracts with concurrent API calls...")
        
        # Process contracts in batches to manage memory and rate limits
        all_api_responses = []
        batch_start_time = time.time()
        
        for i in range(0, len(contracts), self.batch_size):
            batch_contracts = contracts[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(contracts) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches}: {len(batch_contracts)} contracts")
            
            # Fetch snapshots for this batch
            batch_responses = self.get_option_snapshots_batch(batch_contracts, target_date)
            all_api_responses.extend(batch_responses)
            
            # Update statistics
            results['successful_api_calls'] += len(batch_responses)
            results['failed_api_calls'] += len(batch_contracts) - len(batch_responses)
            
            # Log batch progress
            batch_time = time.time() - batch_start_time
            actual_api_rate = len(batch_responses) / batch_time if batch_time > 0 else 0
            logger.info(f"Batch {batch_num} completed: {len(batch_responses)}/{len(batch_contracts)} successful ({actual_api_rate:.1f} API calls/sec, current delay: {self.current_delay:.3f}s)")
            
            batch_start_time = time.time()
        
        # Bulk load all option snapshots in one operation
        if all_api_responses:
            logger.info(f"Bulk loading option snapshots from {len(all_api_responses)} API responses...")
            
            try:
                success = self.bulk_loader.bulk_insert_option_snapshots_batch(
                    all_api_responses, method='auto'
                )
                
                if success:
                    results['total_snapshots_inserted'] = len(all_api_responses)
                    logger.info(f"✓ Successfully bulk-loaded {len(all_api_responses):,} option snapshots")
                else:
                    logger.error("Bulk loading failed")
                    
            except Exception as e:
                logger.error(f"Bulk loading error: {e}")
                results['failed_api_calls'] += results['successful_api_calls']
                results['successful_api_calls'] = 0
        
        # Calculate final statistics
        total_time = time.time() - start_time
        results['processing_time_seconds'] = round(total_time, 2)
        
        if total_time > 0:
            results['api_calls_per_second'] = round(results['successful_api_calls'] / total_time, 1)
            results['snapshots_per_second'] = round(results['total_snapshots_inserted'] / total_time, 1)
        
        logger.info(f"Date {target_date} completed in {total_time:.1f}s: "
                   f"{results['successful_api_calls']} API calls, "
                   f"{results['total_snapshots_inserted']:,} snapshots")
        
        return results
    
    def scrape_snapshots_date_range(self, start_date: str, end_date: str, 
                                   contract_limit: Optional[int] = None,
                                   skip_existing: bool = True) -> Dict[str, Any]:
        """
        Scrape option snapshots for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            contract_limit: Limit contracts per date (for testing)
            skip_existing: Skip existing data
            
        Returns:
            Dictionary with combined results
        """
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        if start > end:
            raise ValueError("Start date must be before or equal to end date")
        
        combined_results = {
            'total_dates_requested': 0,
            'trading_days_found': 0,
            'successful_dates': 0,
            'failed_dates': 0,
            'skipped_dates': 0,
            'total_api_calls': 0,
            'total_snapshots_inserted': 0,
            'total_processing_time': 0,
            'dates_processed': [],
            'failed_dates_list': [],
            'skipped_dates_list': []
        }
        
        current_date = start
        
        while current_date <= end:
            combined_results['total_dates_requested'] += 1
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Skip non-trading days
            if not self.is_trading_day(current_date):
                logger.info(f"Skipping {date_str} - not a trading day")
                combined_results['skipped_dates'] += 1
                combined_results['skipped_dates_list'].append(date_str)
                current_date += timedelta(days=1)
                continue
            
            combined_results['trading_days_found'] += 1
            
            # Process snapshots for this date
            try:
                date_results = self.scrape_snapshots_for_date(
                    date_str, contract_limit=contract_limit, skip_existing=skip_existing
                )
                
                # Aggregate results
                combined_results['total_api_calls'] += date_results['successful_api_calls']
                combined_results['total_snapshots_inserted'] += date_results['total_snapshots_inserted']
                combined_results['total_processing_time'] += date_results['processing_time_seconds']
                
                if date_results['total_snapshots_inserted'] > 0 or date_results['skipped_contracts'] > 0:
                    combined_results['successful_dates'] += 1
                    combined_results['dates_processed'].append(date_str)
                    logger.info(f"✓ Completed snapshots scraping for {date_str}")
                else:
                    combined_results['failed_dates'] += 1
                    combined_results['failed_dates_list'].append(date_str)
                    logger.error(f"Failed snapshots scraping for {date_str}")
                    
            except Exception as e:
                combined_results['failed_dates'] += 1
                combined_results['failed_dates_list'].append(date_str)
                logger.error(f"Error processing snapshots for {date_str}: {e}")
            
            current_date += timedelta(days=1)
        
        return combined_results


def main():
    """Main entry point for the option snapshots scraper."""
    
    parser = argparse.ArgumentParser(description='Polygon.io Option Snapshots Scraper')
    
    parser.add_argument('--start-date', type=str, 
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--recent', type=int, nargs='?', const=1, metavar='DAYS',
                       help='Scrape recent trading days (default: 1, e.g., --recent 5)')
    parser.add_argument('--contract-limit', type=int,
                       help='Limit number of contracts per date (for testing)')
    parser.add_argument('--force', action='store_true',
                       help='Force re-scrape even if data exists')
    
    args = parser.parse_args()
    
    try:
        scraper = PolygonOptionSnapshotsScraper()
        
        if args.recent is not None:
            # Scrape recent trading days
            if args.recent == 1:
                recent_date = scraper.get_most_recent_trading_day()
                logger.info(f"Scraping snapshots for most recent trading day: {recent_date}")
                start_date, end_date = recent_date, recent_date
            else:
                start_date, end_date = scraper.get_recent_trading_days(args.recent)
                logger.info(f"Scraping snapshots for most recent {args.recent} trading days: {start_date} to {end_date}")
            
            results = scraper.scrape_snapshots_date_range(
                start_date, end_date,
                contract_limit=args.contract_limit,
                skip_existing=not args.force
            )
            
        elif args.start_date and args.end_date:
            # Scrape date range
            logger.info(f"Scraping snapshots for date range: {args.start_date} to {args.end_date}")
            
            results = scraper.scrape_snapshots_date_range(
                args.start_date, args.end_date,
                contract_limit=args.contract_limit,
                skip_existing=not args.force
            )
            
        else:
            logger.error("Please provide either --recent or both --start-date and --end-date")
            parser.print_help()
            return
        
        # Print summary
        print("\n" + "=" * 70)
        print("OPTION SNAPSHOTS SCRAPING SUMMARY")
        print("=" * 70)
        print(f"Total dates requested: {results['total_dates_requested']}")
        print(f"Trading days found: {results['trading_days_found']}")
        print(f"Successful dates: {results['successful_dates']}")
        print(f"Failed dates: {results['failed_dates']}")
        print(f"Skipped dates: {results['skipped_dates']}")
        print(f"Total API calls: {results['total_api_calls']:,}")
        print(f"Total snapshots inserted: {results['total_snapshots_inserted']:,}")
        print(f"Total processing time: {results['total_processing_time']:.1f} seconds")
        
        # Calculate performance metrics
        if results['total_processing_time'] > 0:
            api_rate = results['total_api_calls'] / results['total_processing_time']
            snapshot_rate = results['total_snapshots_inserted'] / results['total_processing_time']
            print(f"\nPerformance Metrics:")
            print(f"  API calls per second: {api_rate:.1f}")
            print(f"  Snapshots per second: {snapshot_rate:.1f}")
        
        # Show database performance stats
        if hasattr(scraper, 'bulk_loader'):
            perf_stats = scraper.bulk_loader.get_performance_stats()
            if perf_stats['total_records_processed'] > 0:
                print(f"\nBulk Loading Performance:")
                print(f"  Average speed: {perf_stats['average_records_per_second']:,.0f} snapshots/sec")
                print(f"  Success rate: {perf_stats['success_rate_percent']}%")
        
        if results['dates_processed']:
            print(f"\nSuccessfully processed dates:")
            for date_str in results['dates_processed']:
                print(f"  ✓ {date_str}")
        
        if results['failed_dates_list']:
            print(f"\nFailed dates:")
            for date_str in results['failed_dates_list']:
                print(f"  ✗ {date_str}")
        
    except KeyboardInterrupt:
        logger.info("Option snapshots scraping interrupted by user")
    except Exception as e:
        logger.error(f"Option snapshots scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()
