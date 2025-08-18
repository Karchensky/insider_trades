"""
Optimized Polygon.io Options Contracts Scraper
High-performance scraper using concurrent API calls and batch processing.

Features:
- Concurrent API calls (up to 20/sec)
- Batch processing: collects all symbols per date before bulk loading
- Significantly faster than sequential processing
"""

import os
import sys
import logging
import requests
import argparse
import asyncio
import aiohttp
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.stock_data import StockDataManager
from database.bulk_operations import BulkStockDataLoader
from scrapers.polygon_daily_scraper import PolygonDailyScraper
from scrapers.polygon_options_scraper import PolygonOptionsContractsScraper
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OptimizedPolygonOptionsContractsScraper(PolygonOptionsContractsScraper):
    """
    High-performance scraper for Polygon.io Options Contracts API.
    Uses concurrent API calls and batch processing for maximum throughput.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the optimized options contracts scraper."""
        super().__init__(api_key)
        
        # Optimized settings for high throughput
        self.max_concurrent_requests = 20  # 20 requests per second limit
        self.request_delay = 0.05  # 50ms between requests (20/sec)
        self.batch_size = 500  # Optimize for better parallelism and memory usage
        self.max_retries = 3
        self.retry_delay = 1  # Shorter retry delay for faster recovery
        
        # Dynamic rate limiting settings
        self.dynamic_rate_limiting = True
        self.current_delay = self.request_delay
        self.success_streak = 0
        self.backoff_multiplier = 1.5
        
        # Performance tracking
        self.bulk_loader = BulkStockDataLoader()
        self.session = requests.Session()
        
        # Configure session for better performance with larger connection pool
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
    
    def get_option_contracts_batch(self, tickers: List[str], as_of_date: str) -> List[Dict[str, Any]]:
        """
        Fetch option contracts for multiple tickers concurrently.
        
        Args:
            tickers: List of ticker symbols
            as_of_date: Date for as_of parameter in YYYY-MM-DD format
            
        Returns:
            List of API responses (successful ones)
        """
        logger.info(f"Fetching options for {len(tickers)} tickers concurrently...")
        
        def fetch_single_ticker(ticker: str) -> Optional[Dict[str, Any]]:
            """Fetch options for a single ticker with dynamic rate limiting."""
            url = f"{self.base_url}/v3/reference/options/contracts"
            params = {
                'underlying_ticker': ticker,
                'as_of': as_of_date,
                'expired': 'false',
                'limit': 1000,
                'apikey': self.api_key
            }
            
            for attempt in range(self.max_retries):
                try:
                    response = self.session.get(url, params=params, timeout=30)
                    
                    # Handle rate limiting with dynamic backoff
                    if response.status_code == 429:
                        # Increase delay for future requests
                        self.current_delay = min(self.current_delay * self.backoff_multiplier, 1.0)
                        self.success_streak = 0
                        
                        wait_time = self.retry_delay * (2 ** attempt)
                        logger.warning(f"Rate limited for {ticker}, increasing delay to {self.current_delay:.3f}s, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    data = response.json()
                    
                    if data.get('status') == 'OK':
                        # Success - potentially decrease delay
                        self.success_streak += 1
                        if self.dynamic_rate_limiting and self.success_streak >= 10:
                            # After 10 successful requests, try to speed up slightly
                            self.current_delay = max(self.current_delay * 0.95, self.request_delay)
                            self.success_streak = 0
                        
                        results_count = len(data.get('results', []))
                        if results_count > 0:
                            logger.debug(f"✓ {ticker}: {results_count} contracts")
                        else:
                            logger.debug(f"✓ {ticker}: no contracts")
                        
                        # Add metadata
                        data['underlying_ticker'] = ticker
                        data['as_of_date'] = as_of_date
                        data['fetch_timestamp'] = datetime.now().isoformat()
                        
                        return data
                    else:
                        logger.warning(f"API error for {ticker}: {data.get('status')}")
                        return None
                        
                except requests.exceptions.RequestException as e:
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay * (2 ** attempt)
                        logger.debug(f"Request failed for {ticker} (attempt {attempt + 1}), retrying in {wait_time}s: {e}")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to fetch options for {ticker} after {self.max_retries} attempts: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error for {ticker}: {e}")
                    break
            
            return None
        
        # Use ThreadPoolExecutor for concurrent requests
        successful_responses = []
        failed_tickers = []
        
        with ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
            # Submit all requests
            future_to_ticker = {
                executor.submit(fetch_single_ticker, ticker): ticker 
                for ticker in tickers
            }
            
            # Collect results with rate limiting
            for i, future in enumerate(as_completed(future_to_ticker)):
                ticker = future_to_ticker[future]
                
                try:
                    result = future.result()
                    if result:
                        successful_responses.append(result)
                    else:
                        failed_tickers.append(ticker)
                except Exception as e:
                    logger.error(f"Error processing {ticker}: {e}")
                    failed_tickers.append(ticker)
                
                # Optimized rate limiting: more aggressive approach to 20/sec
                if (i + 1) % self.max_concurrent_requests == 0:
                    time.sleep(1.0)  # Wait 1 second after every 20 requests
                elif (i + 1) % 5 == 0:
                    time.sleep(0.25)  # Brief pause every 5 requests
                else:
                    time.sleep(self.current_delay)  # Minimal delay between requests
        
        logger.info(f"Completed batch: {len(successful_responses)} successful, {len(failed_tickers)} failed")
        
        if failed_tickers:
            logger.warning(f"Failed tickers: {failed_tickers[:10]}{'...' if len(failed_tickers) > 10 else ''}")
        
        return successful_responses
    
    def scrape_options_for_date_optimized(self, target_date: str, ticker_limit: Optional[int] = None,
                                         skip_existing: bool = True) -> Dict[str, Any]:
        """
        Optimized option contracts scraping for a single date.
        Fetches all tickers concurrently and bulk loads in one operation.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            ticker_limit: Limit number of tickers (for testing)
            skip_existing: Skip if options data already exists for date
            
        Returns:
            Dictionary with scraping results and statistics
        """
        logger.info(f"Starting optimized options scraping for {target_date}")
        start_time = time.time()
        
        results = {
            'date': target_date,
            'total_tickers_requested': 0,
            'successful_api_calls': 0,
            'failed_api_calls': 0,
            'skipped_tickers': 0,
            'total_contracts_inserted': 0,
            'processing_time_seconds': 0,
            'api_calls_per_second': 0,
            'contracts_per_second': 0
        }
        
        # Check if we should skip this date entirely
        if skip_existing:
            try:
                existing_check_sql = """
                SELECT COUNT(DISTINCT symbol) as ticker_count 
                FROM option_contracts 
                WHERE date = %s
                """
                from database.connection import db
                existing_count = db.execute_query(existing_check_sql, (target_date,))
                if existing_count and existing_count[0]['ticker_count'] > 0:
                    logger.info(f"Skipping {target_date} - options data already exists for {existing_count[0]['ticker_count']} tickers")
                    results['skipped_tickers'] = existing_count[0]['ticker_count']
                    return results
            except Exception as e:
                logger.warning(f"Could not check existing data for {target_date}: {e}")
        
        # Get underlying tickers for the date
        underlying_tickers = self.get_underlying_tickers_for_date(target_date)
        
        if not underlying_tickers:
            logger.error(f"No underlying tickers found for {target_date}")
            return results
        
        # Apply ticker limit if specified
        if ticker_limit:
            underlying_tickers = underlying_tickers[:ticker_limit]
            logger.info(f"Limited to first {ticker_limit} tickers for processing")
        
        results['total_tickers_requested'] = len(underlying_tickers)
        
        logger.info(f"Processing {len(underlying_tickers)} tickers with concurrent API calls...")
        
        # Process tickers in batches to manage memory and rate limits
        all_api_responses = []
        batch_start_time = time.time()
        
        for i in range(0, len(underlying_tickers), self.batch_size):
            batch_tickers = underlying_tickers[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(underlying_tickers) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches}: {len(batch_tickers)} tickers")
            
            # Fetch options for this batch
            batch_responses = self.get_option_contracts_batch(batch_tickers, target_date)
            all_api_responses.extend(batch_responses)
            
            # Update statistics
            results['successful_api_calls'] += len(batch_responses)
            results['failed_api_calls'] += len(batch_tickers) - len(batch_responses)
            
            # Log batch progress
            batch_time = time.time() - batch_start_time
            batch_rate = len(batch_tickers) / batch_time if batch_time > 0 else 0
            actual_api_rate = len(batch_responses) / batch_time if batch_time > 0 else 0
            logger.info(f"Batch {batch_num} completed: {len(batch_responses)}/{len(batch_tickers)} successful ({actual_api_rate:.1f} API calls/sec, current delay: {self.current_delay:.3f}s)")
            
            batch_start_time = time.time()
        
        # Bulk load all option contracts in one operation
        if all_api_responses:
            logger.info(f"Bulk loading option contracts from {len(all_api_responses)} API responses...")
            
            try:
                success = self.bulk_loader.bulk_insert_option_contracts_batch(
                    all_api_responses, target_date, method='auto'
                )
                
                if success:
                    # Count total contracts
                    total_contracts = sum(len(resp.get('results', [])) for resp in all_api_responses)
                    results['total_contracts_inserted'] = total_contracts
                    
                    logger.info(f"✓ Successfully bulk-loaded {total_contracts:,} option contracts")
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
            results['contracts_per_second'] = round(results['total_contracts_inserted'] / total_time, 1)
        
        logger.info(f"Date {target_date} completed in {total_time:.1f}s: "
                   f"{results['successful_api_calls']} API calls, "
                   f"{results['total_contracts_inserted']:,} contracts")
        
        return results
    
    def scrape_options_date_range_optimized(self, start_date: str, end_date: str, 
                                           ticker_limit: Optional[int] = None,
                                           skip_existing: bool = True) -> Dict[str, Any]:
        """
        Optimized scraping for multiple dates.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            ticker_limit: Limit tickers per date (for testing)
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
            'total_contracts_inserted': 0,
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
            
            # Process options for this date
            try:
                date_results = self.scrape_options_for_date_optimized(
                    date_str, ticker_limit=ticker_limit, skip_existing=skip_existing
                )
                
                # Aggregate results
                combined_results['total_api_calls'] += date_results['successful_api_calls']
                combined_results['total_contracts_inserted'] += date_results['total_contracts_inserted']
                combined_results['total_processing_time'] += date_results['processing_time_seconds']
                
                if date_results['total_contracts_inserted'] > 0 or date_results['skipped_tickers'] > 0:
                    combined_results['successful_dates'] += 1
                    combined_results['dates_processed'].append(date_str)
                    logger.info(f"✓ Completed optimized scraping for {date_str}")
                else:
                    combined_results['failed_dates'] += 1
                    combined_results['failed_dates_list'].append(date_str)
                    logger.error(f"Failed optimized scraping for {date_str}")
                    
            except Exception as e:
                combined_results['failed_dates'] += 1
                combined_results['failed_dates_list'].append(date_str)
                logger.error(f"Error processing options for {date_str}: {e}")
            
            current_date += timedelta(days=1)
        
        return combined_results


def main():
    """Main entry point for the optimized options scraper."""
    
    parser = argparse.ArgumentParser(description='Optimized Polygon.io Options Contracts Scraper')
    
    parser.add_argument('--start-date', type=str, 
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--recent', type=int, nargs='?', const=1, metavar='DAYS',
                       help='Scrape recent trading days (default: 1, e.g., --recent 5)')
    parser.add_argument('--ticker-limit', type=int,
                       help='Limit number of tickers per date (for testing)')
    parser.add_argument('--force', action='store_true',
                       help='Force re-scrape even if data exists')
    
    args = parser.parse_args()
    
    try:
        scraper = OptimizedPolygonOptionsContractsScraper()
        
        if args.recent is not None:
            # Scrape recent trading days
            if args.recent == 1:
                recent_date = scraper.get_most_recent_trading_day()
                logger.info(f"Scraping options for most recent trading day: {recent_date}")
                start_date, end_date = recent_date, recent_date
            else:
                start_date, end_date = scraper.get_recent_trading_days(args.recent)
                logger.info(f"Scraping options for most recent {args.recent} trading days: {start_date} to {end_date}")
            
            results = scraper.scrape_options_date_range_optimized(
                start_date, end_date,
                ticker_limit=args.ticker_limit,
                skip_existing=not args.force
            )
            
        elif args.start_date and args.end_date:
            # Scrape date range
            logger.info(f"Scraping options for date range: {args.start_date} to {args.end_date}")
            
            results = scraper.scrape_options_date_range_optimized(
                args.start_date, args.end_date,
                ticker_limit=args.ticker_limit,
                skip_existing=not args.force
            )
            
        else:
            logger.error("Please provide either --recent or both --start-date and --end-date")
            parser.print_help()
            return
        
        # Print summary
        print("\n" + "=" * 70)
        print("OPTIMIZED OPTIONS CONTRACTS SCRAPING SUMMARY")
        print("=" * 70)
        print(f"Total dates requested: {results['total_dates_requested']}")
        print(f"Trading days found: {results['trading_days_found']}")
        print(f"Successful dates: {results['successful_dates']}")
        print(f"Failed dates: {results['failed_dates']}")
        print(f"Skipped dates: {results['skipped_dates']}")
        print(f"Total API calls: {results['total_api_calls']:,}")
        print(f"Total contracts inserted: {results['total_contracts_inserted']:,}")
        print(f"Total processing time: {results['total_processing_time']:.1f} seconds")
        
        # Calculate performance metrics
        if results['total_processing_time'] > 0:
            api_rate = results['total_api_calls'] / results['total_processing_time']
            contract_rate = results['total_contracts_inserted'] / results['total_processing_time']
            print(f"\nPerformance Metrics:")
            print(f"  API calls per second: {api_rate:.1f}")
            print(f"  Contracts per second: {contract_rate:.1f}")
            
            # Show performance improvement
            old_estimated_time = results['total_api_calls'] * 6  # Old scraper: 6 seconds per API call
            speedup = old_estimated_time / results['total_processing_time'] if results['total_processing_time'] > 0 else 0
            print(f"  Estimated speedup: {speedup:.1f}x faster than sequential processing")
        
        # Show database performance stats
        if hasattr(scraper, 'bulk_loader'):
            perf_stats = scraper.bulk_loader.get_performance_stats()
            if perf_stats['total_records_processed'] > 0:
                print(f"\nBulk Loading Performance:")
                print(f"  Average speed: {perf_stats['average_records_per_second']:,.0f} contracts/sec")
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
        logger.info("Optimized options scraping interrupted by user")
    except Exception as e:
        logger.error(f"Optimized options scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()
