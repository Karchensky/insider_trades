"""
Polygon.io Options Contracts Scraper
Fetches option contracts data for underlying tickers from daily_stock_snapshot table.

Based on: https://polygon.io/docs/rest/options/contracts/all-contracts
Endpoint: GET /v3/reference/options/contracts
"""

import os
import sys
import logging
import requests
import argparse
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from time import sleep

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.stock_data import StockDataManager
from database.bulk_operations import BulkStockDataLoader
from scrapers.polygon_daily_scraper import PolygonDailyScraper
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PolygonOptionsContractsScraper(PolygonDailyScraper):
    """
    Scraper for Polygon.io Options Contracts API.
    Inherits date/trading day logic from PolygonDailyScraper.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the options contracts scraper."""
        super().__init__(api_key)
        
        # Options-specific settings
        self.options_rate_limit_delay = 6  # 10 requests per minute for free tier
        self.max_contracts_per_request = 1000  # API limit
        
        # Performance tracking for options
        self.bulk_loader = BulkStockDataLoader()
    
    def get_underlying_tickers_for_date(self, target_date: str) -> List[str]:
        """
        Get list of underlying tickers from daily_stock_snapshot for a specific date.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of ticker symbols
        """
        try:
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
            
            # Get market snapshot for the date - get ALL tickers (no limit)
            market_data = StockDataManager.get_market_snapshot(target_date_obj, limit=None)
            
            if not market_data:
                logger.warning(f"No stock data found for {target_date}")
                return []
            
            tickers = [record['symbol'] for record in market_data]
            logger.info(f"Found {len(tickers)} underlying tickers for {target_date}")
            
            return tickers
            
        except Exception as e:
            logger.error(f"Failed to get underlying tickers for {target_date}: {e}")
            return []
    
    def get_option_contracts(self, underlying_ticker: str, as_of_date: str, 
                           contract_type: Optional[str] = None, 
                           expired: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch option contracts for a specific underlying ticker from Polygon API.
        
        Args:
            underlying_ticker: The underlying stock ticker
            as_of_date: Date for as_of parameter in YYYY-MM-DD format
            contract_type: Filter by contract type ('call', 'put', or None for both)
            expired: Include expired contracts
            
        Returns:
            API response dict or None if failed
        """
        url = f"{self.base_url}/v3/reference/options/contracts"
        params = {
            'underlying_ticker': underlying_ticker,
            'as_of': as_of_date,
            'expired': str(expired).lower(),
            'limit': self.max_contracts_per_request,
            'apikey': self.api_key
        }
        
        if contract_type:
            params['contract_type'] = contract_type
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Fetching options for {underlying_ticker} as of {as_of_date} (attempt {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limiting
                if response.status_code == 429:
                    logger.warning(f"Rate limited. Waiting {self.retry_delay * 2} seconds...")
                    sleep(self.retry_delay * 2)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if data.get('status') == 'OK':
                    results_count = len(data.get('results', []))
                    logger.debug(f"Retrieved {results_count} option contracts for {underlying_ticker}")
                    
                    # Add metadata for tracking
                    data['fetch_timestamp'] = datetime.now().isoformat()
                    data['underlying_ticker'] = underlying_ticker
                    data['as_of_date'] = as_of_date
                    
                    return data
                else:
                    logger.error(f"API returned status: {data.get('status')} for {underlying_ticker}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {underlying_ticker} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    sleep(self.retry_delay)
                continue
            except Exception as e:
                logger.error(f"Unexpected error for {underlying_ticker}: {e}")
                break
        
        logger.error(f"Failed to fetch options for {underlying_ticker} after {self.max_retries} attempts")
        return None
    
    def scrape_options_for_date(self, target_date: str, ticker_limit: Optional[int] = None,
                               skip_existing: bool = True) -> Dict[str, Any]:
        """
        Scrape option contracts for all underlying tickers on a specific date.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            ticker_limit: Limit number of tickers to process (for testing)
            skip_existing: Skip if options data already exists for date/ticker
            
        Returns:
            Dictionary with scraping results and statistics
        """
        logger.info(f"Starting options scraping for {target_date}")
        
        results = {
            'date': target_date,
            'total_tickers_requested': 0,
            'successful_tickers': 0,
            'failed_tickers': 0,
            'skipped_tickers': 0,
            'total_contracts_inserted': 0,
            'tickers_processed': [],
            'failed_tickers_list': [],
            'skipped_tickers_list': []
        }
        
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
        
        for i, ticker in enumerate(underlying_tickers, 1):
            logger.info(f"Processing ticker {i}/{len(underlying_tickers)}: {ticker}")
            
            # Check if data already exists
            if skip_existing:
                existing_check_sql = """
                SELECT COUNT(*) as count 
                FROM option_contracts 
                WHERE date = %s AND symbol = %s
                """
                try:
                    from database.connection import db
                    existing_count = db.execute_query(existing_check_sql, (target_date, ticker))
                    if existing_count and existing_count[0]['count'] > 0:
                        logger.info(f"Skipping {ticker} - options data already exists")
                        results['skipped_tickers'] += 1
                        results['skipped_tickers_list'].append(ticker)
                        continue
                except Exception as e:
                    logger.warning(f"Could not check existing data for {ticker}: {e}")
            
            # Fetch option contracts
            options_data = self.get_option_contracts(ticker, target_date, expired=False)
            
            if options_data and options_data.get('results'):
                # Store in database using bulk loading
                try:
                    contract_count = len(options_data['results'])
                    logger.info(f"Storing {contract_count:,} option contracts for {ticker}...")
                    
                    success = self.bulk_loader.bulk_insert_option_contracts(
                        options_data, target_date, method='auto'
                    )
                    
                    if success:
                        results['successful_tickers'] += 1
                        results['total_contracts_inserted'] += contract_count
                        results['tickers_processed'].append(ticker)
                        logger.info(f"✓ Successfully stored {contract_count:,} contracts for {ticker}")
                    else:
                        results['failed_tickers'] += 1
                        results['failed_tickers_list'].append(ticker)
                        logger.error(f"Failed to store contracts for {ticker}")
                        
                except Exception as e:
                    results['failed_tickers'] += 1
                    results['failed_tickers_list'].append(ticker)
                    logger.error(f"Database error for {ticker}: {e}")
            
            elif options_data and not options_data.get('results'):
                # No options found for this ticker
                logger.info(f"No option contracts found for {ticker}")
                results['successful_tickers'] += 1
                results['tickers_processed'].append(ticker)
            
            else:
                # API call failed
                results['failed_tickers'] += 1
                results['failed_tickers_list'].append(ticker)
                logger.error(f"Failed to fetch options for {ticker}")
            
            # Rate limiting
            sleep(self.options_rate_limit_delay)
        
        return results
    
    def scrape_options_date_range(self, start_date: str, end_date: str, 
                                 ticker_limit: Optional[int] = None,
                                 skip_existing: bool = True) -> Dict[str, Any]:
        """
        Scrape option contracts for a date range.
        
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
            'total_contracts_inserted': 0,
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
                date_results = self.scrape_options_for_date(
                    date_str, ticker_limit=ticker_limit, skip_existing=skip_existing
                )
                
                if date_results['successful_tickers'] > 0 or date_results['skipped_tickers'] > 0:
                    combined_results['successful_dates'] += 1
                    combined_results['dates_processed'].append(date_str)
                    combined_results['total_contracts_inserted'] += date_results['total_contracts_inserted']
                    logger.info(f"✓ Completed options scraping for {date_str}")
                else:
                    combined_results['failed_dates'] += 1
                    combined_results['failed_dates_list'].append(date_str)
                    logger.error(f"Failed options scraping for {date_str}")
                    
            except Exception as e:
                combined_results['failed_dates'] += 1
                combined_results['failed_dates_list'].append(date_str)
                logger.error(f"Error processing options for {date_str}: {e}")
            
            current_date += timedelta(days=1)
        
        return combined_results


def main():
    """Main entry point for the options scraper."""
    
    parser = argparse.ArgumentParser(description='Polygon.io Options Contracts Scraper')
    
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
        scraper = PolygonOptionsContractsScraper()
        
        if args.recent is not None:
            # Scrape recent trading days
            if args.recent == 1:
                recent_date = scraper.get_most_recent_trading_day()
                logger.info(f"Scraping options for most recent trading day: {recent_date}")
                start_date, end_date = recent_date, recent_date
            else:
                start_date, end_date = scraper.get_recent_trading_days(args.recent)
                logger.info(f"Scraping options for most recent {args.recent} trading days: {start_date} to {end_date}")
            
            results = scraper.scrape_options_date_range(
                start_date, end_date,
                ticker_limit=args.ticker_limit,
                skip_existing=not args.force
            )
            
        elif args.start_date and args.end_date:
            # Scrape date range
            logger.info(f"Scraping options for date range: {args.start_date} to {args.end_date}")
            
            results = scraper.scrape_options_date_range(
                args.start_date, args.end_date,
                ticker_limit=args.ticker_limit,
                skip_existing=not args.force
            )
            
        else:
            logger.error("Please provide either --recent or both --start-date and --end-date")
            parser.print_help()
            return
        
        # Print summary
        print("\n" + "=" * 60)
        print("OPTIONS CONTRACTS SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Total dates requested: {results['total_dates_requested']}")
        print(f"Trading days found: {results['trading_days_found']}")
        print(f"Successful dates: {results['successful_dates']}")
        print(f"Failed dates: {results['failed_dates']}")
        print(f"Skipped dates: {results['skipped_dates']}")
        print(f"Total contracts inserted: {results['total_contracts_inserted']:,}")
        
        # Show performance statistics
        if hasattr(scraper, 'bulk_loader'):
            perf_stats = scraper.bulk_loader.get_performance_stats()
            if perf_stats['total_records_processed'] > 0:
                print(f"\nPerformance Statistics:")
                print(f"  Average speed: {perf_stats['average_records_per_second']:,.0f} contracts/sec")
                print(f"  Total processing time: {perf_stats['total_time_seconds']} seconds")
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
        logger.info("Options scraping interrupted by user")
    except Exception as e:
        logger.error(f"Options scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()
