"""
Polygon.io Daily Market Summary Scraper
Fetches daily OHLC data for all U.S. stocks from Polygon API.

Based on: https://polygon.io/docs/rest/stocks/aggregates/daily-market-summary
Endpoint: GET /v2/aggs/grouped/locale/us/market/stocks/{date}
"""

import os
import sys
import logging
import requests
import argparse
from datetime import date, datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import pytz
from time import sleep

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.stock_data import StockDataManager
from database.core.bulk_operations import BulkStockDataLoader
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PolygonDailyScraper:
    """
    Scraper for Polygon.io Daily Market Summary API.
    Handles rate limiting, error recovery, and data validation.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the scraper.
        
        Args:
            api_key: Polygon API key. If None, reads from POLYGON_API_KEY env var.
        """
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not found in environment variables")
        
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()
        
        # Rate limiting (free tier: 5 requests per minute)
        self.rate_limit_delay = 12  # seconds between requests
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        
        # Performance tracking
        self.bulk_loader = BulkStockDataLoader()
    
    def get_market_holidays(self) -> List[str]:
        """
        Get list of market holidays to skip.
        
        Returns:
            List of holiday dates in YYYY-MM-DD format
        """
        # Basic U.S. market holidays (you can expand this)
        current_year = datetime.now(pytz.timezone('US/Eastern')).year
        holidays = [
            f"{current_year}-01-01",  # New Year's Day
            f"{current_year}-06-19",  # Juneteenth
            f"{current_year}-07-04",  # Independence Day
            f"{current_year}-12-25",  # Christmas Day

        ]
        return holidays
    
    def is_trading_day(self, check_date: date) -> bool:
        """
        Check if a given date is a trading day.
        
        Args:
            check_date: Date to check
            
        Returns:
            True if it's a trading day, False otherwise
        """
        # Skip weekends
        if check_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Skip holidays
        date_str = check_date.strftime('%Y-%m-%d')
        if date_str in self.get_market_holidays():
            return False
        
        return True
    
    def get_daily_market_summary(self, market_date: str, include_otc: bool = False, 
                                adjusted: bool = True) -> Optional[Dict[str, Any]]:
        """
        Fetch daily market summary from Polygon API.
        
        Args:
            market_date: Date in YYYY-MM-DD format
            include_otc: Include OTC securities
            adjusted: Whether results are adjusted for splits
            
        Returns:
            API response dict or None if failed
        """
        url = f"{self.base_url}/v2/aggs/grouped/locale/us/market/stocks/{market_date}"
        params = {
            'adjusted': str(adjusted).lower(),
            'include_otc': str(include_otc).lower(),
            'apikey': self.api_key
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Fetching market data for {market_date} (attempt {attempt + 1})")
                
                response = self.session.get(url, params=params, timeout=30)
                
                # Check for rate limiting
                if response.status_code == 429:
                    logger.warning(f"Rate limited. Waiting {self.retry_delay * 2} seconds...")
                    sleep(self.retry_delay * 2)
                    continue
                
                response.raise_for_status()
                data = response.json()
                
                if data.get('status') == 'OK':
                    results_count = data.get('resultsCount', 0)
                    logger.info(f"Successfully fetched {results_count} stock records for {market_date}")
                    
                    # Add metadata for tracking
                    data['fetch_timestamp'] = datetime.now(pytz.timezone('US/Eastern')).isoformat()
                    data['market_date'] = market_date
                    
                    return data
                else:
                    st = data.get('status')
                    if st == 'DELAYED':
                        logger.info(f"Daily data not ready yet (status=DELAYED) for {market_date}; will skip for now")
                        return {'status': 'DELAYED', 'market_date': market_date}
                    logger.error(f"API returned status: {st} for {market_date}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {market_date} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    sleep(self.retry_delay)
                continue
            except Exception as e:
                logger.error(f"Unexpected error for {market_date}: {e}")
                break
        
        logger.error(f"Failed to fetch data for {market_date} after {self.max_retries} attempts")
        return None
    
    def scrape_date_range(self, start_date: str, end_date: str, 
                         include_otc: bool = False, skip_existing: bool = True) -> Dict[str, Any]:
        """
        Scrape market data for a date range.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            include_otc: Include OTC securities
            skip_existing: Skip dates that already have data in the database
            
        Returns:
            Dictionary with scraping results and statistics
        """
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        if start > end:
            raise ValueError("Start date must be before or equal to end date")
        
        results = {
            'total_dates_requested': 0,
            'trading_days_found': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'skipped_dates': 0,
            'total_records_inserted': 0,
            'dates_processed': [],
            'failed_dates': [],
            'skipped_dates_list': []
        }
        
        current_date = start
        
        while current_date <= end:
            results['total_dates_requested'] += 1
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Skip non-trading days
            if not self.is_trading_day(current_date):
                logger.info(f"Skipping {date_str} - not a trading day")
                results['skipped_dates'] += 1
                results['skipped_dates_list'].append(date_str)
                current_date += timedelta(days=1)
                continue
            
            results['trading_days_found'] += 1
            
            # Check if data already exists
            if skip_existing:
                existing_count = len(StockDataManager.get_market_snapshot(current_date, limit=1))
                if existing_count > 0:
                    logger.info(f"Skipping {date_str} - data already exists")
                    results['skipped_dates'] += 1
                    results['skipped_dates_list'].append(date_str)
                    current_date += timedelta(days=1)
                    continue
            
            # Fetch data
            market_data = self.get_daily_market_summary(date_str, include_otc=include_otc)
            
            if isinstance(market_data, dict) and market_data.get('status') == 'DELAYED':
                logger.info(f"Skipping {date_str} - daily snapshot DELAYED")
                results['skipped_dates'] += 1
                results['skipped_dates_list'].append(date_str)
            elif market_data:
                # Store in database using high-performance bulk loading
                try:
                    record_count = market_data.get('resultsCount', 0)
                    logger.info(f"Starting bulk insert of {record_count:,} records for {date_str}...")
                    
                    # Use bulk loading for better performance
                    success = self.bulk_loader.bulk_insert_daily_snapshots(market_data, method='auto')
                    
                    if success:
                        results['successful_scrapes'] += 1
                        results['total_records_inserted'] += record_count
                        results['dates_processed'].append(date_str)
                        logger.info(f"✓ Successfully bulk-loaded {record_count:,} records for {date_str}")
                    else:
                        results['failed_scrapes'] += 1
                        results['failed_dates'].append(date_str)
                        logger.error(f"Failed to store data for {date_str}")
                        
                except Exception as e:
                    results['failed_scrapes'] += 1
                    results['failed_dates'].append(date_str)
                    logger.error(f"Database error for {date_str}: {e}")
            else:
                results['failed_scrapes'] += 1
                results['failed_dates'].append(date_str)
            
            # Rate limiting
            sleep(self.rate_limit_delay)
            current_date += timedelta(days=1)
        
        return results
    
    def get_most_recent_trading_day(self) -> str:
        """
        Get the most recent trading day.
        
        Returns:
            Date string in YYYY-MM-DD format
        """
        current_date = date.today()
        
        # Go back up to 7 days to find the most recent trading day
        for i in range(7):
            check_date = current_date - timedelta(days=i)
            if self.is_trading_day(check_date):
                return check_date.strftime('%Y-%m-%d')
        
        # Fallback to 5 days ago if no trading day found
        fallback = current_date - timedelta(days=5)
        return fallback.strftime('%Y-%m-%d')
    
    def get_recent_trading_days(self, num_days: int) -> Tuple[str, str]:
        """
        Get date range for the most recent N trading days.
        
        Args:
            num_days: Number of business/trading days to go back
            
        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        if num_days <= 0:
            raise ValueError("Number of days must be positive")
        
        end_date = date.today()
        found_days = 0
        current_date = end_date
        
        # Find the most recent trading day as end date
        for i in range(10):  # Look back up to 10 days to find recent trading day
            check_date = current_date - timedelta(days=i)
            if self.is_trading_day(check_date):
                end_date = check_date
                break
        
        # Now count backwards to find start date
        found_days = 1  # We found the end date
        current_date = end_date - timedelta(days=1)
        
        while found_days < num_days:
            if self.is_trading_day(current_date):
                found_days += 1
                if found_days == num_days:
                    start_date = current_date
                    break
            current_date -= timedelta(days=1)
            
            # Safety check - don't go back more than 2 years
            if (end_date - current_date).days > 730:
                break
        else:
            # If we didn't find enough trading days, use what we found
            start_date = current_date
        
        logger.info(f"Recent {num_days} trading days: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def main():
    """Main entry point for the scraper."""
    
    parser = argparse.ArgumentParser(description='Polygon.io Daily Market Data Scraper')
    
    parser.add_argument('--start-date', type=str, 
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--recent', type=int, nargs='?', const=1, metavar='DAYS',
                       help='Scrape recent trading days (default: 1, e.g., --recent 60)')
    parser.add_argument('--include-otc', action='store_true',
                       help='Include OTC securities')
    parser.add_argument('--force', action='store_true',
                       help='Force re-scrape even if data exists')
    
    args = parser.parse_args()
    
    try:
        scraper = PolygonDailyScraper()
        
        if args.recent is not None:
            # Scrape recent trading days
            if args.recent == 1:
                # Single day - use existing method for compatibility
                recent_date = scraper.get_most_recent_trading_day()
                logger.info(f"Scraping most recent trading day: {recent_date}")
                start_date, end_date = recent_date, recent_date
            else:
                # Multiple days - use new method
                start_date, end_date = scraper.get_recent_trading_days(args.recent)
                logger.info(f"Scraping most recent {args.recent} trading days: {start_date} to {end_date}")
            
            results = scraper.scrape_date_range(
                start_date, end_date,
                include_otc=args.include_otc,
                skip_existing=not args.force
            )
            
        elif args.start_date and args.end_date:
            # Scrape date range
            logger.info(f"Scraping date range: {args.start_date} to {args.end_date}")
            
            results = scraper.scrape_date_range(
                args.start_date, args.end_date,
                include_otc=args.include_otc,
                skip_existing=not args.force
            )
            
        else:
            logger.error("Please provide either --recent or both --start-date and --end-date")
            parser.print_help()
            return
        
        # Print summary
        print("\n" + "=" * 60)
        print("SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Total dates requested: {results['total_dates_requested']}")
        print(f"Trading days found: {results['trading_days_found']}")
        print(f"Successful scrapes: {results['successful_scrapes']}")
        print(f"Failed scrapes: {results['failed_scrapes']}")
        print(f"Skipped dates: {results['skipped_dates']}")
        print(f"Total records inserted: {results['total_records_inserted']:,}")
        
        # Show performance statistics
        if hasattr(scraper, 'bulk_loader'):
            perf_stats = scraper.bulk_loader.get_performance_stats()
            if perf_stats['total_records_processed'] > 0:
                print(f"\nPerformance Statistics:")
                print(f"  Average speed: {perf_stats['average_records_per_second']:,.0f} records/sec")
                print(f"  Total processing time: {perf_stats['total_time_seconds']} seconds")
                print(f"  Success rate: {perf_stats['success_rate_percent']}%")
        
        if results['dates_processed']:
            print(f"\nSuccessfully processed dates:")
            for date_str in results['dates_processed']:
                print(f"  ✓ {date_str}")
        
        if results['failed_dates']:
            print(f"\nFailed dates:")
            for date_str in results['failed_dates']:
                print(f"  ✗ {date_str}")
        
        # Show database stats
        stats = StockDataManager.get_table_stats()
        print(f"\nDatabase Statistics:")
        print(f"  Total records: {stats['total_records']:,}")
        print(f"  Unique symbols: {stats['unique_symbols']:,}")
        print(f"  Date range: {stats['earliest_date']} to {stats['latest_date']}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()
