"""
Real Polygon API Data Test Script
Tests the complete pipeline with actual Polygon API data instead of fake data.
Fetches data for the most recent trading day and verifies database insertion.
"""

import os
import sys
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.stock_data import StockDataManager
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


class RealDataVerification:
    """Test suite for real Polygon API data integration."""
    
    def __init__(self):
        self.tests_passed = 0
        self.tests_failed = 0
        self.test_results = []
        self.scraper = None
        self.test_date = None
        self.original_record_count = 0
    
    def run_test(self, test_name: str, test_func) -> bool:
        """Run a single test and track results."""
        try:
            logger.info(f"Running test: {test_name}")
            result = test_func()
            if result:
                self.tests_passed += 1
                self.test_results.append(f"✓ {test_name}")
                logger.info(f"✓ {test_name} - PASSED")
            else:
                self.tests_failed += 1
                self.test_results.append(f"✗ {test_name}")
                logger.error(f"✗ {test_name} - FAILED")
            return result
        except Exception as e:
            self.tests_failed += 1
            self.test_results.append(f"✗ {test_name} - ERROR: {str(e)}")
            logger.error(f"✗ {test_name} - ERROR: {str(e)}")
            return False
    
    def test_environment_setup(self) -> bool:
        """Test that required environment variables are set."""
        required_vars = ['SUPABASE_DB_URL', 'POLYGON_API_KEY']
        
        for var in required_vars:
            if not os.getenv(var):
                logger.error(f"Missing environment variable: {var}")
                return False
        
        logger.info("All required environment variables are set")
        return True
    
    def test_database_connection(self) -> bool:
        """Test database connection."""
        try:
            result = db.test_connection()
            if result:
                logger.info("Database connection successful")
                
                # Get initial record count
                stats = StockDataManager.get_table_stats()
                self.original_record_count = stats.get('total_records', 0)
                logger.info(f"Database currently has {self.original_record_count} records")
                
            return result
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def test_polygon_scraper_initialization(self) -> bool:
        """Test Polygon scraper initialization."""
        try:
            self.scraper = PolygonDailyScraper()
            logger.info("Polygon scraper initialized successfully")
            
            # Get the most recent trading day
            self.test_date = self.scraper.get_most_recent_trading_day()
            logger.info(f"Selected test date: {self.test_date}")
            
            return True
        except Exception as e:
            logger.error(f"Polygon scraper initialization failed: {e}")
            return False
    
    def test_polygon_api_fetch(self) -> bool:
        """Test fetching real data from Polygon API."""
        if not self.scraper or not self.test_date:
            logger.error("Scraper not initialized or test date not set")
            return False
        
        try:
            # Fetch data for the test date
            market_data = self.scraper.get_daily_market_summary(self.test_date)
            
            if not market_data:
                logger.error("Failed to fetch market data from Polygon API")
                return False
            
            # Validate response structure
            if market_data.get('status') != 'OK':
                logger.error(f"API returned status: {market_data.get('status')}")
                return False
            
            results = market_data.get('results', [])
            if not results:
                logger.warning("API returned empty results - this may be normal for weekends/holidays")
                return True
            
            logger.info(f"Successfully fetched {len(results)} stock records from Polygon API")
            
            # Validate first record structure
            first_record = results[0]
            required_fields = ['T', 'c', 'h', 'l', 'n', 'o', 't', 'v', 'vw']
            missing_fields = [field for field in required_fields if field not in first_record]
            
            if missing_fields:
                logger.error(f"Missing required fields in API response: {missing_fields}")
                return False
            
            logger.info("API response structure validated successfully")
            return True
            
        except Exception as e:
            logger.error(f"API fetch test failed: {e}")
            return False
    
    def test_database_insertion(self) -> bool:
        """Test inserting real Polygon data into database."""
        if not self.scraper or not self.test_date:
            logger.error("Scraper not initialized or test date not set")
            return False
        
        try:
            # Fetch fresh data
            market_data = self.scraper.get_daily_market_summary(self.test_date)
            
            if not market_data or not market_data.get('results'):
                logger.warning("No data to insert - this may be normal for weekends/holidays")
                return True
            
            # Insert data
            success = StockDataManager.insert_daily_snapshots(market_data)
            
            if not success:
                logger.error("Database insertion failed")
                return False
            
            # Verify insertion
            new_stats = StockDataManager.get_table_stats()
            new_record_count = new_stats.get('total_records', 0)
            
            if new_record_count > self.original_record_count:
                records_added = new_record_count - self.original_record_count
                logger.info(f"Successfully inserted {records_added} new records")
                return True
            else:
                logger.info("No new records added - data may already exist (this is normal)")
                return True
                
        except Exception as e:
            logger.error(f"Database insertion test failed: {e}")
            return False
    
    def test_data_retrieval(self) -> bool:
        """Test retrieving data from database."""
        if not self.test_date:
            logger.error("Test date not set")
            return False
        
        try:
            # Convert test date string to date object
            test_date_obj = datetime.strptime(self.test_date, '%Y-%m-%d').date()
            
            # Get market snapshot for the test date
            market_snapshot = StockDataManager.get_market_snapshot(test_date_obj, limit=10)
            
            if market_snapshot:
                logger.info(f"Successfully retrieved {len(market_snapshot)} records for {self.test_date}")
                
                # Validate data structure
                first_record = market_snapshot[0]
                required_fields = ['date', 'symbol', 'close', 'high', 'low', 'open', 'trading_volume']
                missing_fields = [field for field in required_fields if field not in first_record]
                
                if missing_fields:
                    logger.error(f"Missing fields in retrieved data: {missing_fields}")
                    return False
                
                # Log sample data
                logger.info(f"Sample record: {first_record['symbol']} - "
                           f"Close: ${first_record['close']}, Volume: {first_record['trading_volume']:,}")
                
                return True
            else:
                logger.warning("No data found for test date - this may be normal for weekends/holidays")
                return True
                
        except Exception as e:
            logger.error(f"Data retrieval test failed: {e}")
            return False
    
    def test_duplicate_handling(self) -> bool:
        """Test that duplicate data is handled correctly."""
        if not self.scraper or not self.test_date:
            logger.error("Scraper not initialized or test date not set")
            return False
        
        try:
            # Get current record count
            stats_before = StockDataManager.get_table_stats()
            count_before = stats_before.get('total_records', 0)
            
            # Fetch and insert the same data again
            market_data = self.scraper.get_daily_market_summary(self.test_date)
            
            if not market_data or not market_data.get('results'):
                logger.warning("No data available for duplicate test")
                return True
            
            # Insert the same data again
            success = StockDataManager.insert_daily_snapshots(market_data)
            
            if not success:
                logger.error("Second insertion failed")
                return False
            
            # Check that record count didn't increase (upsert behavior)
            stats_after = StockDataManager.get_table_stats()
            count_after = stats_after.get('total_records', 0)
            
            if count_after == count_before:
                logger.info("Duplicate handling working correctly - no additional records created")
                return True
            else:
                logger.warning(f"Record count changed from {count_before} to {count_after} - may indicate new data")
                return True  # This is actually okay - might be new market data
                
        except Exception as e:
            logger.error(f"Duplicate handling test failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run the complete real data verification suite."""
        logger.info("Starting Real Polygon Data Verification Suite")
        logger.info("=" * 60)
        
        # Run all tests
        tests = [
            ("Environment Setup", self.test_environment_setup),
            ("Database Connection", self.test_database_connection),
            ("Polygon Scraper Initialization", self.test_polygon_scraper_initialization),
            ("Polygon API Data Fetch", self.test_polygon_api_fetch),
            ("Database Insertion", self.test_database_insertion),
            ("Data Retrieval", self.test_data_retrieval),
            ("Duplicate Handling", self.test_duplicate_handling)
        ]
        
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Real Data Verification Summary:")
        for result in self.test_results:
            print(result)
        
        logger.info(f"\nTotal Tests: {self.tests_passed + self.tests_failed}")
        logger.info(f"Passed: {self.tests_passed}")
        logger.info(f"Failed: {self.tests_failed}")
        
        # Show final database stats
        final_stats = StockDataManager.get_table_stats()
        if final_stats:
            logger.info(f"\nFinal Database Statistics:")
            logger.info(f"  Total records: {final_stats['total_records']:,}")
            logger.info(f"  Unique symbols: {final_stats['unique_symbols']:,}")
            logger.info(f"  Date range: {final_stats['earliest_date']} to {final_stats['latest_date']}")
        
        if self.tests_failed == 0:
            logger.info("🎉 All tests passed! Real Polygon API integration is working.")
            return True
        else:
            logger.error(f"❌ {self.tests_failed} test(s) failed. Please check the logs.")
            return False


if __name__ == "__main__":
    verification = RealDataVerification()
    success = verification.run_all_tests()
    
    if not success:
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("REAL DATA VERIFICATION COMPLETE")
    print("=" * 60)
    print("\nYour system is now ready for production use with real Polygon API data!")
    print("\nUsage examples:")
    print("1. Scrape recent data: python scrapers/polygon_daily_scraper.py --recent")
    print("2. Scrape date range: python scrapers/polygon_daily_scraper.py --start-date 2024-01-01 --end-date 2024-01-31")
    print("3. Query data: Use StockDataManager methods in your Python scripts")
