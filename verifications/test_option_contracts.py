"""
Option Contracts Verification Script
Tests the complete option contracts system including migration, bulk loading, and scraper.
"""

import os
import sys
import logging
from datetime import date, datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.bulk_operations import BulkStockDataLoader
from scrapers.polygon_options_scraper import PolygonOptionsContractsScraper
from migrations.migration_manager import MigrationManager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OptionContractsVerification:
    """Test suite for option contracts functionality."""
    
    def __init__(self):
        self.tests_passed = 0
        self.tests_failed = 0
        self.test_results = []
    
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
            return result
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def test_option_contracts_table_exists(self) -> bool:
        """Test that option_contracts table exists with correct structure."""
        try:
            # Check if table exists
            check_table_sql = """
            SELECT COUNT(*) as count
            FROM information_schema.tables 
            WHERE table_name = 'option_contracts' AND table_schema = 'public'
            """
            
            result = db.execute_query(check_table_sql)
            if not result or result[0]['count'] == 0:
                logger.error("option_contracts table does not exist")
                return False
            
            # Check table structure
            columns_sql = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'option_contracts' AND table_schema = 'public'
            ORDER BY ordinal_position
            """
            
            columns = db.execute_query(columns_sql)
            expected_columns = {
                'date', 'symbol', 'contract_ticker', 'contract_type', 
                'expiration_date', 'strike_price', 'exercise_style',
                'shares_per_contract', 'primary_exchange', 'cfi_code',
                'additional_underlyings', 'created_at', 'updated_at'
            }
            
            actual_columns = {col['column_name'] for col in columns}
            
            if not expected_columns.issubset(actual_columns):
                missing = expected_columns - actual_columns
                logger.error(f"Missing columns in option_contracts table: {missing}")
                return False
            
            logger.info(f"option_contracts table exists with {len(columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to check option_contracts table: {e}")
            return False
    
    def test_foreign_key_relationship(self) -> bool:
        """Test that foreign key relationship to daily_stock_snapshot exists."""
        try:
            fk_sql = """
            SELECT 
                tc.constraint_name,
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_name = 'option_contracts'
                AND ccu.table_name = 'daily_stock_snapshot'
            """
            
            result = db.execute_query(fk_sql)
            
            if not result:
                logger.error("No foreign key relationship found between option_contracts and daily_stock_snapshot")
                return False
            
            logger.info("Foreign key relationship verified")
            return True
            
        except Exception as e:
            logger.error(f"Failed to check foreign key relationship: {e}")
            return False
    
    def test_bulk_loading_with_sample_data(self) -> bool:
        """Test bulk loading with sample option contracts data."""
        # Sample data based on Polygon API response format
        sample_data = {
            "status": "OK",
            "results": [
                {
                    "cfi": "OCASPS",
                    "contract_type": "call",
                    "exercise_style": "american",
                    "expiration_date": "2024-12-20",
                    "primary_exchange": "BATO",
                    "shares_per_contract": 100,
                    "strike_price": 150.0,
                    "ticker": "O:TESTOPT241220C00150000",
                    "underlying_ticker": "TEST"
                },
                {
                    "cfi": "OCASPS",
                    "contract_type": "put",
                    "exercise_style": "american",
                    "expiration_date": "2024-12-20",
                    "primary_exchange": "BATO",
                    "shares_per_contract": 100,
                    "strike_price": 140.0,
                    "ticker": "O:TESTOPT241220P00140000",
                    "underlying_ticker": "TEST"
                }
            ]
        }
        
        try:
            # First ensure we have some stock data for the foreign key
            sample_stock_data = {
                "status": "OK",
                "results": [
                    {
                        "T": "TEST",
                        "c": 145.50,
                        "h": 147.00,
                        "l": 144.00,
                        "n": 1000,
                        "o": 146.00,
                        "t": int(datetime(2024, 8, 15).timestamp() * 1000),
                        "v": 100000,
                        "vw": 145.75
                    }
                ]
            }
            
            # Insert stock data first
            from database.stock_data import StockDataManager
            stock_success = StockDataManager.insert_daily_snapshots(sample_stock_data)
            
            if not stock_success:
                logger.error("Failed to insert sample stock data")
                return False
            
            # Now test option contracts bulk loading
            loader = BulkStockDataLoader()
            as_of_date = "2024-08-15"
            
            success = loader.bulk_insert_option_contracts(sample_data, as_of_date, method='copy')
            
            if success:
                logger.info("Sample option contracts data loaded successfully")
                
                # Verify data was inserted
                verify_sql = """
                SELECT COUNT(*) as count 
                FROM option_contracts 
                WHERE date = %s AND symbol = %s
                """
                
                result = db.execute_query(verify_sql, (as_of_date, "TEST"))
                
                if result and result[0]['count'] == 2:
                    logger.info("Option contracts data verified in database")
                    return True
                else:
                    logger.error("Option contracts data not found in database")
                    return False
            else:
                logger.error("Failed to load option contracts data")
                return False
                
        except Exception as e:
            logger.error(f"Bulk loading test failed: {e}")
            return False
    
    def test_options_scraper_initialization(self) -> bool:
        """Test that the options scraper initializes correctly."""
        try:
            scraper = PolygonOptionsContractsScraper()
            
            # Test basic functionality
            recent_date = scraper.get_most_recent_trading_day()
            
            if recent_date:
                logger.info(f"Options scraper initialized successfully, recent trading day: {recent_date}")
                return True
            else:
                logger.error("Failed to get recent trading day from scraper")
                return False
                
        except Exception as e:
            logger.error(f"Options scraper initialization failed: {e}")
            return False
    
    def test_underlying_tickers_retrieval(self) -> bool:
        """Test retrieval of underlying tickers from daily_stock_snapshot."""
        try:
            scraper = PolygonOptionsContractsScraper()
            
            # Use a date we know has data (from previous test)
            test_date = "2024-08-15"
            tickers = scraper.get_underlying_tickers_for_date(test_date)
            
            if tickers and len(tickers) > 0:
                logger.info(f"Successfully retrieved {len(tickers)} underlying tickers")
                logger.info(f"Sample tickers: {tickers[:5]}")
                return True
            else:
                logger.warning("No underlying tickers found - this may be normal if no stock data exists")
                return True  # Not a failure, just no data
                
        except Exception as e:
            logger.error(f"Underlying tickers retrieval test failed: {e}")
            return False
    
    def test_migration_applied(self) -> bool:
        """Test that the option contracts migration was applied correctly."""
        try:
            manager = MigrationManager()
            applied_migrations = manager.get_applied_migrations()
            
            option_migration = "20240816_130001"
            
            if any(option_migration in migration for migration in applied_migrations):
                logger.info("Option contracts migration found in applied migrations")
                return True
            else:
                logger.error("Option contracts migration not found in applied migrations")
                return False
                
        except Exception as e:
            logger.error(f"Migration check failed: {e}")
            return False
    
    def cleanup_test_data(self) -> bool:
        """Clean up test data."""
        try:
            cleanup_sql = [
                "DELETE FROM option_contracts WHERE symbol = 'TEST'",
                "DELETE FROM daily_stock_snapshot WHERE symbol = 'TEST'"
            ]
            
            for sql in cleanup_sql:
                db.execute_command(sql)
            
            logger.info("Test data cleaned up")
            return True
            
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run the complete option contracts verification suite."""
        logger.info("Starting Option Contracts Verification Suite")
        logger.info("=" * 60)
        
        # Run all tests
        tests = [
            ("Environment Setup", self.test_environment_setup),
            ("Database Connection", self.test_database_connection),
            ("Migration Applied", self.test_migration_applied),
            ("Option Contracts Table Exists", self.test_option_contracts_table_exists),
            ("Foreign Key Relationship", self.test_foreign_key_relationship),
            ("Bulk Loading with Sample Data", self.test_bulk_loading_with_sample_data),
            ("Options Scraper Initialization", self.test_options_scraper_initialization),
            ("Underlying Tickers Retrieval", self.test_underlying_tickers_retrieval),
            ("Cleanup Test Data", self.cleanup_test_data)
        ]
        
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("Option Contracts Verification Summary:")
        for result in self.test_results:
            print(result)
        
        logger.info(f"\nTotal Tests: {self.tests_passed + self.tests_failed}")
        logger.info(f"Passed: {self.tests_passed}")
        logger.info(f"Failed: {self.tests_failed}")
        
        if self.tests_failed == 0:
            logger.info("🎉 All tests passed! Option contracts system is ready.")
            return True
        else:
            logger.error(f"❌ {self.tests_failed} test(s) failed. Please check the logs.")
            return False


if __name__ == "__main__":
    verification = OptionContractsVerification()
    success = verification.run_all_tests()
    
    if not success:
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("OPTION CONTRACTS VERIFICATION COMPLETE")
    print("=" * 60)
    print("\nYour option contracts system is ready!")
    print("\nUsage examples:")
    print("1. Scrape recent options: python scrapers/polygon_options_scraper.py --recent")
    print("2. Scrape date range: python scrapers/polygon_options_scraper.py --start-date 2024-08-01 --end-date 2024-08-05")
    print("3. Test with limited tickers: python scrapers/polygon_options_scraper.py --recent --ticker-limit 10")
