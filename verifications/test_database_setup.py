"""
Verification script to test database connection and table setup.
Tests the complete database infrastructure for the insider trades project.
"""

import logging
import sys
import os
from datetime import date, datetime
from typing import Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.stock_data import StockDataManager
from migrations.20240101_000001_create_daily_stock_snapshot import create_daily_stock_snapshot_table, get_table_info

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseVerification:
    """Comprehensive database verification suite."""
    
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
    
    def test_environment_variables(self) -> bool:
        """Test that required environment variables are set."""
        required_vars = ['SUPABASE_DB_URL', 'POLYGON_API_KEY']
        
        for var in required_vars:
            if not os.getenv(var):
                logger.error(f"Missing environment variable: {var}")
                return False
        
        logger.info("All required environment variables are set")
        return True
    
    def test_database_connection(self) -> bool:
        """Test basic database connection."""
        try:
            result = db.test_connection()
            if result:
                logger.info("Database connection successful")
            else:
                logger.error("Database connection failed")
            return result
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            return False
    
    def test_table_creation(self) -> bool:
        """Test table creation and schema."""
        try:
            # Create the table
            create_daily_stock_snapshot_table()
            
            # Verify table exists and has correct structure
            table_info = get_table_info()
            
            expected_columns = {
                'date', 'symbol', 'close', 'high', 'low', 
                'transaction_volume', 'open', 'trading_volume', 
                'weighted_average_price', 'created_at', 'updated_at'
            }
            
            actual_columns = {col['column_name'] for col in table_info}
            
            if expected_columns.issubset(actual_columns):
                logger.info(f"Table created with {len(table_info)} columns")
                return True
            else:
                missing = expected_columns - actual_columns
                logger.error(f"Missing columns: {missing}")
                return False
                
        except Exception as e:
            logger.error(f"Table creation failed: {e}")
            return False
    
    def test_data_insertion(self) -> bool:
        """Test data insertion with sample Polygon API response."""
        # Sample data based on your provided Polygon API response
        sample_polygon_response = {
            "adjusted": True,
            "queryCount": 3,
            "request_id": "test_request",
            "results": [
                {
                    "T": "TEST1",
                    "c": 25.9102,
                    "h": 26.25,
                    "l": 25.91,
                    "n": 74,
                    "o": 26.07,
                    "t": 1602705600000,  # 2020-10-15
                    "v": 4369,
                    "vw": 26.0407
                },
                {
                    "T": "TEST2",
                    "c": 23.4,
                    "h": 24.763,
                    "l": 22.65,
                    "n": 1096,
                    "o": 24.5,
                    "t": 1602705600000,  # 2020-10-15
                    "v": 25933,
                    "vw": 23.493
                }
            ],
            "resultsCount": 2,
            "status": "OK"
        }
        
        try:
            # Insert sample data
            result = StockDataManager.insert_daily_snapshots(sample_polygon_response)
            
            if result:
                logger.info("Sample data inserted successfully")
                
                # Verify data was inserted
                test_date = date(2020, 10, 15)
                snapshot = StockDataManager.get_daily_snapshot("TEST1", test_date)
                
                if snapshot and snapshot['symbol'] == 'TEST1':
                    logger.info("Data retrieval verified")
                    return True
                else:
                    logger.error("Data retrieval failed")
                    return False
            else:
                logger.error("Data insertion failed")
                return False
                
        except Exception as e:
            logger.error(f"Data insertion test failed: {e}")
            return False
    
    def test_query_operations(self) -> bool:
        """Test various query operations."""
        try:
            # Test table statistics
            stats = StockDataManager.get_table_stats()
            
            if stats and 'total_records' in stats:
                logger.info(f"Table stats: {stats['total_records']} records, "
                           f"{stats['unique_symbols']} symbols")
                
                # Test latest snapshots
                latest = StockDataManager.get_latest_snapshots(limit=5)
                logger.info(f"Retrieved {len(latest)} latest snapshots")
                
                return True
            else:
                logger.error("Failed to get table statistics")
                return False
                
        except Exception as e:
            logger.error(f"Query operations test failed: {e}")
            return False
    
    def cleanup_test_data(self) -> bool:
        """Clean up test data."""
        try:
            cleanup_sql = "DELETE FROM daily_stock_snapshot WHERE symbol LIKE 'TEST%'"
            db.execute_command(cleanup_sql)
            logger.info("Test data cleaned up")
            return True
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            return False
    
    def run_all_tests(self):
        """Run the complete verification suite."""
        logger.info("Starting Database Verification Suite")
        logger.info("=" * 50)
        
        # Run all tests
        tests = [
            ("Environment Variables", self.test_environment_variables),
            ("Database Connection", self.test_database_connection),
            ("Table Creation", self.test_table_creation),
            ("Data Insertion", self.test_data_insertion),
            ("Query Operations", self.test_query_operations),
            ("Cleanup Test Data", self.cleanup_test_data)
        ]
        
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        # Print summary
        logger.info("=" * 50)
        logger.info("Verification Summary:")
        for result in self.test_results:
            print(result)
        
        logger.info(f"\nTotal Tests: {self.tests_passed + self.tests_failed}")
        logger.info(f"Passed: {self.tests_passed}")
        logger.info(f"Failed: {self.tests_failed}")
        
        if self.tests_failed == 0:
            logger.info("🎉 All tests passed! Database setup is ready.")
            return True
        else:
            logger.error(f"❌ {self.tests_failed} test(s) failed. Please check the logs.")
            return False


if __name__ == "__main__":
    verification = DatabaseVerification()
    success = verification.run_all_tests()
    
    if not success:
        sys.exit(1)
    
    print("\n" + "=" * 50)
    print("DATABASE SETUP VERIFICATION COMPLETE")
    print("=" * 50)
    print("\nYour Supabase database is now ready to receive Polygon API data!")
    print("\nNext steps:")
    print("1. Use StockDataManager.insert_daily_snapshots() to insert Polygon API responses")
    print("2. Query data using the various get_* methods in StockDataManager")
    print("3. Monitor your data with StockDataManager.get_table_stats()")
