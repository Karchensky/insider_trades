"""
Bulk Loading Performance Test
Tests and compares different bulk loading methods for database insertion.
"""

import os
import sys
import time
import logging
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.stock_data import StockDataManager
from database.bulk_operations import BulkStockDataLoader, bulk_insert_polygon_data
from scrapers.polygon_daily_scraper import PolygonDailyScraper

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_test_data(record_count: int = 1000):
    """Generate test Polygon API response data."""
    import random
    
    results = []
    symbols = [f"TEST{i:04d}" for i in range(min(record_count, 1000))]
    base_timestamp = int(datetime(2024, 1, 15).timestamp() * 1000)
    
    for i in range(record_count):
        # Use unique symbol per record to avoid duplicate key issues in tests
        symbol = f"TEST{i:06d}"
        base_price = random.uniform(10, 500)
        
        result = {
            "T": symbol,
            "c": round(base_price, 2),
            "h": round(base_price * random.uniform(1.0, 1.1), 2),
            "l": round(base_price * random.uniform(0.9, 1.0), 2),
            "n": random.randint(100, 10000),
            "o": round(base_price * random.uniform(0.95, 1.05), 2),
            "t": base_timestamp,
            "v": random.randint(1000, 1000000),
            "vw": round(base_price * random.uniform(0.98, 1.02), 2)
        }
        results.append(result)
    
    return {
        "status": "OK",
        "resultsCount": record_count,
        "results": results
    }


def test_loading_methods():
    """Test and compare different loading methods."""
    
    print("=" * 80)
    print("BULK LOADING PERFORMANCE TEST")
    print("=" * 80)
    
    # Test with different record counts
    test_sizes = [100, 1000, 5000]
    
    for size in test_sizes:
        print(f"\n📊 Testing with {size:,} records:")
        print("-" * 50)
        
        # Generate test data
        test_data = generate_test_data(size)
        
        # Clean up any existing test data
        cleanup_sql = "DELETE FROM daily_stock_snapshot WHERE symbol LIKE 'TEST%'"
        try:
            from database.connection import db
            db.execute_command(cleanup_sql)
            logger.info("Cleaned up existing test data")
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
        
        # Test 1: Original method (execute_many)
        print("🔸 Testing original method (execute_many)...")
        start_time = time.time()
        try:
            success = StockDataManager.insert_daily_snapshots(test_data)
            end_time = time.time()
            if success:
                elapsed = end_time - start_time
                rate = size / elapsed
                print(f"  ✓ Original method: {elapsed:.2f}s ({rate:.0f} records/sec)")
            else:
                print("  ✗ Original method failed")
        except Exception as e:
            print(f"  ✗ Original method error: {e}")
        
        # Clean up for next test
        try:
            db.execute_command(cleanup_sql)
        except:
            pass
        
        # Test 2: execute_values method
        print("🔸 Testing execute_values method...")
        loader = BulkStockDataLoader()
        start_time = time.time()
        try:
            success = loader.bulk_upsert_execute_values(test_data)
            end_time = time.time()
            if success:
                elapsed = end_time - start_time
                rate = size / elapsed
                print(f"  ✓ execute_values: {elapsed:.2f}s ({rate:.0f} records/sec)")
            else:
                print("  ✗ execute_values method failed")
        except Exception as e:
            print(f"  ✗ execute_values error: {e}")
        
        # Clean up for next test
        try:
            db.execute_command(cleanup_sql)
        except:
            pass
        
        # Test 3: COPY method (fastest)
        print("🔸 Testing COPY method...")
        start_time = time.time()
        try:
            success = loader.bulk_upsert_copy(test_data)
            end_time = time.time()
            if success:
                elapsed = end_time - start_time
                rate = size / elapsed
                print(f"  ✓ COPY method: {elapsed:.2f}s ({rate:.0f} records/sec)")
            else:
                print("  ✗ COPY method failed")
        except Exception as e:
            print(f"  ✗ COPY method error: {e}")
        
        # Clean up test data
        try:
            db.execute_command(cleanup_sql)
            logger.info("Cleaned up test data")
        except:
            pass


def test_real_polygon_data():
    """Test bulk loading with real Polygon API data."""
    
    print("\n" + "=" * 80)
    print("REAL POLYGON DATA BULK LOADING TEST")
    print("=" * 80)
    
    try:
        scraper = PolygonDailyScraper()
        
        # Get recent trading day
        recent_date = scraper.get_most_recent_trading_day()
        print(f"Testing with real data from: {recent_date}")
        
        # Fetch real data
        print("🔹 Fetching data from Polygon API...")
        market_data = scraper.get_daily_market_summary(recent_date)
        
        if not market_data or not market_data.get('results'):
            print("❌ No data available for testing")
            return
        
        record_count = len(market_data['results'])
        print(f"📦 Retrieved {record_count:,} real market records")
        
        # Test bulk loading
        print("🔹 Testing bulk loading performance...")
        start_time = time.time()
        
        success = bulk_insert_polygon_data(market_data, method='copy')
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        if success:
            rate = record_count / elapsed
            print(f"✅ Success! Loaded {record_count:,} records in {elapsed:.2f}s")
            print(f"📈 Performance: {rate:.0f} records/second")
            
            # Show database stats
            stats = StockDataManager.get_table_stats()
            print(f"📊 Database now has {stats['total_records']:,} total records")
        else:
            print("❌ Bulk loading failed")
            
    except Exception as e:
        print(f"❌ Real data test failed: {e}")


def main():
    """Main test runner."""
    
    print("🚀 Starting bulk loading performance tests...")
    
    # Check environment
    if not os.getenv('SUPABASE_DB_URL'):
        print("❌ SUPABASE_DB_URL not found in environment")
        print("Please set up your .env file first")
        return
    
    # Test different loading methods
    test_loading_methods()
    
    # Test with real data if API key is available
    if os.getenv('POLYGON_API_KEY'):
        test_real_polygon_data()
    else:
        print("\n⚠️  POLYGON_API_KEY not found - skipping real data test")
    
    print("\n" + "=" * 80)
    print("🎯 PERFORMANCE TEST COMPLETE")
    print("=" * 80)
    print("Key takeaways:")
    print("• COPY method is fastest for large datasets (>1000 records)")
    print("• execute_values is good middle ground")
    print("• Original execute_many is slowest but most compatible")
    print("• Use 'auto' method in production for best performance")


if __name__ == "__main__":
    main()
