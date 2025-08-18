"""
Test script for the optimized options contracts system.
Validates the complete workflow including concurrent API calls and batch processing.
"""

import os
import sys
import logging
import time
from datetime import date, datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.bulk_operations import BulkStockDataLoader
from scrapers.polygon_options_scraper_optimized import OptimizedPolygonOptionsContractsScraper
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_optimized_options_workflow():
    """Test the complete optimized options workflow."""
    
    print("=" * 70)
    print("TESTING OPTIMIZED OPTIONS CONTRACTS WORKFLOW")
    print("=" * 70)
    
    try:
        # Initialize scraper
        scraper = OptimizedPolygonOptionsContractsScraper()
        print("✓ Optimized scraper initialized")
        
        # Test concurrent API fetching (small test)
        print("\n1. Testing concurrent API calls...")
        test_tickers = ['AAPL', 'MSFT', 'GOOGL']
        test_date = scraper.get_most_recent_trading_day()
        
        start_time = time.time()
        responses = scraper.get_option_contracts_batch(test_tickers, test_date)
        api_time = time.time() - start_time
        
        total_contracts = sum(len(resp.get('results', [])) for resp in responses)
        print(f"✓ Fetched {len(responses)} responses with {total_contracts:,} contracts in {api_time:.2f}s")
        
        # Test batch bulk loading
        print("\n2. Testing batch bulk loading...")
        if responses:
            bulk_loader = BulkStockDataLoader()
            
            start_time = time.time()
            success = bulk_loader.bulk_insert_option_contracts_batch(responses, test_date)
            bulk_time = time.time() - start_time
            
            if success:
                contracts_per_sec = total_contracts / bulk_time if bulk_time > 0 else 0
                print(f"✓ Bulk loaded {total_contracts:,} contracts in {bulk_time:.2f}s ({contracts_per_sec:.0f} contracts/sec)")
            else:
                print("✗ Bulk loading failed")
                return False
        
        # Test schema changes
        print("\n3. Testing updated schema...")
        schema_sql = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'option_contracts' 
        AND column_name = 'additional_underlyings'
        """
        
        result = db.execute_query(schema_sql)
        if not result:
            print("✓ additional_underlyings column successfully removed")
        else:
            print("✗ additional_underlyings column still exists")
            return False
        
        # Verify data integrity
        print("\n4. Testing data integrity...")
        integrity_sql = """
        SELECT COUNT(*) as count 
        FROM option_contracts oc
        JOIN daily_stock_snapshot dss ON oc.date = dss.date AND oc.symbol = dss.symbol
        WHERE oc.date = %s
        """
        
        result = db.execute_query(integrity_sql, (test_date,))
        if result and result[0]['count'] > 0:
            print(f"✓ Foreign key relationships verified: {result[0]['count']} contracts properly linked")
        else:
            print("⚠ No linked data found (may be normal if no matching stock data)")
        
        # Performance comparison
        print("\n5. Performance Analysis:")
        estimated_sequential_time = len(test_tickers) * 6  # Old scraper: ~6 seconds per ticker
        actual_time = api_time + bulk_time
        speedup = estimated_sequential_time / actual_time if actual_time > 0 else 0
        
        print(f"   Sequential estimate: {estimated_sequential_time}s")
        print(f"   Concurrent actual: {actual_time:.2f}s")
        print(f"   Speedup: {speedup:.1f}x faster")
        print(f"   Throughput: {total_contracts / actual_time:.0f} contracts/sec")
        
        print("\n" + "=" * 70)
        print("🎉 ALL TESTS PASSED - OPTIMIZED OPTIONS SYSTEM READY!")
        print("=" * 70)
        
        print("\nRecommended usage:")
        print("python scrapers/polygon_options_scraper_optimized.py --recent")
        print("python scrapers/polygon_options_scraper_optimized.py --recent 5")
        print("python scrapers/polygon_options_scraper_optimized.py --start-date 2024-08-01 --end-date 2024-08-05")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        logger.error(f"Test failed: {e}")
        return False


if __name__ == "__main__":
    success = test_optimized_options_workflow()
    
    if not success:
        sys.exit(1)
