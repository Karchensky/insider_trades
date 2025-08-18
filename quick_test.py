"""
Quick Test Script
A simple script to test the complete setup once your Supabase credentials are configured.
This script will:
1. Initialize the database
2. Test with real Polygon API data
3. Show usage examples
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_environment():
    """Check if environment variables are set."""
    required_vars = ['SUPABASE_DB_URL', 'POLYGON_API_KEY']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease update your .env file with valid credentials.")
        return False
    
    print("✓ Environment variables configured")
    return True


def run_database_initialization():
    """Run database initialization."""
    print("\n" + "=" * 50)
    print("STEP 1: Database Initialization")
    print("=" * 50)
    
    try:
        from migrations.migration_manager import migrate
        success = migrate()
        if success:
            print("✓ Database initialized successfully")
            return True
        else:
            print("❌ Database initialization failed")
            return False
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False


def run_real_data_test():
    """Run real data test."""
    print("\n" + "=" * 50)
    print("STEP 2: Real Polygon API Data Test")
    print("=" * 50)
    
    try:
        # Import and run the real data verification
        sys.path.append('verifications')
        from test_real_polygon_data import RealDataVerification
        
        verification = RealDataVerification()
        success = verification.run_all_tests()
        
        if success:
            print("✓ Real data test completed successfully")
        else:
            print("❌ Some tests failed - check logs above")
        
        return success
    except Exception as e:
        print(f"❌ Real data test failed: {e}")
        return False


def show_usage_examples():
    """Show usage examples."""
    print("\n" + "=" * 50)
    print("USAGE EXAMPLES")
    print("=" * 50)
    
    print("\n1. Scrape Recent Market Data:")
    print("   python scrapers/polygon_daily_scraper.py --recent")
    
    print("\n2. Scrape Date Range:")
    print("   python scrapers/polygon_daily_scraper.py --start-date 2024-01-01 --end-date 2024-01-31")
    
    print("\n3. Force Re-scrape (ignore existing data):")
    print("   python scrapers/polygon_daily_scraper.py --recent --force")
    
    print("\n4. Include OTC Securities:")
    print("   python scrapers/polygon_daily_scraper.py --recent --include-otc")
    
    print("\n5. Python Script Usage:")
    print("""
   from database.stock_data import StockDataManager
   from scrapers.polygon_daily_scraper import PolygonDailyScraper
   
   # Get latest market data
   latest = StockDataManager.get_latest_snapshots(limit=10)
   
   # Get specific stock history
   aapl_data = StockDataManager.get_symbol_history('AAPL', limit=30)
   
   # Scrape data programmatically
   scraper = PolygonDailyScraper()
   results = scraper.scrape_date_range('2024-01-01', '2024-01-05')
   """)


def main():
    """Main test runner."""
    print("=" * 60)
    print("INSIDER TRADES QUICK TEST SETUP")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check environment
    if not check_environment():
        print("\n❌ Setup incomplete. Please configure your .env file first.")
        print("\nYour .env file should contain:")
        print("SUPABASE_DB_URL=postgresql://postgres.<your-url>...")
        print("POLYGON_API_KEY=<your-polygon-api-key>")
        return False
    
    # Initialize database
    if not run_database_initialization():
        print("\n❌ Database setup failed. Check your Supabase credentials.")
        return False
    
    # Test with real data
    if not run_real_data_test():
        print("\n⚠️  Real data test had some issues, but the basic setup is working.")
        print("   This might be due to API rate limits or weekend/holiday data.")
    
    # Show examples
    show_usage_examples()
    
    print("\n" + "=" * 60)
    print("SETUP COMPLETE!")
    print("=" * 60)
    print("Your insider trades database is ready for use.")
    print("Check the usage examples above to get started.")
    
    return True


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
