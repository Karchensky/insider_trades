"""
Example script showing how to fetch data from Polygon API and store it in the database.
This demonstrates the complete flow from API to database.
"""

import os
import sys
import logging
import requests
from datetime import date, datetime
from typing import Dict, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.stock_data import StockDataManager
from database.bulk_operations import bulk_insert_polygon_data
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PolygonAPIClient:
    """Simple client for Polygon API to fetch daily market data."""
    
    def __init__(self):
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not found in environment variables")
        
        self.base_url = "https://api.polygon.io"
    
    def get_daily_market_summary(self, market_date: str) -> Optional[Dict[str, Any]]:
        """
        Fetch daily market summary from Polygon API.
        
        Args:
            market_date: Date in YYYY-MM-DD format
            
        Returns:
            API response dict or None if failed
        """
        url = f"{self.base_url}/v2/aggs/grouped/locale/us/market/stocks/{market_date}"
        params = {
            'adjusted': 'true',
            'apikey': self.api_key
        }
        
        try:
            logger.info(f"Fetching market data for {market_date}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK':
                logger.info(f"Received {data.get('resultsCount', 0)} results")
                return data
            else:
                logger.error(f"API returned status: {data.get('status')}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def get_ticker_daily_summary(self, ticker: str, market_date: str) -> Optional[Dict[str, Any]]:
        """
        Fetch daily summary for a specific ticker.
        
        Args:
            ticker: Stock symbol
            market_date: Date in YYYY-MM-DD format
            
        Returns:
            API response dict or None if failed
        """
        url = f"{self.base_url}/v1/open-close/{ticker}/{market_date}"
        params = {
            'adjusted': 'true',
            'apikey': self.api_key
        }
        
        try:
            logger.info(f"Fetching data for {ticker} on {market_date}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK':
                # Convert single ticker response to match grouped format
                converted_data = {
                    'status': 'OK',
                    'resultsCount': 1,
                    'results': [{
                        'T': data.get('symbol'),
                        'c': data.get('close'),
                        'h': data.get('high'),
                        'l': data.get('low'),
                        'n': data.get('transactions', 0),
                        'o': data.get('open'),
                        't': int(datetime.strptime(market_date, '%Y-%m-%d').timestamp() * 1000),
                        'v': data.get('volume', 0),
                        'vw': data.get('close')  # Use close as approximation for vw
                    }]
                }
                return converted_data
            else:
                logger.error(f"API returned status: {data.get('status')}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None


def main():
    """Example usage of the Polygon API to database workflow."""
    
    # Initialize API client
    try:
        api_client = PolygonAPIClient()
    except ValueError as e:
        logger.error(f"Failed to initialize API client: {e}")
        return
    
    # Example 1: Fetch and store market data for a specific date
    market_date = "2024-01-15"  # Example date - replace with desired date
    
    logger.info("=" * 60)
    logger.info("EXAMPLE 1: Fetch Full Market Data")
    logger.info("=" * 60)
    
    market_data = api_client.get_daily_market_summary(market_date)
    
    if market_data:
        # Store in database using high-performance bulk loading
        record_count = market_data.get('resultsCount', 0)
        logger.info(f"Using bulk loading for {record_count:,} records...")
        
        success = bulk_insert_polygon_data(market_data, method='auto')
        
        if success:
            logger.info("Market data successfully stored in database using bulk loading")
            
            # Show some statistics
            stats = StockDataManager.get_table_stats()
            logger.info(f"Database now contains {stats['total_records']:,} total records")
            logger.info(f"Covering {stats['unique_symbols']:,} unique symbols")
        else:
            logger.error("Failed to store market data")
    else:
        logger.error("Failed to fetch market data")
    
    # Example 2: Fetch specific ticker data
    logger.info("\n" + "=" * 60)
    logger.info("EXAMPLE 2: Fetch Specific Ticker Data")
    logger.info("=" * 60)
    
    ticker = "AAPL"  # Example ticker
    ticker_data = api_client.get_ticker_daily_summary(ticker, market_date)
    
    if ticker_data:
        success = bulk_insert_polygon_data(ticker_data, method='auto')
        
        if success:
            logger.info(f"{ticker} data successfully stored using bulk loading")
            
            # Retrieve and display the stored data
            snapshot = StockDataManager.get_daily_snapshot(
                ticker, 
                datetime.strptime(market_date, '%Y-%m-%d').date()
            )
            
            if snapshot:
                logger.info(f"Stored data for {ticker}:")
                logger.info(f"  Open: ${snapshot['open']}")
                logger.info(f"  Close: ${snapshot['close']}")
                logger.info(f"  High: ${snapshot['high']}")
                logger.info(f"  Low: ${snapshot['low']}")
                logger.info(f"  Volume: {snapshot['trading_volume']:,}")
    
    # Example 3: Query recent data
    logger.info("\n" + "=" * 60)
    logger.info("EXAMPLE 3: Query Recent Data")
    logger.info("=" * 60)
    
    recent_snapshots = StockDataManager.get_latest_snapshots(limit=10)
    
    if recent_snapshots:
        logger.info(f"Top 10 most recent snapshots by volume:")
        for i, snapshot in enumerate(recent_snapshots[:10], 1):
            logger.info(f"{i:2d}. {snapshot['symbol']} - "
                       f"${snapshot['close']:.2f} - "
                       f"Vol: {snapshot['trading_volume']:,}")
    
    logger.info("\n" + "=" * 60)
    logger.info("EXAMPLE COMPLETE")
    logger.info("=" * 60)
    
    print("\nTo use this with real data:")
    print("1. Set your POLYGON_API_KEY in the .env file")
    print("2. Modify the market_date variable to a recent trading day")
    print("3. Run the script to fetch and store real market data")
    print("\nData will be stored with the composite primary key (date, symbol)")
    print("Duplicate entries will be updated automatically.")


if __name__ == "__main__":
    main()
