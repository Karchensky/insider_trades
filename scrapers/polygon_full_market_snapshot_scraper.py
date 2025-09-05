"""
Polygon.io Full Market Snapshot Scraper
Fetches the entire US market snapshot in a single API call and bulk-loads
into TEMP_STOCK_SNAPSHOT using COPY for performance.

API: https://polygon.io/docs/rest/stocks/snapshots/full-market-snapshot
Endpoint: GET /v2/snapshot/locale/us/markets/stocks/tickers
"""

import os
import sys
import logging
import requests
import argparse
from typing import Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.bulk_operations import BulkStockDataLoader
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FullMarketSnapshotScraper:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not found in environment variables")
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()

    def fetch_full_market_snapshot(self, include_otc: bool = False) -> Optional[dict]:
        url = f"{self.base_url}/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {
            'include_otc': str(include_otc).lower(),
            'apikey': self.api_key
        }
        try:
            logger.info("Requesting full-market snapshot (OTC=%s)...", include_otc)
            resp = self.session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if data.get('status') == 'OK' and data.get('tickers'):
                logger.info("Received %s tickers", data.get('count', len(data['tickers'])))
                return data
            logger.warning("Snapshot returned no tickers or non-OK status: %s", data.get('status'))
            return None
        except requests.RequestException as e:
            logger.error("Snapshot request failed: %s", e)
            return None


def main():
    parser = argparse.ArgumentParser(description='Polygon Full Market Snapshot Scraper')
    parser.add_argument('--include-otc', action='store_true', help='Include OTC securities (default: false)')
    args = parser.parse_args()

    scraper = FullMarketSnapshotScraper()
    loader = BulkStockDataLoader()

    snapshot = scraper.fetch_full_market_snapshot(include_otc=args.include_otc)
    if not snapshot:
        logger.error("No snapshot data to process")
        return

    # Use existing high-performance COPY path that maps snapshot payload → temp_stock
    result = loader.bulk_upsert_temp_snapshots_copy(snapshot)
    if result.get('success'):
        logger.info("✓ Loaded %s records in %.2fs (%.0f rec/s)",
                    result['records_processed'], result['execution_time'], result['records_per_second'])
    else:
        logger.error("Bulk load failed: %s", result.get('error'))


if __name__ == '__main__':
    main()


