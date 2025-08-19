"""
Polygon.io Unified Snapshot (Options) Scraper

Fetches options snapshots from /v3/snapshot (type=options) with pagination
and bulk-loads into TEMP_OPTION_SNAPSHOT using COPY for performance.

Docs: https://polygon.io/docs/rest/stocks/snapshots/unified-snapshot
Endpoint: GET /v3/snapshot
"""

import os
import sys
import logging
import requests
import argparse
from typing import Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.bulk_operations import BulkStockDataLoader
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedOptionsSnapshotScraper:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not found in environment variables")
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()

    def fetch_page(self, ticker: Optional[str], limit: int, sort: Optional[str], order: Optional[str], next_url: Optional[str]):
        if next_url:
            # Ensure apikey is present on next_url
            if 'apikey=' not in next_url:
                sep = '&' if ('?' in next_url) else '?'
                url = f"{next_url}{sep}apikey={self.api_key}"
            else:
                url = next_url
            params = None
        else:
            url = f"{self.base_url}/v3/snapshot"
            params = {
                'type': 'options',
                'limit': str(limit),
                'apikey': self.api_key
            }
            if ticker:
                params['ticker'] = ticker
            if sort:
                params['sort'] = sort
            if order:
                params['order'] = order

        resp = self.session.get(url, params=params, timeout=60)
        if resp.status_code == 401 and next_url:
            # Retry with apikey appended to URL (some cursors omit api key propagation)
            fixed = url
            if 'apikey=' not in fixed:
                sep = '&' if ('?' in fixed) else '?'
                fixed = f"{fixed}{sep}apikey={self.api_key}"
            resp = self.session.get(fixed, params=None, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def fetch_by_tickers(self, tickers: list[str]) -> dict:
        """
        Request unified snapshot for a comma-separated list of tickers (max 250).
        tickers: list of option contract tickers (O:...)
        """
        if not tickers:
            return {'results': []}
        url = f"{self.base_url}/v3/snapshot"
        params = {
            'type': 'options',
            'ticker': ','.join(tickers),
            'apikey': self.api_key
        }
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()


def main():
    parser = argparse.ArgumentParser(description='Polygon Unified Snapshot (Options) Scraper')
    parser.add_argument('--ticker', type=str, help='Filter by ticker range/prefix')
    parser.add_argument('--limit', type=int, default=250, help='Page size (max 250)')
    parser.add_argument('--max-pages', type=int, default=1, help='Maximum number of pages to fetch')
    parser.add_argument('--sort', type=str, help='Sort field')
    parser.add_argument('--order', type=str, choices=['asc','desc'], help='Order direction')
    args = parser.parse_args()

    scraper = UnifiedOptionsSnapshotScraper()
    loader = BulkStockDataLoader()

    pages_fetched = 0
    next_url = None
    total_loaded = 0
    while pages_fetched < args.max_pages:
        try:
            data = scraper.fetch_page(args.ticker, args.limit, args.sort, args.order, next_url)
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            break

        results = data.get('results') or []
        logger.info("Fetched page %d: %d results", pages_fetched + 1, len(results))
        if not results:
            break

        out = loader.bulk_upsert_temp_option_snapshot_copy({'results': results})
        if out.get('success'):
            logger.info("✓ Loaded %s option rows in %.2fs (%.0f rec/s)", out['records_processed'], out['execution_time'], out['records_per_second'])
            total_loaded += out['records_processed']
        else:
            logger.error("Bulk load failed: %s", out.get('error'))
            break

        pages_fetched += 1
        next_url = data.get('next_url')
        if not next_url:
            break

    logger.info("Done. Total loaded: %d", total_loaded)


if __name__ == '__main__':
    main()


