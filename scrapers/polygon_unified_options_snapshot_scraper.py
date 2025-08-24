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
        # Increase HTTP connection pool to support high concurrency
        try:
            from requests.adapters import HTTPAdapter
            adapter = HTTPAdapter(pool_connections=100, pool_maxsize=200, max_retries=3)
            self.session.mount('https://', adapter)
            self.session.mount('http://', adapter)
        except Exception:
            pass

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
        Request unified snapshot for a comma-separated list of tickers.
        If the request fails with a client error (e.g., URL too long or bad request),
        recursively split the batch and merge results. This keeps the intraday run resilient.
        """
        if not tickers:
            return {'results': []}

        def _request(ts: list[str]) -> dict:
            url = f"{self.base_url}/v3/snapshot"
            # Always use ticker.any_of per official client guidance
            params = {
                # Omit 'type' when using ticker.any_of, let prefixes (O:, X:, C:) drive market
                'ticker.any_of': ','.join(ts),
                'apikey': self.api_key
            }

            # Dynamically split to respect URL length limits in the stack
            # Default conservative limit if not provided
            try:
                max_url = int(os.getenv('POLYGON_MAX_URL_LEN', '1900'))
            except Exception:
                max_url = 1900
            # Compute prepared URL length
            preq = requests.PreparedRequest()
            preq.prepare_url(url, params)
            if len(preq.url) > max_url and len(ts) > 1:
                mid = len(ts) // 2
                left = _request(ts[:mid])
                right = _request(ts[mid:])
                merged = {'results': []}
                if left and left.get('results'):
                    merged['results'].extend(left['results'])
                if right and right.get('results'):
                    merged['results'].extend(right['results'])
                return merged

            resp = self.session.get(url, params=params, timeout=60)
            try:
                resp.raise_for_status()
                data = resp.json()
                # Deduplicate results by ticker within a single response
                results = data.get('results') or []
                if results:
                    seen = set()
                    deduped = []
                    for r in results:
                        tk = r.get('ticker')
                        if not tk or tk in seen:
                            continue
                        seen.add(tk)
                        deduped.append(r)
                    data['results'] = deduped
                return data
            except requests.HTTPError as e:
                status = resp.status_code if resp is not None else None
                # Fallback: split the batch on client errors
                if status in (400, 414) and len(ts) > 1:
                    mid = len(ts) // 2
                    left = _request(ts[:mid])
                    right = _request(ts[mid:])
                    merged = {'results': []}
                    if left and left.get('results'):
                        merged['results'].extend(left['results'])
                    if right and right.get('results'):
                        merged['results'].extend(right['results'])
                    return merged
                raise

        return _request(tickers)


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


