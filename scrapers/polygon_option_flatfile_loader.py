"""
Polygon.io Options Day Aggregates (flat files) loader.

Downloads a single day's CSV.GZ from Polygon Flat Files S3 endpoint and bulk-upserts
into daily_option_snapshot using the existing high-performance COPY path.

Docs:
- Flat Files Quickstart: https://polygon.io/docs/flat-files/quickstart
- Options Day Aggregates: https://polygon.io/docs/flat-files/options/day-aggregates
"""

import os
import sys
import logging
import tempfile
import gzip
import csv
import io
from typing import Iterator, Dict, Any, Optional
import requests
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db
from database.core.bulk_operations import BulkStockDataLoader

logger = logging.getLogger(__name__)


class PolygonOptionFlatFileLoader:
    """
    Loader for Polygon flat-file day aggregates for options.
    """

    def __init__(self, api_key: Optional[str] = None):
        # Load .env for local development
        try:
            load_dotenv()
        except Exception:
            pass
        # REST API key is not used for Flat Files (S3) access; require S3 keys instead
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        self.session = requests.Session()

        # S3 settings per Quickstart
        self.s3_endpoint = os.getenv('POLYGON_FLATFILES_ENDPOINT', 'https://files.polygon.io')
        self.s3_bucket = os.getenv('POLYGON_FLATFILES_BUCKET', 'flatfiles')
        # Accept multiple env var names for convenience
        self.s3_access_key = (
            os.getenv('POLYGON_S3_ACCESS_KEY')
            or os.getenv('POLYGON_FLATFILES_ACCESS_KEY')
            or os.getenv('AWS_ACCESS_KEY_ID')
        )
        self.s3_secret_key = (
            os.getenv('POLYGON_S3_SECRET_KEY')
            or os.getenv('POLYGON_FLATFILES_SECRET_KEY')
            or os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        # Dataset prefix (options day aggregates)
        self.dataset_prefix = os.getenv('POLYGON_FLATFILES_PREFIX', 'us_options_opra/day_aggs_v1')

    def build_s3_keys(self, target_date: str) -> list[str]:
        """
        Build candidate S3 keys for the dataset.
        Based on your working AWS CLI path, primary layout is:
          us_options_opra/day_aggs_v1/YYYY/MM/YYYY-MM-DD.csv.gz
        We'll also try a couple of alternates just in case.
        """
        year = target_date[0:4]
        month = target_date[5:7]
        yyyymmdd = target_date.replace('-', '')
        return [
            f"{self.dataset_prefix}/{year}/{month}/{target_date}.csv.gz",
            f"{self.dataset_prefix}/{year}/{target_date}.csv.gz",
            f"{self.dataset_prefix}/{year}/{yyyymmdd}.csv.gz",
        ]

    def iter_rows_from_s3(self, target_date: str) -> Iterator[Dict[str, Any]]:
        """
        Stream rows from S3 object using Boto3. Requires S3 access keys.
        """
        if not (self.s3_access_key and self.s3_secret_key):
            raise RuntimeError("Polygon Flat Files require S3 Access Key/Secret. Set POLYGON_S3_ACCESS_KEY and POLYGON_S3_SECRET_KEY.")

        try:
            import boto3
            from botocore.config import Config
        except ImportError as e:
            raise RuntimeError("boto3 is required for Polygon Flat Files. Please install requirements.") from e

        # Use path-style addressing and SigV4 per common S3-compatible setups
        config = Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        s3 = boto3.client(
            's3',
            endpoint_url=self.s3_endpoint,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            region_name=os.getenv('POLYGON_FLATFILES_REGION', 'us-east-1'),
            config=config,
        )
        last_err = None
        for idx, key in enumerate(self.build_s3_keys(target_date), start=1):
            try:
                logger.info(f"Fetching S3 object (candidate {idx}) s3://{self.s3_bucket}/{key}")
                obj = s3.get_object(Bucket=self.s3_bucket, Key=key)
                body = obj['Body']  # StreamingBody
                with gzip.GzipFile(fileobj=body) as gz:
                    text_stream = io.TextIOWrapper(gz)
                    reader = csv.DictReader(text_stream)
                    for row in reader:
                        yield {
                            'ticker': row.get('ticker') or row.get('symbol') or row.get('T') or '',
                            'volume': row.get('volume') or row.get('v') or '',
                            'open': row.get('open') or row.get('o') or '',
                            'close': row.get('close') or row.get('c') or '',
                            'high': row.get('high') or row.get('h') or '',
                            'low': row.get('low') or row.get('l') or '',
                            'window_start': row.get('window_start') or row.get('t') or '',
                            'transactions': row.get('transactions') or row.get('n') or '',
                        }
                return
            except Exception as e:
                last_err = e
                logger.info(f"Candidate key failed: {key} ({e})")
                continue
        if last_err:
            raise last_err
        raise RuntimeError("No candidate S3 key succeeded and no error captured")

    def iter_rows_from_file(self, gz_path: str) -> Iterator[Dict[str, Any]]:
        """
        Yield CSV rows as dicts from the gzip file. Column names per docs:
        ticker, volume, open, close, high, low, window_start, transactions
        """
        with gzip.open(gz_path, mode='rt', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Normalize keys lower/strip in case of variations
                yield {
                    'ticker': row.get('ticker') or row.get('symbol') or row.get('T') or '',
                    'volume': row.get('volume') or row.get('v') or '',
                    'open': row.get('open') or row.get('o') or '',
                    'close': row.get('close') or row.get('c') or '',
                    'high': row.get('high') or row.get('h') or '',
                    'low': row.get('low') or row.get('l') or '',
                    'window_start': row.get('window_start') or row.get('t') or '',
                    'transactions': row.get('transactions') or row.get('n') or '',
                }

    def load_for_date(self, target_date: str, skip_existing: bool = True) -> Dict[str, Any]:
        """
        Load a single day's option aggregates file into daily_option_snapshot.
        Returns a result dict with stats similar to the API path.
        """
        results = {
            'date': target_date,
            'downloaded': False,
            'rows_processed': 0,
            'skipped_existing': 0,
            'success': False,
        }

        # Skip if already loaded
        if skip_existing:
            try:
                existing = db.execute_query(
                    "SELECT COUNT(*) AS c FROM daily_option_snapshot WHERE date = %s",
                    (target_date,)
                )
                first = existing[0] if existing else {}
                # Prefer 'c' alias; fallback to 'count' if driver renames
                count = 0
                if isinstance(first, dict):
                    count = int(first.get('c') or first.get('count') or 0)
                if count and count > 0:
                    logger.info(f"Skipping {target_date} - snapshot data already exists for {count} contracts")
                    results['skipped_existing'] = count
                    results['success'] = True
                    return results
            except Exception as e:
                logger.warning(f"Existing check failed for {target_date}: {e}")

        # Fetch and stream rows via S3
        rows_iter = self.iter_rows_from_s3(target_date)
        loader = BulkStockDataLoader()
        ok = loader.bulk_upsert_option_snapshots_from_flat_rows(rows_iter, default_date=target_date)
        results['downloaded'] = True
        results['success'] = bool(ok)
        return results


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    p = argparse.ArgumentParser(description='Load Polygon options day aggregates (flat file)')
    p.add_argument('--date', required=True, help='Date YYYY-MM-DD')
    p.add_argument('--force', action='store_true', help='Load even if rows exist')
    args = p.parse_args()

    loader = PolygonOptionFlatFileLoader()
    res = loader.load_for_date(args.date, skip_existing=not args.force)
    print(res)


if __name__ == '__main__':
    main()


