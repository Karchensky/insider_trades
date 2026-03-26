"""
SEC EDGAR Form 4 Enrichment

Checks for recent insider transactions (Form 4 filings) around a ticker.
Corporate insiders (officers, directors, 10%+ owners) must file Form 4 within
2 business days of a transaction.

Key insight: If anomalous call buying coincides with insider BUYING, that's
alignment. If corporate insiders are selling while we see call anomalies,
that's contradictory and lowers conviction.

No API key needed. EDGAR requires a User-Agent header identifying the requester.
Rate limit: 10 requests/second.
"""

import os
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# Cache directory for CIK mapping
CACHE_DIR = Path(__file__).parent / '.cache'


class EdgarInsiderChecker:
    """Check SEC EDGAR for recent Form 4 (insider transaction) filings."""

    EDGAR_BASE = "https://data.sec.gov"
    EFTS_BASE = "https://efts.sec.gov/LATEST"
    TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

    def __init__(self, user_agent: Optional[str] = None):
        self.user_agent = user_agent or os.getenv(
            'EDGAR_USER_AGENT',
            'InsiderTradesProject admin@example.com'
        )
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent,
            'Accept-Encoding': 'gzip, deflate',
        })
        self._cik_map = None
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce 10 req/sec rate limit for EDGAR."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 0.1:
            time.sleep(0.1 - elapsed)
        self._last_request_time = time.time()

    def _load_cik_map(self) -> Dict[str, str]:
        """Load ticker->CIK mapping from SEC. Cached locally."""
        if self._cik_map is not None:
            return self._cik_map

        CACHE_DIR.mkdir(exist_ok=True)
        cache_file = CACHE_DIR / 'company_tickers.json'

        # Use cache if less than 7 days old
        if cache_file.exists():
            age = time.time() - cache_file.stat().st_mtime
            if age < 7 * 86400:
                try:
                    with open(cache_file) as f:
                        data = json.load(f)
                    self._cik_map = {
                        v['ticker'].upper(): str(v['cik_str'])
                        for v in data.values()
                    }
                    return self._cik_map
                except Exception:
                    pass

        # Fetch from SEC
        try:
            self._rate_limit()
            resp = self.session.get(self.TICKERS_URL, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            with open(cache_file, 'w') as f:
                json.dump(data, f)

            self._cik_map = {
                v['ticker'].upper(): str(v['cik_str'])
                for v in data.values()
            }
            return self._cik_map

        except Exception as e:
            logger.warning(f"Failed to load CIK mapping: {e}")
            self._cik_map = {}
            return self._cik_map

    def _get_cik(self, symbol: str) -> Optional[str]:
        """Map ticker symbol to CIK number."""
        cik_map = self._load_cik_map()
        return cik_map.get(symbol.upper())

    def check_insider_filings(self, symbol: str, event_date: datetime,
                              lookback_days: int = 14) -> Dict[str, Any]:
        """
        Check for recent Form 4 insider filings.

        Returns dict with:
          - has_filings: bool
          - filing_count: int
          - recent_buys: int (insider purchases)
          - recent_sells: int (insider sales)
          - net_direction: 'buying'/'selling'/'mixed'/'none'
          - filings: list of {filer, date, form_type}
          - insider_alignment: 'aligned'/'contradictory'/'neutral' (vs anomaly direction)
        """
        cik = self._get_cik(symbol)
        if not cik:
            return {
                'has_filings': None,
                'filing_count': 0,
                'recent_buys': 0,
                'recent_sells': 0,
                'net_direction': 'unknown',
                'filings': [],
                'insider_alignment': 'unknown',
                'error': f'No CIK found for {symbol}',
            }

        padded_cik = cik.zfill(10)
        start_date = (event_date - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        end_date = event_date.strftime('%Y-%m-%d')

        try:
            # Use EDGAR full-text search for Form 4 filings
            self._rate_limit()
            url = f"{self.EFTS_BASE}/search-index"
            params = {
                'q': f'"{padded_cik}"',
                'dateRange': 'custom',
                'startdt': start_date,
                'enddt': end_date,
                'forms': '4',
                'from': '0',
                'size': '20',
            }

            resp = self.session.get(url, params=params, timeout=15)

            # EFTS might return different structure — try submissions API as fallback
            if resp.status_code != 200:
                return self._check_via_submissions(padded_cik, event_date, lookback_days)

            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])

            if not hits:
                # Try submissions API as fallback
                return self._check_via_submissions(padded_cik, event_date, lookback_days)

            filings = []
            for hit in hits[:20]:
                source = hit.get('_source', {})
                filings.append({
                    'filer': source.get('display_names', ['Unknown'])[0] if source.get('display_names') else 'Unknown',
                    'date': source.get('file_date', ''),
                    'form_type': source.get('form_type', '4'),
                })

            return {
                'has_filings': len(filings) > 0,
                'filing_count': len(filings),
                'recent_buys': 0,  # Would need to parse XML for transaction details
                'recent_sells': 0,
                'net_direction': 'present' if filings else 'none',
                'filings': filings[:5],
                'insider_alignment': 'unknown',  # Need transaction details to determine
            }

        except Exception as e:
            logger.warning(f"EDGAR check failed for {symbol}: {e}")
            return {
                'has_filings': None,
                'filing_count': 0,
                'recent_buys': 0,
                'recent_sells': 0,
                'net_direction': 'unknown',
                'filings': [],
                'insider_alignment': 'unknown',
                'error': str(e),
            }

    def _check_via_submissions(self, padded_cik: str, event_date: datetime,
                               lookback_days: int) -> Dict[str, Any]:
        """Fallback: check via EDGAR submissions API."""
        try:
            self._rate_limit()
            url = f"{self.EDGAR_BASE}/submissions/CIK{padded_cik}.json"
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            recent = data.get('filings', {}).get('recent', {})
            forms = recent.get('form', [])
            dates = recent.get('filingDate', [])
            names = recent.get('primaryDocDescription', [])

            start_date = event_date - timedelta(days=lookback_days)
            filings = []

            for i, (form, date_str) in enumerate(zip(forms, dates)):
                if form not in ('4', '4/A'):
                    continue
                try:
                    filing_date = datetime.strptime(date_str, '%Y-%m-%d')
                except ValueError:
                    continue
                if start_date <= filing_date <= event_date:
                    filings.append({
                        'filer': names[i] if i < len(names) else 'Unknown',
                        'date': date_str,
                        'form_type': form,
                    })

            return {
                'has_filings': len(filings) > 0,
                'filing_count': len(filings),
                'recent_buys': 0,
                'recent_sells': 0,
                'net_direction': 'present' if filings else 'none',
                'filings': filings[:5],
                'insider_alignment': 'unknown',
            }

        except Exception as e:
            logger.warning(f"EDGAR submissions fallback failed: {e}")
            return {
                'has_filings': None,
                'filing_count': 0,
                'recent_buys': 0,
                'recent_sells': 0,
                'net_direction': 'unknown',
                'filings': [],
                'insider_alignment': 'unknown',
                'error': str(e),
            }
