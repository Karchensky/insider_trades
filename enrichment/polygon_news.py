"""
Polygon News Enrichment

Checks for recent news around a ticker near the anomaly event date.
Key insight: anomalous options flow WITH NO public news is more suspicious
(suggests non-public information). Flow with known catalysts is less suspicious.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


class PolygonNewsChecker:
    """Check Polygon news API for recent ticker news around an event."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY not found in environment variables")
        self.base_url = "https://api.polygon.io"
        self.session = requests.Session()

    def check_news(self, symbol: str, event_date: datetime,
                   lookback_days: int = 3, lookforward_days: int = 1) -> Dict[str, Any]:
        """
        Check for news around an event date.

        Returns dict with:
          - has_news: bool
          - news_count: int
          - news_items: list of {title, published, source, keywords}
          - catalyst_keywords: list of detected catalyst terms
          - news_sentiment: 'positive'/'negative'/'mixed'/'none'
        """
        start_date = (event_date - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        end_date = (event_date + timedelta(days=lookforward_days)).strftime('%Y-%m-%d')

        try:
            url = f"{self.base_url}/v2/reference/news"
            params = {
                'ticker': symbol,
                'published_utc.gte': f"{start_date}T00:00:00Z",
                'published_utc.lte': f"{end_date}T23:59:59Z",
                'order': 'desc',
                'limit': 20,
                'apiKey': self.api_key,
            }

            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            results = data.get('results', [])
            if not results:
                return {
                    'has_news': False,
                    'news_count': 0,
                    'news_items': [],
                    'catalyst_keywords': [],
                    'news_sentiment': 'none',
                    'has_catalyst_news': False,
                }

            # Parse news items
            news_items = []
            all_keywords = []
            for item in results:
                news_items.append({
                    'title': item.get('title', ''),
                    'published': item.get('published_utc', ''),
                    'source': item.get('publisher', {}).get('name', 'Unknown'),
                    'keywords': item.get('keywords', []),
                })
                all_keywords.extend(item.get('keywords', []))

            # Detect catalyst keywords in titles
            catalyst_terms = [
                'fda', 'approval', 'acquisition', 'merger', 'buyout', 'takeover',
                'deal', 'trial', 'phase', 'earnings', 'guidance', 'upgrade',
                'downgrade', 'analyst', 'settlement', 'lawsuit', 'investigation',
                'sec', 'recall', 'patent', 'contract', 'partnership', 'ipo',
                'offering', 'dilution', 'bankruptcy', 'restructuring', 'layoff',
                'dividend', 'split', 'buyback', 'ceo', 'resign', 'appoint',
            ]

            found_catalysts = set()
            for item in results:
                title_lower = item.get('title', '').lower()
                desc_lower = item.get('description', '').lower()
                combined = title_lower + ' ' + desc_lower
                for term in catalyst_terms:
                    if term in combined:
                        found_catalysts.add(term)

            return {
                'has_news': True,
                'news_count': len(results),
                'news_items': news_items[:5],  # Top 5 only
                'catalyst_keywords': sorted(found_catalysts),
                'news_sentiment': 'mixed',  # Could enhance with NLP later
                'has_catalyst_news': len(found_catalysts) > 0,
            }

        except requests.RequestException as e:
            logger.warning(f"Polygon news check failed for {symbol}: {e}")
            return {
                'has_news': None,  # Unknown (API failure)
                'news_count': 0,
                'news_items': [],
                'catalyst_keywords': [],
                'news_sentiment': 'unknown',
                'has_catalyst_news': None,
                'error': str(e),
            }
