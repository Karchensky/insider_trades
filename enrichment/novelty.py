"""
Symbol Novelty Scorer

Computes how "novel" an anomaly trigger is for a given symbol.
First-time triggers on quiet stocks are more suspicious than
daily triggers on high-volume tickers (which are just volatile).

Key insight: Actual insider trading produces a SUDDEN anomaly on a stock
that doesn't normally show unusual options activity. If AAPL triggers
every day, that's just AAPL being AAPL. If some small biotech triggers
for the first time ever, that's worth paying attention to.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

from database.core.connection import db

logger = logging.getLogger(__name__)


class NoveltyScorer:
    """Score how unusual/novel an anomaly trigger is for a given symbol."""

    def __init__(self):
        self._symbol_history = {}

    def score_novelty(self, symbol: str, event_date: datetime,
                      lookback_days: int = 30) -> Dict[str, Any]:
        """
        Compute novelty score for a symbol on a given date.

        Returns dict with:
          - trigger_count_30d: How many times this symbol triggered in last 30 days
          - trigger_count_7d: How many times in last 7 days
          - days_since_last_trigger: Days since previous trigger (None if first)
          - is_first_trigger: True if never triggered before in our lookback
          - novelty_score: 0.0 (common) to 1.0 (very novel)
          - avg_daily_triggers: Average daily trigger count for this symbol
        """
        try:
            conn = db.connect()
            with conn.cursor() as cur:
                # Count triggers in different windows
                cur.execute("""
                    SELECT
                        event_date,
                        total_magnitude,
                        z_score
                    FROM daily_anomaly_snapshot
                    WHERE symbol = %s
                      AND event_date >= %s
                      AND event_date < %s
                      AND total_magnitude >= 20000
                      AND COALESCE(is_bot_driven, false) = false
                      AND COALESCE(is_earnings_related, false) = false
                    ORDER BY event_date DESC
                """, (symbol,
                      event_date - timedelta(days=lookback_days),
                      event_date))

                history = [dict(row) for row in cur.fetchall()]

                # Also check if this symbol has EVER triggered before
                cur.execute("""
                    SELECT COUNT(*) as total_ever,
                           MIN(event_date) as first_seen
                    FROM daily_anomaly_snapshot
                    WHERE symbol = %s
                      AND event_date < %s
                      AND total_magnitude >= 20000
                      AND COALESCE(is_bot_driven, false) = false
                """, (symbol, event_date))

                ever_row = dict(cur.fetchone())

            trigger_count_30d = len(history)
            trigger_count_7d = sum(
                1 for h in history
                if h['event_date'] >= event_date - timedelta(days=7)
            )

            # Days since last trigger
            if history:
                last_date = history[0]['event_date']
                days_since = (event_date - last_date).days
            else:
                days_since = None

            total_ever = ever_row['total_ever'] or 0
            is_first = total_ever == 0

            # Compute novelty score (0-1)
            # High novelty = rare trigger, low novelty = frequent trigger
            if is_first:
                novelty = 1.0
            elif trigger_count_30d == 0:
                novelty = 0.9
            elif trigger_count_30d <= 2:
                novelty = 0.7
            elif trigger_count_30d <= 5:
                novelty = 0.5
            elif trigger_count_30d <= 10:
                novelty = 0.3
            else:
                novelty = max(0.1, 1.0 - trigger_count_30d / 30.0)

            return {
                'trigger_count_30d': trigger_count_30d,
                'trigger_count_7d': trigger_count_7d,
                'days_since_last_trigger': days_since,
                'is_first_trigger': is_first,
                'novelty_score': round(novelty, 2),
                'total_triggers_ever': total_ever,
                'avg_daily_triggers_30d': round(trigger_count_30d / 30.0, 2),
            }

        except Exception as e:
            logger.warning(f"Novelty scoring failed for {symbol}: {e}")
            return {
                'trigger_count_30d': None,
                'trigger_count_7d': None,
                'days_since_last_trigger': None,
                'is_first_trigger': None,
                'novelty_score': None,
                'total_triggers_ever': None,
                'avg_daily_triggers_30d': None,
                'error': str(e),
            }

    def batch_score(self, symbols_dates: list) -> Dict[str, Dict]:
        """
        Score novelty for multiple (symbol, event_date) pairs efficiently.
        Uses a single query to get all history at once.
        """
        if not symbols_dates:
            return {}

        try:
            conn = db.connect()
            symbols = list(set(s for s, _ in symbols_dates))
            min_date = min(d for _, d in symbols_dates) - timedelta(days=30)

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, event_date, total_magnitude, z_score
                    FROM daily_anomaly_snapshot
                    WHERE symbol = ANY(%s)
                      AND event_date >= %s
                      AND total_magnitude >= 20000
                      AND COALESCE(is_bot_driven, false) = false
                      AND COALESCE(is_earnings_related, false) = false
                    ORDER BY symbol, event_date
                """, (symbols, min_date))

                all_history = {}
                for row in cur.fetchall():
                    row = dict(row)
                    sym = row['symbol']
                    if sym not in all_history:
                        all_history[sym] = []
                    all_history[sym].append(row)

            results = {}
            for symbol, event_date in symbols_dates:
                key = f"{symbol}_{event_date}"
                sym_history = all_history.get(symbol, [])

                prior = [
                    h for h in sym_history
                    if h['event_date'] < event_date
                    and h['event_date'] >= event_date - timedelta(days=30)
                ]
                prior_7d = [
                    h for h in prior
                    if h['event_date'] >= event_date - timedelta(days=7)
                ]

                total_ever = len([
                    h for h in sym_history
                    if h['event_date'] < event_date
                ])

                count_30d = len(prior)
                count_7d = len(prior_7d)
                is_first = total_ever == 0

                if prior:
                    days_since = (event_date - max(h['event_date'] for h in prior)).days
                else:
                    days_since = None

                if is_first:
                    novelty = 1.0
                elif count_30d == 0:
                    novelty = 0.9
                elif count_30d <= 2:
                    novelty = 0.7
                elif count_30d <= 5:
                    novelty = 0.5
                elif count_30d <= 10:
                    novelty = 0.3
                else:
                    novelty = max(0.1, 1.0 - count_30d / 30.0)

                results[key] = {
                    'trigger_count_30d': count_30d,
                    'trigger_count_7d': count_7d,
                    'days_since_last_trigger': days_since,
                    'is_first_trigger': is_first,
                    'novelty_score': round(novelty, 2),
                    'total_triggers_ever': total_ever,
                    'avg_daily_triggers_30d': round(count_30d / 30.0, 2),
                }

            return results

        except Exception as e:
            logger.error(f"Batch novelty scoring failed: {e}")
            return {}
