"""
Signal Enrichment Pipeline

Orchestrates all enrichment sources for anomaly events:
1. Polygon News — is there a known catalyst?
2. SEC EDGAR Form 4 — are corporate insiders also trading?
3. Symbol Novelty — how unusual is this trigger for this stock?

The enrichment adds context to each alert, helping the human reviewer
make better decisions. It can also be used to filter/rank alerts.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, List, Any

from .polygon_news import PolygonNewsChecker
from .edgar_insider import EdgarInsiderChecker
from .novelty import NoveltyScorer

logger = logging.getLogger(__name__)


class SignalEnrichment:
    """Enrich anomaly signals with external context."""

    def __init__(self, skip_news: bool = False, skip_edgar: bool = False):
        self.news_checker = None if skip_news else PolygonNewsChecker()
        self.edgar_checker = None if skip_edgar else EdgarInsiderChecker()
        self.novelty_scorer = NoveltyScorer()

    def enrich_event(self, symbol: str, event_date: datetime,
                     direction: str = 'call_heavy') -> Dict[str, Any]:
        """
        Enrich a single anomaly event with all available context.

        Args:
            symbol: Stock ticker
            event_date: Date of the anomaly
            direction: 'call_heavy' or 'put_heavy'

        Returns dict with enrichment data from all sources.
        """
        enrichment = {
            'symbol': symbol,
            'event_date': str(event_date),
            'enriched_at': datetime.now().isoformat(),
        }

        # 1. News check
        if self.news_checker:
            try:
                news = self.news_checker.check_news(symbol, event_date)
                enrichment['news'] = news
            except Exception as e:
                logger.warning(f"News enrichment failed for {symbol}: {e}")
                enrichment['news'] = {'error': str(e)}
        else:
            enrichment['news'] = {'skipped': True}

        # 2. EDGAR insider filings
        if self.edgar_checker:
            try:
                edgar = self.edgar_checker.check_insider_filings(symbol, event_date)

                # Compute alignment with anomaly direction
                if edgar.get('net_direction') in ('buying', 'selling'):
                    if direction == 'call_heavy' and edgar['net_direction'] == 'buying':
                        edgar['insider_alignment'] = 'aligned'
                    elif direction == 'put_heavy' and edgar['net_direction'] == 'selling':
                        edgar['insider_alignment'] = 'aligned'
                    elif edgar['net_direction'] in ('buying', 'selling'):
                        edgar['insider_alignment'] = 'contradictory'

                enrichment['edgar'] = edgar
            except Exception as e:
                logger.warning(f"EDGAR enrichment failed for {symbol}: {e}")
                enrichment['edgar'] = {'error': str(e)}
        else:
            enrichment['edgar'] = {'skipped': True}

        # 3. Novelty score
        try:
            novelty = self.novelty_scorer.score_novelty(symbol, event_date)
            enrichment['novelty'] = novelty
        except Exception as e:
            logger.warning(f"Novelty enrichment failed for {symbol}: {e}")
            enrichment['novelty'] = {'error': str(e)}

        # 4. Compute composite enrichment signal
        enrichment['conviction_modifiers'] = self._compute_conviction(enrichment, direction)

        return enrichment

    def enrich_batch(self, events: List[Dict]) -> List[Dict]:
        """
        Enrich a batch of anomaly events.

        Each event should have: symbol, event_date, direction
        Returns list of enrichment dicts.
        """
        results = []
        for ev in events:
            enrichment = self.enrich_event(
                ev['symbol'],
                ev['event_date'],
                ev.get('direction', 'call_heavy'),
            )
            results.append(enrichment)
        return results

    def _compute_conviction(self, enrichment: Dict, direction: str) -> Dict[str, Any]:
        """
        Compute conviction modifiers based on enrichment data.

        Returns factors that increase or decrease conviction:
          - no_news_bonus: +1 if no recent news (information asymmetry)
          - catalyst_penalty: -1 if known catalyst (public info driving flow)
          - insider_alignment_bonus: +1 if corporate insiders align
          - insider_contradiction_penalty: -1 if insiders contradict
          - novelty_bonus: +1 if first-time trigger
          - frequent_trigger_penalty: -1 if symbol triggers constantly
          - net_modifier: sum of all modifiers (-3 to +3)
        """
        modifiers = {}

        # News-based modifiers
        news = enrichment.get('news', {})
        if news.get('has_news') is False:
            modifiers['no_news_bonus'] = 1
        elif news.get('has_catalyst_news'):
            modifiers['catalyst_penalty'] = -1
        else:
            modifiers['no_news_bonus'] = 0
            modifiers['catalyst_penalty'] = 0

        # EDGAR-based modifiers
        edgar = enrichment.get('edgar', {})
        if edgar.get('insider_alignment') == 'aligned':
            modifiers['insider_alignment_bonus'] = 1
        elif edgar.get('insider_alignment') == 'contradictory':
            modifiers['insider_contradiction_penalty'] = -1
        else:
            modifiers['insider_alignment_bonus'] = 0
            modifiers['insider_contradiction_penalty'] = 0

        # Novelty-based modifiers
        novelty = enrichment.get('novelty', {})
        novelty_score = novelty.get('novelty_score')
        if novelty_score is not None:
            if novelty_score >= 0.9:
                modifiers['novelty_bonus'] = 1
            elif novelty_score <= 0.2:
                modifiers['frequent_trigger_penalty'] = -1
            else:
                modifiers['novelty_bonus'] = 0
                modifiers['frequent_trigger_penalty'] = 0

        modifiers['net_modifier'] = sum(modifiers.values())
        return modifiers

    @staticmethod
    def format_for_email(enrichment: Dict) -> str:
        """Format enrichment data as HTML snippet for email alerts."""
        parts = []

        # Novelty
        novelty = enrichment.get('novelty', {})
        if novelty.get('is_first_trigger'):
            parts.append('[!!!] <b>FIRST-TIME TRIGGER</b> -- never seen anomaly on this ticker')
        elif novelty.get('trigger_count_30d', 0) <= 2:
            parts.append(f'[!!] <b>RARE TRIGGER</b> -- {novelty.get("trigger_count_30d", "?")}x in 30 days')
        elif novelty.get('trigger_count_30d', 0) >= 10:
            parts.append(f'[--] Frequent trigger ({novelty.get("trigger_count_30d")}x in 30 days)')

        # News
        news = enrichment.get('news', {})
        if news.get('has_news') is False:
            parts.append('[!!!] <b>NO RECENT NEWS</b> -- possible information asymmetry')
        elif news.get('has_catalyst_news'):
            catalysts = ', '.join(news.get('catalyst_keywords', [])[:3])
            parts.append(f'[--] Known catalyst news: {catalysts}')
        elif news.get('has_news'):
            parts.append(f'[!] {news.get("news_count", 0)} recent articles (no catalyst keywords)')

        # EDGAR
        edgar = enrichment.get('edgar', {})
        if edgar.get('has_filings'):
            parts.append(f'[EDGAR] {edgar.get("filing_count", 0)} Form 4 filings in last 14 days')
            if edgar.get('insider_alignment') == 'aligned':
                parts.append('[!!!] <b>INSIDER ALIGNMENT</b> -- corporate insiders trading same direction')
        elif edgar.get('has_filings') is False:
            parts.append('[EDGAR] No recent insider filings')

        # Net conviction
        mods = enrichment.get('conviction_modifiers', {})
        net = mods.get('net_modifier', 0)
        if net >= 2:
            parts.append(f'<br><b>>>> ENRICHMENT CONVICTION: HIGH (+{net})</b>')
        elif net >= 1:
            parts.append(f'<br><b>Enrichment conviction: elevated (+{net})</b>')
        elif net <= -1:
            parts.append(f'<br><i>Enrichment conviction: reduced ({net})</i>')

        return '<br>'.join(parts) if parts else '<i>No enrichment data available</i>'
