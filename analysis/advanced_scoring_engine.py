"""
Advanced Scoring Engine for Insider Trading Detection

This module implements sophisticated scoring algorithms that combine multiple signals
to identify high-conviction insider trading opportunities with maximum return potential.

Key Features:
- Multi-factor scoring combining technical, fundamental, and sentiment indicators
- Risk-adjusted return probability calculations
- Pattern recognition for known insider trading signatures
- Dynamic threshold adjustments based on market conditions
- Confidence intervals and conviction ratings
"""

import os
import json
import logging
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass

from database.connection import db

logger = logging.getLogger(__name__)


@dataclass
class ScoringFactors:
    """Data class to hold all scoring factors for a symbol/signal."""
    volume_z_score: float = 0.0
    oi_momentum: float = 0.0
    price_momentum: float = 0.0
    volatility_skew: float = 0.0
    time_decay: float = 0.0
    greeks_alignment: float = 0.0
    concentration_factor: float = 0.0
    market_cap_factor: float = 0.0
    liquidity_factor: float = 0.0
    pattern_recognition: float = 0.0


@dataclass
class ConvictionRating:
    """Conviction rating with confidence intervals."""
    score: float
    confidence: float
    conviction_level: str  # LOW, MEDIUM, HIGH, EXTREME
    expected_return: float
    risk_factor: float
    time_horizon: int  # days
    supporting_factors: List[str]
    risk_factors: List[str]


class AdvancedScoringEngine:
    """Advanced scoring engine for insider trading detection."""
    
    def __init__(self):
        self.conviction_thresholds = {
            'LOW': 3.0,
            'MEDIUM': 5.0, 
            'HIGH': 7.5,
            'EXTREME': 10.0
        }
        self.max_score = 15.0  # Maximum possible score
        
    def calculate_comprehensive_score(self, symbol: str, contract_data: Dict[str, Any], 
                                    market_data: Dict[str, Any]) -> ConvictionRating:
        """
        Calculate comprehensive score combining all available signals.
        
        Args:
            symbol: Stock symbol
            contract_data: Options contract data
            market_data: Market and fundamental data
            
        Returns:
            ConvictionRating with detailed scoring breakdown
        """
        factors = self._extract_scoring_factors(symbol, contract_data, market_data)
        
        # Calculate weighted composite score
        weights = self._get_dynamic_weights(symbol, market_data)
        composite_score = self._calculate_weighted_score(factors, weights)
        
        # Apply market condition adjustments
        adjusted_score = self._apply_market_adjustments(composite_score, market_data)
        
        # Calculate confidence and supporting factors
        confidence = self._calculate_confidence(factors, market_data)
        supporting_factors = self._identify_supporting_factors(factors)
        risk_factors = self._identify_risk_factors(factors, market_data)
        
        # Determine conviction level
        conviction_level = self._determine_conviction_level(adjusted_score)
        
        # Calculate expected return and risk
        expected_return = self._calculate_expected_return(adjusted_score, factors, market_data)
        risk_factor = self._calculate_risk_factor(factors, market_data)
        
        # Estimate optimal time horizon
        time_horizon = self._estimate_time_horizon(factors, contract_data)
        
        return ConvictionRating(
            score=round(adjusted_score, 2),
            confidence=round(confidence, 3),
            conviction_level=conviction_level,
            expected_return=round(expected_return, 3),
            risk_factor=round(risk_factor, 3),
            time_horizon=time_horizon,
            supporting_factors=supporting_factors,
            risk_factors=risk_factors
        )
    
    def _extract_scoring_factors(self, symbol: str, contract_data: Dict[str, Any], 
                               market_data: Dict[str, Any]) -> ScoringFactors:
        """Extract all scoring factors from available data."""
        factors = ScoringFactors()
        
        # Volume Z-Score (from enhanced detection)
        factors.volume_z_score = contract_data.get('z_score', 0.0)
        
        # Open Interest Momentum
        current_oi = contract_data.get('current_oi', 0)
        previous_oi = contract_data.get('previous_oi', 0)
        if previous_oi > 0:
            factors.oi_momentum = min((current_oi - previous_oi) / previous_oi * 10, 5.0)
        
        # Price Momentum (stock movement correlation)
        stock_change = market_data.get('stock_change_pct', 0.0)
        factors.price_momentum = min(abs(stock_change) * 20, 3.0)  # Scale to max 3.0
        
        # Volatility Skew (IV vs historical volatility)
        iv = contract_data.get('implied_volatility', 0.0)
        hv = market_data.get('historical_volatility', 0.0)
        if hv > 0:
            factors.volatility_skew = min(abs(iv - hv) / hv * 2, 2.0)
        
        # Time Decay Factor (favor shorter expiries for insider plays)
        days_to_expiry = contract_data.get('days_to_expiry', 30)
        factors.time_decay = max(2.0 - (days_to_expiry / 15), 0)  # Higher score for <15 days
        
        # Greeks Alignment (delta/gamma positioning)
        delta = abs(contract_data.get('delta', 0.0))
        gamma = contract_data.get('gamma', 0.0)
        factors.greeks_alignment = min(delta * 2 + gamma * 100, 3.0)
        
        # Concentration Factor (unusual focus on specific strikes)
        strike_concentration = contract_data.get('strike_concentration', 1.0)
        factors.concentration_factor = min(strike_concentration * 1.5, 2.0)
        
        # Market Cap Factor (favor smaller caps for insider edge)
        market_cap = market_data.get('market_cap', 10000000000)  # Default 10B
        if market_cap < 1000000000:  # <1B
            factors.market_cap_factor = 2.0
        elif market_cap < 5000000000:  # <5B
            factors.market_cap_factor = 1.0
        else:
            factors.market_cap_factor = 0.0
        
        # Liquidity Factor (unusual activity relative to normal liquidity)
        avg_volume = contract_data.get('avg_volume', 1)
        current_volume = contract_data.get('current_volume', 0)
        if avg_volume > 0:
            liquidity_ratio = current_volume / avg_volume
            factors.liquidity_factor = min(np.log(liquidity_ratio + 1) * 1.5, 2.5)
        
        # Pattern Recognition (identify known insider patterns)
        factors.pattern_recognition = self._calculate_pattern_score(contract_data, market_data)
        
        return factors
    
    def _get_dynamic_weights(self, symbol: str, market_data: Dict[str, Any]) -> Dict[str, float]:
        """Get dynamic weights based on market conditions and symbol characteristics."""
        base_weights = {
            'volume_z_score': 0.25,
            'oi_momentum': 0.15,
            'price_momentum': 0.15,
            'volatility_skew': 0.10,
            'time_decay': 0.08,
            'greeks_alignment': 0.12,
            'concentration_factor': 0.05,
            'market_cap_factor': 0.05,
            'liquidity_factor': 0.03,
            'pattern_recognition': 0.02
        }
        
        # Adjust weights based on market volatility
        market_volatility = market_data.get('market_volatility', 0.20)
        if market_volatility > 0.30:  # High volatility environment
            base_weights['volatility_skew'] *= 1.5
            base_weights['time_decay'] *= 1.3
        elif market_volatility < 0.15:  # Low volatility environment
            base_weights['volume_z_score'] *= 1.2
            base_weights['oi_momentum'] *= 1.3
        
        # Adjust for earnings season
        is_earnings_season = market_data.get('earnings_within_30_days', False)
        if is_earnings_season:
            base_weights['time_decay'] *= 1.5
            base_weights['greeks_alignment'] *= 1.2
        
        # Normalize weights to sum to 1.0
        total_weight = sum(base_weights.values())
        return {k: v / total_weight for k, v in base_weights.items()}
    
    def _calculate_weighted_score(self, factors: ScoringFactors, weights: Dict[str, float]) -> float:
        """Calculate weighted composite score from all factors."""
        factor_values = {
            'volume_z_score': factors.volume_z_score,
            'oi_momentum': factors.oi_momentum,
            'price_momentum': factors.price_momentum,
            'volatility_skew': factors.volatility_skew,
            'time_decay': factors.time_decay,
            'greeks_alignment': factors.greeks_alignment,
            'concentration_factor': factors.concentration_factor,
            'market_cap_factor': factors.market_cap_factor,
            'liquidity_factor': factors.liquidity_factor,
            'pattern_recognition': factors.pattern_recognition
        }
        
        weighted_score = sum(
            factor_values[factor] * weight 
            for factor, weight in weights.items()
        )
        
        return min(weighted_score, self.max_score)
    
    def _apply_market_adjustments(self, score: float, market_data: Dict[str, Any]) -> float:
        """Apply market condition adjustments to the base score."""
        adjusted_score = score
        
        # Market trend adjustment
        market_trend = market_data.get('market_trend', 'neutral')  # bullish, bearish, neutral
        if market_trend == 'bullish':
            adjusted_score *= 1.1  # Slight boost in bull markets
        elif market_trend == 'bearish':
            adjusted_score *= 1.15  # Larger boost in bear markets (more insider activity)
        
        # Sector rotation adjustment
        sector_momentum = market_data.get('sector_momentum', 0.0)
        if abs(sector_momentum) > 0.05:  # Strong sector moves
            adjusted_score *= 1.08
        
        # Economic events adjustment
        major_events_coming = market_data.get('major_events_within_week', False)
        if major_events_coming:
            adjusted_score *= 1.12  # Higher likelihood of insider activity before events
        
        return min(adjusted_score, self.max_score)
    
    def _calculate_confidence(self, factors: ScoringFactors, market_data: Dict[str, Any]) -> float:
        """Calculate confidence level in the scoring."""
        confidence_factors = []
        
        # Data quality confidence
        data_completeness = market_data.get('data_completeness', 0.8)
        confidence_factors.append(data_completeness)
        
        # Statistical significance
        sample_size = market_data.get('baseline_sample_size', 10)
        stat_confidence = min(sample_size / 30, 1.0)  # Max confidence at 30+ samples
        confidence_factors.append(stat_confidence)
        
        # Signal consistency (multiple factors pointing same direction)
        strong_factors = sum([
            1 for factor_value in [
                factors.volume_z_score, factors.oi_momentum, factors.price_momentum,
                factors.greeks_alignment, factors.pattern_recognition
            ] if factor_value > 1.0
        ])
        consistency = min(strong_factors / 3, 1.0)  # Max when 3+ factors strong
        confidence_factors.append(consistency)
        
        # Time stability (signal persists over time)
        time_stability = market_data.get('signal_stability', 0.7)
        confidence_factors.append(time_stability)
        
        return np.mean(confidence_factors)
    
    def _identify_supporting_factors(self, factors: ScoringFactors) -> List[str]:
        """Identify the strongest supporting factors."""
        factor_strengths = {
            'Exceptional Volume Spike': factors.volume_z_score,
            'Strong OI Momentum': factors.oi_momentum,
            'Price Momentum Alignment': factors.price_momentum,
            'Volatility Expansion': factors.volatility_skew,
            'Short Time Frame': factors.time_decay,
            'Favorable Greeks': factors.greeks_alignment,
            'Strike Concentration': factors.concentration_factor,
            'Small Cap Premium': factors.market_cap_factor,
            'Liquidity Surge': factors.liquidity_factor,
            'Pattern Recognition': factors.pattern_recognition
        }
        
        # Return factors with strength > 1.0, sorted by strength
        strong_factors = [
            name for name, strength in factor_strengths.items() 
            if strength > 1.0
        ]
        
        return sorted(strong_factors, key=lambda x: factor_strengths[x], reverse=True)[:5]
    
    def _identify_risk_factors(self, factors: ScoringFactors, market_data: Dict[str, Any]) -> List[str]:
        """Identify potential risk factors that could impact the trade."""
        risk_factors = []
        
        # Time decay risk
        days_to_expiry = market_data.get('days_to_expiry', 30)
        if days_to_expiry < 7:
            risk_factors.append('Very Short Time to Expiry')
        elif days_to_expiry < 14:
            risk_factors.append('Short Time to Expiry')
        
        # Liquidity risk
        if factors.liquidity_factor < 0.5:
            risk_factors.append('Low Liquidity')
        
        # High volatility risk
        if factors.volatility_skew > 1.5:
            risk_factors.append('High Volatility Environment')
        
        # Market cap risk
        market_cap = market_data.get('market_cap', 10000000000)
        if market_cap < 500000000:  # <500M
            risk_factors.append('Very Small Cap Risk')
        
        # Earnings proximity risk
        if market_data.get('earnings_within_7_days', False):
            risk_factors.append('Earnings Announcement Imminent')
        
        # Market trend risk
        if market_data.get('market_trend') == 'bearish':
            risk_factors.append('Bearish Market Environment')
        
        return risk_factors
    
    def _determine_conviction_level(self, score: float) -> str:
        """Determine conviction level based on score."""
        if score >= self.conviction_thresholds['EXTREME']:
            return 'EXTREME'
        elif score >= self.conviction_thresholds['HIGH']:
            return 'HIGH'
        elif score >= self.conviction_thresholds['MEDIUM']:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _calculate_expected_return(self, score: float, factors: ScoringFactors, 
                                 market_data: Dict[str, Any]) -> float:
        """Calculate expected return based on historical patterns."""
        # Base expected return scales with score
        base_return = min(score / self.max_score * 2.0, 2.0)  # Max 200% return
        
        # Adjust for time horizon
        days_to_expiry = market_data.get('days_to_expiry', 30)
        time_multiplier = max(1.0 - (days_to_expiry - 7) / 30, 0.3)  # Decay over time
        
        # Adjust for market conditions
        market_volatility = market_data.get('market_volatility', 0.20)
        vol_multiplier = 1.0 + (market_volatility - 0.20) * 2  # Higher returns in volatile markets
        
        # Greeks adjustment
        delta = abs(market_data.get('delta', 0.5))
        delta_multiplier = 0.5 + delta  # Higher delta = higher leverage
        
        expected_return = base_return * time_multiplier * vol_multiplier * delta_multiplier
        
        return min(expected_return, 5.0)  # Cap at 500%
    
    def _calculate_risk_factor(self, factors: ScoringFactors, market_data: Dict[str, Any]) -> float:
        """Calculate risk factor (0-1, where 1 is highest risk)."""
        risk_components = []
        
        # Time decay risk
        days_to_expiry = market_data.get('days_to_expiry', 30)
        time_risk = max((14 - days_to_expiry) / 14, 0)  # Higher risk as expiry approaches
        risk_components.append(time_risk)
        
        # Liquidity risk
        liquidity_risk = max((1.0 - factors.liquidity_factor / 2.0), 0)
        risk_components.append(liquidity_risk)
        
        # Volatility risk
        vol_risk = min(factors.volatility_skew / 3.0, 1.0)
        risk_components.append(vol_risk)
        
        # Market cap risk (smaller = riskier)
        market_cap = market_data.get('market_cap', 10000000000)
        if market_cap < 1000000000:
            cap_risk = 0.8
        elif market_cap < 5000000000:
            cap_risk = 0.4
        else:
            cap_risk = 0.1
        risk_components.append(cap_risk)
        
        return min(np.mean(risk_components), 1.0)
    
    def _estimate_time_horizon(self, factors: ScoringFactors, contract_data: Dict[str, Any]) -> int:
        """Estimate optimal time horizon for the trade in days."""
        days_to_expiry = contract_data.get('days_to_expiry', 30)
        
        # Base on expiry but consider other factors
        base_horizon = min(days_to_expiry, 21)  # Max 3 weeks
        
        # Adjust for signal strength
        signal_strength = factors.volume_z_score + factors.oi_momentum
        if signal_strength > 5.0:
            # Strong signals should move quickly
            base_horizon = min(base_horizon, 7)
        elif signal_strength > 3.0:
            base_horizon = min(base_horizon, 14)
        
        # Adjust for time decay
        if factors.time_decay > 1.5:
            # High time decay suggests very short horizon
            base_horizon = min(base_horizon, 5)
        
        return max(base_horizon, 1)
    
    def _calculate_pattern_score(self, contract_data: Dict[str, Any], 
                               market_data: Dict[str, Any]) -> float:
        """Calculate pattern recognition score based on known insider patterns."""
        pattern_score = 0.0
        
        # Pattern 1: Large OTM call volume before earnings
        if (contract_data.get('contract_type') == 'call' and
            contract_data.get('moneyness', 0) > 0.05 and
            market_data.get('earnings_within_30_days', False) and
            contract_data.get('volume_ratio', 1) > 10):
            pattern_score += 1.5
        
        # Pattern 2: Put volume spike before bad news
        if (contract_data.get('contract_type') == 'put' and
            contract_data.get('volume_ratio', 1) > 8 and
            market_data.get('negative_news_sentiment', 0) > 0.7):
            pattern_score += 1.2
        
        # Pattern 3: Short-dated options with high gamma
        if (contract_data.get('days_to_expiry', 30) < 14 and
            contract_data.get('gamma', 0) > 0.05 and
            contract_data.get('volume_ratio', 1) > 5):
            pattern_score += 1.0
        
        # Pattern 4: Unusual activity in illiquid name
        if (market_data.get('market_cap', 10000000000) < 2000000000 and
            contract_data.get('volume_ratio', 1) > 15 and
            contract_data.get('avg_volume', 100) < 500):
            pattern_score += 1.8
        
        # Pattern 5: Coordinated strikes (ladder pattern)
        if contract_data.get('strike_ladder_detected', False):
            pattern_score += 1.0
        
        return min(pattern_score, 3.0)  # Cap at 3.0


def score_symbol_with_advanced_engine(symbol: str, contract_data: List[Dict[str, Any]], 
                                    market_data: Dict[str, Any]) -> List[ConvictionRating]:
    """
    Score a symbol using the advanced scoring engine.
    
    Args:
        symbol: Stock symbol
        contract_data: List of options contracts data
        market_data: Market and fundamental data
        
    Returns:
        List of ConvictionRating objects for each significant contract
    """
    engine = AdvancedScoringEngine()
    ratings = []
    
    for contract in contract_data:
        try:
            rating = engine.calculate_comprehensive_score(symbol, contract, market_data)
            if rating.score >= 3.0:  # Only include meaningful scores
                ratings.append(rating)
        except Exception as e:
            logger.warning(f"Failed to score contract {contract.get('contract_ticker', 'unknown')}: {e}")
    
    # Sort by score descending
    return sorted(ratings, key=lambda x: x.score, reverse=True)


def get_top_conviction_plays(min_score: float = 7.5, max_results: int = 20) -> List[Dict[str, Any]]:
    """
    Get top conviction plays from current temp anomaly events.
    
    Args:
        min_score: Minimum score threshold
        max_results: Maximum number of results to return
        
    Returns:
        List of top conviction trading opportunities
    """
    sql = """
    SELECT 
        symbol,
        kind,
        score,
        details,
        direction,
        expiry_date,
        as_of_timestamp
    FROM temp_anomaly
    WHERE event_date = CURRENT_DATE
        AND score >= %s
    ORDER BY score DESC
    LIMIT %s
    """
    
    rows = db.execute_query(sql, (min_score, max_results))
    
    top_plays = []
    for row in rows:
        details = json.loads(row['details']) if row['details'] else {}
        
        play = {
            'symbol': row['symbol'],
            'score': float(row['score']),
            'kind': row['kind'],
            'direction': row['direction'],
            'expiry_date': str(row['expiry_date']) if row['expiry_date'] else None,
            'contract_ticker': details.get('contract_ticker'),
            'volume': details.get('volume', details.get('intraday_volume', 0)),
            'strike': details.get('strike', 0),
            'underlying_price': details.get('underlying_price', details.get('underlying', 0)),
            'timestamp': row['as_of_timestamp'],
            'details': details
        }
        
        top_plays.append(play)
    
    return top_plays
