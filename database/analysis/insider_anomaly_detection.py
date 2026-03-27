#!/usr/bin/env python3
"""
Two-Tier Insider Trading Anomaly Detection System

TIER 1 — EVENT SCORING (symbol-level, gates alerts):
  Identifies unusual options activity using volume-based metrics:
  - volume_score >= threshold (volume anomaly z-score)
  - z_score >= threshold (raw statistical deviation)
  - vol_oi_score >= threshold (volume:OI ratio)
  - magnitude >= threshold (dollar volume, $50K+)
  Alert gate: 3+ of 4 factors met (is_high_conviction = True)
  Filters: NOT bot-driven (< 5% intraday move), NOT earnings-related

TIER 2 — CONTRACT SELECTION (contract-level, picks recommended option):
  Among tradeable contracts ($0.05–$5.00, vol > 50, direction-aligned):
  - Default strategy: max_volume (highest volume = most liquid)
  - Greek values stored for informational tracking

Legacy composite score (0-10) still computed and stored for backward compatibility.
"""

import logging
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Optional
import pytz
from database.core.connection import db

logger = logging.getLogger(__name__)

class InsiderAnomalyDetector:
    def __init__(self, baseline_days: int = 90, use_model: bool = True):
        self.baseline_days = baseline_days
        # Use EST timezone for all date/time operations
        self.est_tz = pytz.timezone('US/Eastern')
        self.current_date = datetime.now(self.est_tz).date()
        
        # ML model for P(TP100) prediction
        self._tp100_model = None
        self._use_model = use_model
        if use_model:
            self._load_tp100_model()
    
    def _load_tp100_model(self) -> bool:
        """Load the TP100 prediction model if available."""
        try:
            from analysis.tp100_model import TP100Model
            self._tp100_model = TP100Model()
            if self._tp100_model.load():
                logger.info(f"Loaded TP100 model version: {self._tp100_model.model_version}")
                return True
            else:
                logger.info("No trained TP100 model available - predictions disabled")
                self._tp100_model = None
                return False
        except Exception as e:
            logger.warning(f"Failed to load TP100 model: {e}")
            self._tp100_model = None
            return False
    
    def predict_tp100_probability(self, anomaly_data: Dict) -> Optional[float]:
        """
        Predict P(TP100) for an anomaly using the trained model.
        
        Args:
            anomaly_data: Dict with anomaly features
        
        Returns:
            Predicted probability (0-1) or None if model unavailable
        """
        if self._tp100_model is None:
            return None
        
        try:
            result = self._tp100_model.predict(anomaly_data)
            return result.get('predicted_tp100_prob')
        except Exception as e:
            logger.warning(f"TP100 prediction failed: {e}")
            return None
    
    def run_detection(self) -> Dict[str, Any]:
        """Run high-conviction insider trading anomaly detection (1-10 scoring)."""
        start_time = time.time()
        
        try:
            # Get current intraday options data
            intraday_data = self._get_current_intraday_data()
            if not intraday_data:
                logger.warning("No intraday options data found for analysis")
                return {'success': True, 'anomalies_detected': 0, 'message': 'No data to analyze'}
            
            logger.info(f"Analyzing {len(intraday_data)} option contracts")
            
            # Get baseline statistics for comparison
            baseline_stats = self._calculate_baseline_statistics()
            if not baseline_stats:
                logger.warning("No baseline statistics available for comparison")
                return {'success': True, 'anomalies_detected': 0, 'message': 'No baseline data available'}
            
            # High-conviction insider trading detection
            # Focus on statistical anomalies, not absolute volumes
            high_conviction_anomalies = self._detect_high_conviction_insider_activity(intraday_data, baseline_stats)
            
            execution_time = time.time() - start_time
            logger.info(f"High-conviction anomaly detection completed in {execution_time:.2f}s. Detected {len(high_conviction_anomalies)} high-conviction anomalies")
            
            return {
                'success': True,
                'anomalies_detected': len(high_conviction_anomalies),
                'execution_time': execution_time,
                'contracts_analyzed': len(intraday_data),
                'symbols_with_anomalies': len(high_conviction_anomalies),
                'detection_method': 'high_conviction_insider_focus',
                'anomalies': high_conviction_anomalies
            }
            
        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': time.time() - start_time
            }

    def _get_current_intraday_data(self) -> List[Dict[str, Any]]:
        """Get current intraday options data from temp_option table."""
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Get today's intraday options data with contract metadata and underlying price
                cur.execute("""
                    WITH latest_temp_stock AS (
                        -- Get the most recent stock data for each symbol
                        SELECT DISTINCT ON (symbol) 
                            symbol, day_close, day_vwap, as_of_timestamp
                        FROM temp_stock
                        WHERE day_close > 0 OR day_vwap > 0
                        ORDER BY symbol, as_of_timestamp DESC
                    ),
                    latest_temp_option AS (
                        -- Get the most recent option data for each contract
                        SELECT DISTINCT ON (symbol, contract_ticker) 
                            symbol, contract_ticker, session_volume, session_close, 
                            implied_volatility, open_interest, greeks_delta, 
                            greeks_gamma, greeks_theta, greeks_vega,
                            underlying_price, as_of_timestamp,
                            contract_type, strike_price, expiration_date
                        FROM temp_option
                        WHERE session_volume > 0
                        ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                    )
                    SELECT 
                        o.symbol,
                        o.contract_ticker,
                        o.contract_type,
                        o.strike_price,
                        o.expiration_date,
                        o.session_volume,
                        o.session_close,
                        o.implied_volatility,
                        o.open_interest,
                        o.greeks_delta,
                        o.greeks_gamma,
                        o.greeks_theta,
                        o.greeks_vega,
                        COALESCE(oc.shares_per_contract, 100) as shares_per_contract,
                        COALESCE(s.day_close, s.day_vwap, o.underlying_price, 0) as underlying_price,
                        o.as_of_timestamp
                    FROM latest_temp_option o
                    LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                    LEFT JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
                    WHERE COALESCE(s.day_close, s.day_vwap, o.underlying_price, 0) > 0
                      AND o.contract_type IN ('call', 'put')
                      AND o.expiration_date >= CURRENT_DATE
                    ORDER BY o.symbol, o.contract_ticker
                """)
                
                rows = cur.fetchall()
                
                # Handle both RealDictRow and tuple formats
                if rows and hasattr(rows[0], 'keys'):
                    # RealDictRow format
                    return [dict(row) for row in rows]
                else:
                    # Tuple format - convert to dict
                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in rows]
                    
        except Exception as e:
            logger.error(f"Failed to get current intraday data: {e}")
            return []
        finally:
            conn.close()

    def _calculate_baseline_statistics(self) -> Dict[str, Any]:
        """Calculate baseline statistics from historical daily snapshots."""
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Get available date range
                cur.execute("""
                    SELECT MIN(date) as earliest, MAX(date) as latest, COUNT(DISTINCT date) as days
                    FROM daily_option_snapshot
                    WHERE volume > 0
                """)
                
                result = cur.fetchone()
                if not result:
                    logger.warning("No historical data available for baseline calculation")
                    return {}
                
                # Handle both RealDictRow and tuple formats
                if hasattr(result, 'keys'):
                    earliest = result['earliest']
                    latest = result['latest'] 
                    total_days = result['days']
                else:
                    earliest, latest, total_days = result
                
                if not earliest:
                    logger.warning("No historical data available for baseline calculation")
                    return {}
                logger.info(f"Available daily_option_snapshot data: {earliest} to {latest} ({total_days} days)")
                
                # Calculate baseline period
                baseline_start = self.current_date - timedelta(days=self.baseline_days)
                baseline_end = self.current_date - timedelta(days=1)
                
                # Ensure we don't go beyond available data
                actual_start_date = max(baseline_start, earliest) if earliest else baseline_start
                actual_end_date = baseline_end
                
                logger.info(f"Baseline calculation: {actual_start_date} to {actual_end_date}")
                
                # Calculate comprehensive baseline statistics from historical data
                cur.execute("""
                    WITH daily_volumes AS (
                        SELECT 
                            dos.date,
                            oc.symbol,
                            oc.contract_type,
                            SUM(dos.volume) as total_volume,
                            SUM(dos.open_interest) as total_open_interest,
                            CASE WHEN SUM(dos.open_interest) > 0 then 1 else 0 end as open_interest_day,
                            CASE WHEN SUM(dos.open_interest) > 0 
                                THEN SUM(dos.volume)::DECIMAL / SUM(dos.open_interest) 
                                ELSE 0 END as daily_volume_oi_ratio
                        FROM daily_option_snapshot dos
                        INNER JOIN option_contracts oc ON dos.contract_ticker = oc.contract_ticker
                        WHERE dos.date BETWEEN %s AND %s
                          AND dos.volume > 0
                          AND oc.contract_type IN ('call', 'put')
                        GROUP BY dos.date, oc.symbol, oc.contract_type
                    )
                    SELECT 
                        symbol,
                        contract_type,
                        COUNT(DISTINCT date) as baseline_days_count,
                        COUNT(DISTINCT CASE WHEN OPEN_INTEREST_DAY  = 1 THEN DATE ELSE NULL end)  as OI_days_count,
                        -- Volume metrics (used in volume anomaly scoring)
                        SUM(total_volume) / COUNT(DISTINCT date) as avg_daily_volume,
                        STDDEV(total_volume) as stddev_daily_volume,
                        -- Volume:OI Ratio metrics (used in volume:OI ratio scoring)
                        AVG(case when OPEN_INTEREST_DAY = 1 then daily_volume_oi_ratio else null end) as avg_daily_volume_oi_ratio,
                        STDDEV(case when OPEN_INTEREST_DAY = 1 then daily_volume_oi_ratio else null end) as stddev_daily_volume_oi_ratio
                    FROM daily_volumes
                    WHERE contract_type != 'unknown'
                    GROUP BY symbol, contract_type
                    HAVING COUNT(DISTINCT date) >= 1
                """, (actual_start_date, actual_end_date))
                
                rows = cur.fetchall()
                logger.info(f"Retrieved {len(rows)} comprehensive baseline records")
                
                # Organize baseline data
                volume_stats = {}
                iv_stats = {}
                
                for row in rows:
                    if hasattr(row, 'keys'):
                        # RealDictRow format
                        data = dict(row)
                    else:
                        # Tuple format
                        columns = [desc[0] for desc in cur.description]
                        data = dict(zip(columns, row))
                    
                    symbol = data['symbol']
                    contract_type = data['contract_type']
                    key = f"{symbol}_{contract_type}"
                    
                    volume_stats[key] = {
                        'symbol': symbol,
                        'contract_type': contract_type,
                        'baseline_days_count': data['baseline_days_count'],
                        'avg_daily_volume': float(data['avg_daily_volume']) if data['avg_daily_volume'] else 0,
                        'stddev_daily_volume': float(data['stddev_daily_volume']) if data['stddev_daily_volume'] else 0,
                        'avg_daily_volume_oi_ratio': float(data['avg_daily_volume_oi_ratio']) if data['avg_daily_volume_oi_ratio'] else 0,
                        'stddev_daily_volume_oi_ratio': float(data['stddev_daily_volume_oi_ratio']) if data['stddev_daily_volume_oi_ratio'] else 0
                    }
                    
                    # Note: IV stats removed as they're not used in current scoring system
                    # iv_stats[key] = {
                    #     'symbol': symbol,
                    #     'contract_type': contract_type,
                    #     'baseline_iv_days_count': data['baseline_iv_days_count'],
                    #     'avg_iv': float(data['avg_daily_iv']) if data['avg_daily_iv'] else 0,
                    #     'stddev_iv': float(data['stddev_daily_iv']) if data['stddev_daily_iv'] else 0
                    # }
                
                return {
                    'volume_stats': volume_stats,
                    'baseline_period': {
                        'start_date': actual_start_date,
                        'end_date': actual_end_date,
                        'days': self.baseline_days
                    }
                }
                
        except Exception as e:
            logger.error(f"Failed to calculate baseline statistics: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {}
        finally:
            conn.close()
    
    def _get_intraday_price_moves(self, symbols: List[str]) -> Dict[str, float]:
        """
        Get intraday price movement percentage for each symbol.
        Used to detect bot-driven trades where stock already moved significantly.
        
        Returns dict mapping symbol to intraday % move (day_close-day_open)/day_open * 100
        """
        if not symbols:
            return {}
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Get today's stock prices from temp_stock (intraday data)
                # Use DISTINCT ON to get most recent record per symbol
                cur.execute("""
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        CASE 
                            WHEN day_open > 0 THEN ((day_close - day_open) / day_open) * 100
                            ELSE 0
                        END as intraday_pct
                    FROM temp_stock
                    WHERE symbol = ANY(%s)
                    ORDER BY symbol, as_of_timestamp DESC
                """, (symbols,))
                
                results = {}
                for row in cur.fetchall():
                    # Handle both dict-like (RealDictRow) and tuple access
                    if hasattr(row, 'keys'):
                        results[row['symbol']] = float(row['intraday_pct'] or 0)
                    else:
                        results[row[0]] = float(row[1] or 0)
                
                return results
                
        except Exception as e:
            logger.warning(f"Failed to get intraday price moves: {e}")
            return {}
        finally:
            conn.close()

    # =========================================================================
    # TWO-TIER SCORING ARCHITECTURE
    # =========================================================================
    #
    # TIER 1: EVENT SCORING (symbol-level) — gates whether to alert
    #   Uses volume-based metrics that are truly predictive of TP100 on the
    #   recommended contract. These are symbol-level aggregates.
    #
    # TIER 2: CONTRACT SELECTION (contract-level) — picks the best option
    #   Uses Greeks (gamma, risk/reward) to select which contract to recommend.
    #   Greeks are useful for picking contracts, not for gating events.
    #
    # See analysis.py (two-tier validation) for methodology.
    # =========================================================================

    # TIER 1: Event-level scoring thresholds
    # These gate whether an event triggers an alert (score >= 3 of 4)
    EVENT_SCORE_THRESHOLDS = {
        'volume_score': 2.0,   # Volume anomaly z-score component
        'z_score': 3.0,        # Raw z-score (statistical deviation from baseline)
        'vol_oi_score': 1.2,   # Volume:OI ratio score
        'magnitude': 50000,    # Dollar magnitude ($50K institutional threshold)
    }
    EVENT_MIN_SCORE = 3  # At least 3 of 4 factors must be met

    # TIER 2: Contract-level thresholds (used for Greeks-informed selection)
    # These are stored for informational purposes and contract ranking
    HIGH_CONVICTION_THRESHOLDS = {
        'gamma': 0.2560,      # Gamma — used for contract selection ranking
        'vega': 0.2299,       # Vega — used for contract selection ranking
        'magnitude': 50000,   # Dollar magnitude (shared with event scoring)
        'vol_oi_score': 1.2,  # Volume:OI ratio score (shared with event scoring)
    }

    # Legacy thresholds (kept for backward compatibility / tracking)
    LEGACY_THRESHOLDS = {
        'theta': 0.2743,
        'gamma': 0.2560,
        'vega': 0.2299,
        'otm_score': 1.2000
    }
    
    # Contract selection strategies
    CONTRACT_SELECTION_STRATEGIES = ['max_volume', 'max_gamma', 'best_rr', 'atm_preference', 'model_ranked']
    
    def _score_single_contract(self, contract: Dict, otm_score: float, underlying_price: float = None,
                                magnitude: float = 0, vol_oi_score: float = 0) -> Dict:
        """
        Score a single contract based on Greeks and symbol-level metrics.
        
        Updated scoring based on statistical validation:
        - gamma + vega + magnitude + vol_oi_score is the optimal combination
        Returns dict with all scoring components for the contract.
        """
        theta = abs(float(contract.get('greeks_theta') or 0))
        gamma = float(contract.get('greeks_gamma') or 0)
        vega = float(contract.get('greeks_vega') or 0)
        
        # New optimized scoring factors
        gamma_met = gamma >= self.HIGH_CONVICTION_THRESHOLDS['gamma']
        vega_met = vega >= self.HIGH_CONVICTION_THRESHOLDS['vega']
        magnitude_met = magnitude >= self.HIGH_CONVICTION_THRESHOLDS['magnitude']
        vol_oi_met = vol_oi_score >= self.HIGH_CONVICTION_THRESHOLDS['vol_oi_score']
        
        # New high conviction score (0-4) based on optimal factors
        greeks_score = sum([gamma_met, vega_met, magnitude_met, vol_oi_met])
        
        # Legacy scoring for backward compatibility
        theta_met = theta >= self.LEGACY_THRESHOLDS['theta']
        otm_met = otm_score >= self.LEGACY_THRESHOLDS['otm_score']
        
        strike = float(contract.get('strike_price') or 0)
        moneyness = strike / underlying_price if underlying_price and underlying_price > 0 else None
        
        risk_reward = (gamma * vega / abs(theta)) if abs(theta) > 0.001 else 0
        
        return {
            'contract_ticker': contract.get('contract_ticker'),
            'contract_type': contract.get('contract_type'),
            'strike_price': strike,
            'expiration_date': str(contract.get('expiration_date')) if contract.get('expiration_date') else None,
            'session_volume': contract.get('session_volume') or 0,
            'session_close': contract.get('session_close') or 0,
            'theta': theta,
            'gamma': gamma,
            'vega': vega,
            # New optimized factors
            'gamma_met': gamma_met,
            'vega_met': vega_met,
            'magnitude_met': magnitude_met,
            'vol_oi_met': vol_oi_met,
            # Legacy factors (for backward compatibility)
            'theta_met': theta_met,
            'otm_met': otm_met,
            'greeks_score': greeks_score,
            'moneyness': moneyness,
            'risk_reward': risk_reward,
            'atm_distance': abs(moneyness - 1.0) if moneyness else 999,
        }
    
    def _calculate_high_conviction_score_multi(self, contracts: List[Dict], direction: str, otm_score: float, 
                                                underlying_price: float = None, 
                                                selection_strategy: str = 'max_volume',
                                                top_n: int = 3,
                                                magnitude: float = 0,
                                                vol_oi_score: float = 0) -> tuple:
        """
        Calculate high conviction score with multi-contract analysis.
        
        Two-tier system: event-level scoring (volume_score, z_score, vol_oi_score, magnitude)
        gates alerts; contract selection picks the recommended option.

        Returns tuple of:
        - high_conviction_score (0-4)
        - is_high_conviction (bool)
        - recommended_option (str)
        - component_flags (dict)
        - greek_values (dict)
        - contract_candidates (list of top N contracts with scores)
        - best_contracts (dict with best_gamma, best_theta, best_rr tickers)
        - selection_strategy (str)
        """
        if direction == 'call_heavy':
            eligible_contracts = [c for c in contracts if c['contract_type'] == 'call']
        elif direction == 'put_heavy':
            eligible_contracts = [c for c in contracts if c['contract_type'] == 'put']
        else:
            eligible_contracts = contracts
        
        tradeable_contracts = [
            c for c in eligible_contracts 
            if 0.05 <= (c.get('session_close') or 0) <= 5.00 
            and (c.get('session_volume') or 0) > 50
        ]
        
        empty_result = (
            0, False, None, 
            {'gamma': False, 'vega': False, 'magnitude': False, 'vol_oi': False}, 
            {},
            [],
            {'best_gamma': None, 'best_theta': None, 'best_rr': None},
            selection_strategy
        )
        
        if not tradeable_contracts:
            return empty_result
        
        scored_contracts = []
        for c in tradeable_contracts:
            score_data = self._score_single_contract(c, otm_score, underlying_price, magnitude, vol_oi_score)
            scored_contracts.append(score_data)
        
        for i, sc in enumerate(sorted(scored_contracts, key=lambda x: x['session_volume'], reverse=True)):
            sc['rank_volume'] = i + 1
        for i, sc in enumerate(sorted(scored_contracts, key=lambda x: x['gamma'], reverse=True)):
            sc['rank_gamma'] = i + 1
        for i, sc in enumerate(sorted(scored_contracts, key=lambda x: x['risk_reward'], reverse=True)):
            sc['rank_rr'] = i + 1
        for i, sc in enumerate(sorted(scored_contracts, key=lambda x: x['atm_distance'])):
            sc['rank_atm'] = i + 1
        
        best_by_volume = min(scored_contracts, key=lambda x: x['rank_volume'])
        best_by_gamma = min(scored_contracts, key=lambda x: x['rank_gamma'])
        best_by_rr = min(scored_contracts, key=lambda x: x['rank_rr'])
        best_by_atm = min(scored_contracts, key=lambda x: x['rank_atm'])
        best_by_theta = max(scored_contracts, key=lambda x: x['theta'])
        
        best_contracts = {
            'best_gamma': best_by_gamma['contract_ticker'],
            'best_theta': best_by_theta['contract_ticker'],
            'best_rr': best_by_rr['contract_ticker'],
        }
        
        # Model-ranked selection requires TP100 model predictions
        if selection_strategy == 'model_ranked' and self._tp100_model is not None:
            # Score each contract with the model
            for sc in scored_contracts:
                contract_features = {
                    'greeks_theta_value': sc['theta'],
                    'greeks_gamma_value': sc['gamma'],
                    'greeks_vega_value': sc['vega'],
                    'greeks_theta_met': sc['theta_met'],
                    'greeks_gamma_met': sc['gamma_met'],
                    'greeks_vega_met': sc['vega_met'],
                    'greeks_otm_met': sc['otm_met'],
                    'moneyness': sc['moneyness'],
                    'otm_score': otm_score,
                }
                pred = self._tp100_model.predict(contract_features)
                sc['predicted_tp100'] = pred.get('predicted_tp100_prob', 0)
            
            best_by_model = max(scored_contracts, key=lambda x: x.get('predicted_tp100', 0))
            selected = best_by_model
        elif selection_strategy == 'max_gamma':
            selected = best_by_gamma
        elif selection_strategy == 'best_rr':
            selected = best_by_rr
        elif selection_strategy == 'atm_preference':
            selected = best_by_atm
        else:
            selected = best_by_volume
            selection_strategy = 'max_volume'
        
        recommended_option = selected['contract_ticker']
        
        # New optimized component flags (gamma + vega + magnitude + vol_oi)
        component_flags = {
            'gamma': selected['gamma_met'],
            'vega': selected['vega_met'],
            'magnitude': selected['magnitude_met'],
            'vol_oi': selected['vol_oi_met'],
            # Legacy flags for backward compatibility
            'theta': selected['theta_met'],
            'otm': selected['otm_met'],
        }
        
        score = selected['greeks_score']
        is_high_conviction = score >= 3
        
        def calc_percentile(value: float, threshold: float) -> float:
            if value <= 0:
                return 0.0
            if value >= threshold:
                pct = 95.0 + min(((value - threshold) / threshold) * 5.0, 5.0)
            else:
                pct = (value / threshold) * 95.0
            return round(pct, 2)
        
        greek_values = {
            'gamma': selected['gamma'],
            'vega': selected['vega'],
            'magnitude': magnitude,
            'vol_oi_score': vol_oi_score,
            # Percentiles based on new thresholds
            'gamma_percentile': calc_percentile(selected['gamma'], self.HIGH_CONVICTION_THRESHOLDS['gamma']),
            'vega_percentile': calc_percentile(selected['vega'], self.HIGH_CONVICTION_THRESHOLDS['vega']),
            'magnitude_percentile': calc_percentile(magnitude, self.HIGH_CONVICTION_THRESHOLDS['magnitude']),
            'vol_oi_percentile': calc_percentile(vol_oi_score, self.HIGH_CONVICTION_THRESHOLDS['vol_oi_score']),
            # Legacy values
            'theta': selected['theta'],
            'otm': otm_score,
            'moneyness': selected['moneyness'],
            'risk_reward': selected['risk_reward'],
        }
        
        sorted_by_score = sorted(scored_contracts, key=lambda x: (-x['greeks_score'], -x['session_volume']))
        contract_candidates = []
        for sc in sorted_by_score[:top_n]:
            contract_candidates.append({
                'ticker': sc['contract_ticker'],
                'type': sc['contract_type'],
                'strike': sc['strike_price'],
                'expiry': sc['expiration_date'],
                'price': sc['session_close'],
                'volume': sc['session_volume'],
                'gamma': round(sc['gamma'], 6) if sc['gamma'] else None,
                'theta': round(sc['theta'], 6) if sc['theta'] else None,
                'vega': round(sc['vega'], 6) if sc['vega'] else None,
                'moneyness': round(sc['moneyness'], 4) if sc['moneyness'] else None,
                'greeks_score': sc['greeks_score'],
                'risk_reward': round(sc['risk_reward'], 4) if sc['risk_reward'] else None,
                'rank_volume': sc.get('rank_volume'),
                'rank_gamma': sc.get('rank_gamma'),
                'rank_rr': sc.get('rank_rr'),
            })
        
        return (
            score, 
            is_high_conviction, 
            recommended_option, 
            component_flags, 
            greek_values,
            contract_candidates,
            best_contracts,
            selection_strategy
        )
    
    def _calculate_high_conviction_score(self, contracts: List[Dict], direction: str, otm_score: float) -> tuple:
        """
        Calculate high conviction score based on option Greeks.
        
        Returns tuple of (high_conviction_score, is_high_conviction, recommended_option, component_flags, greek_values)
        
        Scoring: Count of factors above 93rd percentile thresholds
        - Theta >= 0.1624
        - Gamma >= 0.4683
        - Vega >= 0.1326
        - OTM Score >= 1.4
        
        High conviction: score >= 3 (at least 3 of 4 factors)
        
        NOTE: This is the legacy interface. Use _calculate_high_conviction_score_multi for full functionality.
        """
        result = self._calculate_high_conviction_score_multi(contracts, direction, otm_score)
        return (result[0], result[1], result[2], result[3], result[4])
    
    def _detect_high_conviction_insider_activity(self, data: List[Dict], baseline: Dict) -> Dict[str, Dict]:
        """
        NEW: High-conviction insider trading detection with 1-10 scoring.
        Focus on statistical anomalies vs baseline, not absolute volumes.
        """
        logger.info("Running high-conviction insider trading analysis...")
        
        # Group data by symbol
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(contract)
        
        high_conviction_symbols = {}
        all_anomaly_data = []  # Collect all anomaly data for bulk processing
        
        # Get most recent open interest for all symbols in one query (performance optimization)
        symbols_list = list(symbol_data.keys())
        
        # Get intraday stock price movement for bot-driven detection
        intraday_moves = self._get_intraday_price_moves(symbols_list)
        
        for symbol, contracts in symbol_data.items():
            # Calculate symbol-level metrics
            call_volume = int(sum(c['session_volume'] for c in contracts if c['contract_type'] == 'call'))
            put_volume = int(sum(c['session_volume'] for c in contracts if c['contract_type'] == 'put'))
            total_volume = call_volume + put_volume
            
            # Calculate call and put open interest separately
            call_open_interest = int(sum(c.get('open_interest', 0) for c in contracts if c['contract_type'] == 'call'))
            put_open_interest = int(sum(c.get('open_interest', 0) for c in contracts if c['contract_type'] == 'put'))
            
            # Calculate call and put magnitude separately (volume * price * shares_per_contract)
            # shares_per_contract is already fetched in the main query, so no additional DB lookup needed
            call_magnitude = 0
            put_magnitude = 0
            
            for c in contracts:
                volume = c.get('session_volume', 0) or 0
                price = c.get('session_close', 0) or 0
                shares_per_contract = c.get('shares_per_contract') or 100  # Default to 100 if None or missing
                
                magnitude = volume * price * shares_per_contract
                
                if c['contract_type'] == 'call':
                    call_magnitude += magnitude
                else:
                    put_magnitude += magnitude
            
            call_magnitude = int(call_magnitude)
            put_magnitude = int(put_magnitude)
            total_magnitude = call_magnitude + put_magnitude
            
            # Process all symbols regardless of volume
            
            # Get baseline data for this symbol
            call_key = f"{symbol}_call"
            put_key = f"{symbol}_put"
            call_baseline = baseline.get('volume_stats', {}).get(call_key, {})
            put_baseline = baseline.get('volume_stats', {}).get(put_key, {})
            
            if not call_baseline and not put_baseline:
                continue  # No baseline data available
            
            # Calculate individual test scores
            volume_score = self._calculate_volume_anomaly_score_v2(call_volume, put_volume, call_baseline, put_baseline)
            volume_oi_ratio_score = self._calculate_volume_oi_ratio_score(call_volume, put_volume, call_open_interest, put_open_interest, call_baseline, put_baseline)
            otm_score = self._calculate_otm_call_score_v2(contracts)
            directional_score = self._calculate_directional_bias_score_v2(call_volume, put_volume, total_volume, call_magnitude, put_magnitude, total_magnitude)
            time_pressure_score = self._calculate_time_pressure_score_v2(contracts)
            
            # Calculate additional metrics for enhanced storage
            call_baseline_avg = float(call_baseline.get('avg_daily_volume', 0))
            put_baseline_avg = float(put_baseline.get('avg_daily_volume', 0))
            
            # Calculate z-scores for pattern analysis
            call_std = float(call_baseline.get('stddev_daily_volume', 1))
            put_std = float(put_baseline.get('stddev_daily_volume', 1))
            call_z_score = abs(call_volume - call_baseline_avg) / call_std if call_std > 0 else 0
            put_z_score = abs(put_volume - put_baseline_avg) / put_std if put_std > 0 else 0
            max_z_score = max(call_z_score, put_z_score)
            
            # Calculate OTM and short-term percentages
            call_contracts = [c for c in contracts if c['contract_type'] == 'call']
            total_call_volume = int(sum(c['session_volume'] for c in call_contracts))
            
            otm_call_volume = 0
            short_term_volume = 0
            today = date.today()
            
            for contract in contracts:
                volume = int(contract['session_volume'])
                
                # Check if OTM call (strike > current price * 1.05)
                if (contract['contract_type'] == 'call' and 
                    float(contract.get('strike_price', 0)) > float(contract.get('underlying_price', 0)) * 1.05):
                    otm_call_volume += volume
                
                # Check if short-term (<=21 days)
                exp_date = contract.get('expiration_date')
                if isinstance(exp_date, str):
                    exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
                if exp_date and (exp_date - today).days <= 21:
                    short_term_volume += volume
            
            otm_call_percentage = (otm_call_volume / total_call_volume * 100) if total_call_volume > 0 else 0
            short_term_percentage = (short_term_volume / total_volume * 100) if total_volume > 0 else 0
            
            # Composite score (0-10 scale)
            composite_score = min(volume_score + volume_oi_ratio_score + otm_score + directional_score + time_pressure_score, 10.0)
            
            # Round composite score for consistent comparison and storage
            rounded_composite_score = round(composite_score, 1)
            
            # Determine direction for high conviction calculation
            if call_volume > put_volume * 2:
                direction = 'call_heavy'
            elif put_volume > call_volume * 2:
                direction = 'put_heavy'
            else:
                direction = 'mixed'
            
            # Get underlying price for moneyness calculation (from first contract with valid price)
            underlying_price = None
            for c in contracts:
                if c.get('underlying_price'):
                    underlying_price = float(c['underlying_price'])
                    break
            
            # Get configured contract selection strategy
            try:
                from config.contract_selection import get_active_strategy
                active_strategy = get_active_strategy().value
            except ImportError:
                active_strategy = 'max_volume'
            
            # Calculate high conviction score with multi-contract analysis
            # Pass magnitude and vol_oi_score for the new optimized scoring
            (high_conviction_score, is_high_conviction, recommended_option, 
             component_flags, greek_values, contract_candidates, 
             best_contracts, selection_strategy) = self._calculate_high_conviction_score_multi(
                contracts, direction, otm_score, underlying_price, 
                selection_strategy=active_strategy,
                magnitude=total_magnitude,
                vol_oi_score=volume_oi_ratio_score
            )
            
            # Get intraday price move for this symbol (for bot-driven detection)
            intraday_price_move_pct = intraday_moves.get(symbol, 0.0)

            # =========================================================
            # TIER 1: EVENT-LEVEL SCORING (volume-based, symbol-level)
            # This gates whether the event triggers an alert.
            # =========================================================
            event_volume_met = volume_score >= self.EVENT_SCORE_THRESHOLDS['volume_score']
            event_z_score_met = max_z_score >= self.EVENT_SCORE_THRESHOLDS['z_score']
            event_vol_oi_met = volume_oi_ratio_score >= self.EVENT_SCORE_THRESHOLDS['vol_oi_score']
            event_magnitude_met = total_magnitude >= self.EVENT_SCORE_THRESHOLDS['magnitude']

            event_score = sum([event_volume_met, event_z_score_met, event_vol_oi_met, event_magnitude_met])
            is_high_conviction_event = event_score >= self.EVENT_MIN_SCORE

            # Store ALL scores in database for historical tracking
            anomaly_data = {
                'symbol': symbol,
                'composite_score': rounded_composite_score,
                'total_volume': total_volume,  # Add total_volume at top level for email filtering
                'total_magnitude': total_magnitude,  # Add total_magnitude for filtering
                'intraday_price_move_pct': intraday_price_move_pct,  # For bot-driven detection
                # EVENT SCORE drives is_high_conviction (not Greeks)
                'high_conviction_score': event_score,  # Event-level score (0-4)
                'is_high_conviction': is_high_conviction_event,  # Event score >= 3
                'recommended_option': recommended_option,  # Best tradeable option (selected by Greeks)
                'greeks_theta_met': component_flags.get('theta', False),  # Individual greek components
                'greeks_gamma_met': component_flags.get('gamma', False),
                'greeks_vega_met': component_flags.get('vega', False),
                'greeks_otm_met': component_flags.get('otm', False),
                'greeks_theta_value': greek_values.get('theta'),  # Actual greek values
                'greeks_gamma_value': greek_values.get('gamma'),
                'greeks_vega_value': greek_values.get('vega'),
                'greeks_otm_value': greek_values.get('otm'),
                'greeks_theta_percentile': greek_values.get('theta_percentile'),  # Percentiles (0-100)
                'greeks_gamma_percentile': greek_values.get('gamma_percentile'),
                'greeks_vega_percentile': greek_values.get('vega_percentile'),
                'greeks_otm_percentile': greek_values.get('otm_percentile'),
                # NEW: Multi-contract scoring fields
                'contract_candidates': contract_candidates,  # Top N contracts with scores (JSON)
                'best_gamma_contract': best_contracts.get('best_gamma'),
                'best_theta_contract': best_contracts.get('best_theta'),
                'best_rr_contract': best_contracts.get('best_rr'),
                'contract_selection_strategy': selection_strategy,
                'anomaly_types': ['insider_activity'] if rounded_composite_score >= 7.5 and total_magnitude >= 20000 else ['low_score_activity'],
                'total_anomalies': 1,
                'details': {
                    'volume_score': round(volume_score, 1),
                    'volume_oi_ratio_score': round(volume_oi_ratio_score, 1),
                    'otm_score': round(otm_score, 1),
                    'directional_score': round(directional_score, 1),
                    'time_score': round(time_pressure_score, 1),
                    'call_volume': call_volume,
                    'put_volume': put_volume,
                    'total_volume': total_volume,
                    'call_magnitude': call_magnitude,
                    'put_magnitude': put_magnitude,
                    'total_magnitude': total_magnitude,
                    'call_open_interest': call_open_interest,
                    'put_open_interest': put_open_interest,
                    'call_baseline_avg': call_baseline_avg,
                    'put_baseline_avg': put_baseline_avg,
                    'z_score': round(max_z_score, 2),
                    'otm_call_percentage': round(otm_call_percentage, 1),
                    'short_term_percentage': round(short_term_percentage, 1),
                    'call_volume_oi_ratio': round(call_volume / call_open_interest, 4) if call_open_interest > 0 else 0,
                    'put_volume_oi_ratio': round(put_volume / put_open_interest, 4) if put_open_interest > 0 else 0,
                    'call_volume_oi_z_score': round(min(max((call_volume / call_open_interest - call_baseline.get('avg_daily_volume_oi_ratio', 0)) / call_baseline.get('stddev_daily_volume_oi_ratio', 1), -999.999), 999.999), 3) if call_open_interest > 0 and call_baseline.get('stddev_daily_volume_oi_ratio', 1) > 0 else 0,
                    'put_volume_oi_z_score': round(min(max((put_volume / put_open_interest - put_baseline.get('avg_daily_volume_oi_ratio', 0)) / put_baseline.get('stddev_daily_volume_oi_ratio', 1), -999.999), 999.999), 3) if put_open_interest > 0 and put_baseline.get('stddev_daily_volume_oi_ratio', 1) > 0 else 0,
                    'call_volume_oi_avg': round(call_baseline.get('avg_daily_volume_oi_ratio', 0), 4),
                    'put_volume_oi_avg': round(put_baseline.get('avg_daily_volume_oi_ratio', 0), 4)
                },
                'max_individual_score': composite_score
            }
            
            # Collect all anomaly data for bulk processing
            all_anomaly_data.append(anomaly_data)

            # Flag for notifications: event-level scoring gates alerts
            # Event score >= 3 AND magnitude >= $20K (magnitude is also in event score, but enforce floor)
            meets_event_threshold = is_high_conviction_event  # Event score >= 3
            meets_magnitude_threshold = total_magnitude >= 20000

            if meets_event_threshold and meets_magnitude_threshold:
                high_conviction_symbols[symbol] = anomaly_data
                logger.debug(
                    f"[ALERT CANDIDATE] {symbol}: Event score {event_score}/4 "
                    f"(vol={event_volume_met}, z={event_z_score_met}, "
                    f"voi={event_vol_oi_met}, mag={event_magnitude_met}) "
                    f"| Contract: {recommended_option} (strategy={selection_strategy})"
                )
        
        # Enrich anomaly data with additional features before storing
        if all_anomaly_data:
            logger.info(f"Enriching {len(all_anomaly_data)} anomalies with additional features...")
            all_anomaly_data = self.enrich_anomalies_with_features(all_anomaly_data)
            
            # Update high_conviction_symbols with enriched data
            for anomaly in all_anomaly_data:
                if anomaly['symbol'] in high_conviction_symbols:
                    high_conviction_symbols[anomaly['symbol']] = anomaly
            
            stored_count = self._store_anomalies_bulk(all_anomaly_data)
            logger.info(f"Bulk stored {stored_count} anomaly records in database")
        
        logger.info(f"Analysis complete: {len(high_conviction_symbols)} high-conviction events (event score >= {self.EVENT_MIN_SCORE}) out of {len(symbol_data)} symbols analyzed.")
        return high_conviction_symbols
    
    def _calculate_volume_anomaly_score_v2(self, call_volume: int, put_volume: int, call_baseline: Dict, put_baseline: Dict) -> float:
        """Calculate volume anomaly score (0-3 points) - only reward HIGH volume anomalies."""
        call_score = 0.0
        put_score = 0.0
        
        # Call volume z-score (only reward HIGH volume, not low volume)
        call_avg = float(call_baseline.get('avg_daily_volume', 0))
        call_std = float(call_baseline.get('stddev_daily_volume', 1))
        if call_std > 0 and call_avg > 0 and call_volume > call_avg:
            call_z = (call_volume - call_avg) / call_std  # Only positive z-scores (high volume)
            call_score = min(call_z / 2, 3.0)  # Divide by 2, max 3.0 points at 6 standard deviations
        
        # Put volume z-score (only reward HIGH volume, not low volume)
        put_avg = float(put_baseline.get('avg_daily_volume', 0))
        put_std = float(put_baseline.get('stddev_daily_volume', 1))
        if put_std > 0 and put_avg > 0 and put_volume > put_avg:
            put_z = (put_volume - put_avg) / put_std  # Only positive z-scores (high volume)
            put_score = min(put_z / 2, 3.0)  # Divide by 2, max 3.0 points at 6 standard deviations
        
        # Take the highest anomaly (either call or put direction)
        return max(call_score, put_score)
    
    
    def _calculate_volume_oi_ratio_score(self, call_volume: int, put_volume: int, 
                                        call_open_interest: int, put_open_interest: int,
                                        call_baseline: Dict, put_baseline: Dict) -> float:
        """
        Calculate volume:open interest ratio anomaly score (0-2 points).
        Uses z-score analysis vs historical baseline.
        
        Args:
            call_volume: Current day call volume
            put_volume: Current day put volume  
            call_open_interest: Current day call open interest
            put_open_interest: Current day put open interest
            call_baseline: Call baseline statistics
            put_baseline: Put baseline statistics
        
        Returns:
            float: Score (0-2 points)
        """
        call_score = 0.0
        put_score = 0.0
        
        # Call volume:OI ratio z-score (only reward HIGH ratios)
        if call_open_interest > 0:
            call_ratio = call_volume / call_open_interest
            call_avg_ratio = float(call_baseline.get('avg_daily_volume_oi_ratio', 0))
            call_std_ratio = float(call_baseline.get('stddev_daily_volume_oi_ratio', 1))
            
            if call_std_ratio > 0 and call_avg_ratio > 0 and call_ratio > call_avg_ratio:
                call_z = (call_ratio - call_avg_ratio) / call_std_ratio
                call_score = min(call_z, 4.0) / 2.0  # Max 2.0 points at 4 standard deviations
        
        # Put volume:OI ratio z-score (only reward HIGH ratios)
        if put_open_interest > 0:
            put_ratio = put_volume / put_open_interest
            put_avg_ratio = float(put_baseline.get('avg_daily_volume_oi_ratio', 0))
            put_std_ratio = float(put_baseline.get('stddev_daily_volume_oi_ratio', 1))
            
            if put_std_ratio > 0 and put_avg_ratio > 0 and put_ratio > put_avg_ratio:
                put_z = (put_ratio - put_avg_ratio) / put_std_ratio
                put_score = min(put_z, 4.0) / 2.0  # Max 2.0 points at 4 standard deviations
        
        # Take the highest anomaly (either call or put direction)
        return max(call_score, put_score)
    
    def _calculate_otm_call_score_v2(self, contracts: List[Dict]) -> float:
        """Calculate out-of-the-money options concentration score (0-2 points) - includes both calls and puts."""
        call_contracts = [c for c in contracts if c['contract_type'] == 'call']
        put_contracts = [c for c in contracts if c['contract_type'] == 'put']
        
        if not call_contracts and not put_contracts:
            return 0.0
        
        # Get underlying price
        underlying_price = 0
        for contract in contracts:
            if contract.get('underlying_price'):
                underlying_price = float(contract['underlying_price'])
                break
        
        if underlying_price == 0:
            return 0.0
        
        # Calculate OTM call metrics
        otm_call_volume = 0
        total_call_volume = 0
        short_term_otm_call_volume = 0
        
        # Calculate OTM put metrics
        otm_put_volume = 0
        total_put_volume = 0
        short_term_otm_put_volume = 0
        
        today = date.today()
        
        # Process call contracts
        for contract in call_contracts:
            volume = contract['session_volume']
            strike = float(contract['strike_price']) if contract['strike_price'] else 0
            exp_date = contract['expiration_date']
            
            total_call_volume += volume
            
            # OTM calls (strike > underlying * 1.05)
            if float(strike) > float(underlying_price) * 1.05:
                otm_call_volume += volume
                
                # Short-term OTM calls (highest conviction)
                if isinstance(exp_date, str):
                    exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
                
                days_to_exp = (exp_date - today).days
                if days_to_exp <= 21:  # 3 weeks or less
                    short_term_otm_call_volume += volume
        
        # Process put contracts
        for contract in put_contracts:
            volume = contract['session_volume']
            strike = float(contract['strike_price']) if contract['strike_price'] else 0
            exp_date = contract['expiration_date']
            
            total_put_volume += volume
            
            # OTM puts (strike < underlying * 0.95)
            if float(strike) < float(underlying_price) * 0.95:
                otm_put_volume += volume
                
                # Short-term OTM puts (highest conviction)
                if isinstance(exp_date, str):
                    exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
                
                days_to_exp = (exp_date - today).days
                if days_to_exp <= 21:  # 3 weeks or less
                    short_term_otm_put_volume += volume
        
        # Calculate call score
        call_score = 0.0
        if total_call_volume > 0:
            otm_call_ratio = float(otm_call_volume) / float(total_call_volume)
            short_term_call_ratio = float(short_term_otm_call_volume) / float(total_call_volume)
            call_score = (otm_call_ratio * 1.0) + (short_term_call_ratio * 1.0)
        
        # Calculate put score
        put_score = 0.0
        if total_put_volume > 0:
            otm_put_ratio = float(otm_put_volume) / float(total_put_volume)
            short_term_put_ratio = float(short_term_otm_put_volume) / float(total_put_volume)
            put_score = (otm_put_ratio * 1.0) + (short_term_put_ratio * 1.0)
        
        # Use the score from the direction with the most volume
        if total_call_volume > total_put_volume:
            score = call_score
        elif total_put_volume > total_call_volume:
            score = put_score
        else:
            # If volumes are equal, use the maximum score
            score = max(call_score, put_score)
        
        return min(score, 2.0)  # Cap at 2.0
    
    def _calculate_directional_bias_score_v2(self, call_volume: int, put_volume: int, total_volume: int, 
                                            call_magnitude: float, put_magnitude: float, total_magnitude: float) -> float:
        """Calculate directional bias score (0-1 points) using both volume and magnitude with directional consideration."""
        if total_volume == 0 or total_magnitude == 0:
            return 0.0
        
        # Calculate volume-based directional bias
        call_volume_ratio = call_volume / total_volume
        volume_distance_from_50_50 = abs(call_volume_ratio - 0.5)
        volume_score = volume_distance_from_50_50 * 2  # Max 1.0 for 100% calls or puts
        
        # Calculate magnitude-based directional bias
        call_magnitude_ratio = call_magnitude / total_magnitude
        magnitude_distance_from_50_50 = abs(call_magnitude_ratio - 0.5)
        magnitude_score = magnitude_distance_from_50_50 * 2  # Max 1.0 for 100% calls or puts
        
        # Determine direction for each component
        volume_call_direction = 1 if call_volume_ratio > 0.5 else -1  # 1 for call bias, -1 for put bias
        magnitude_call_direction = 1 if call_magnitude_ratio > 0.5 else -1  # 1 for call bias, -1 for put bias
        
        # Calculate directional scores (0 to 0.5 each)
        volume_directional_score = volume_score * 0.5 * volume_call_direction
        magnitude_directional_score = magnitude_score * 0.5 * magnitude_call_direction
        
        # Combine scores - they can counteract each other if directions differ
        total_directional_score = volume_directional_score + magnitude_directional_score
        
        # Convert back to absolute score (0 to 1.0)
        final_score = abs(total_directional_score)
        
        return min(final_score, 1.0)  # Cap at 1.0
    
    def _calculate_time_pressure_score_v2(self, contracts: List[Dict]) -> float:
        """Calculate time pressure score based on expiration clustering (0-2 points)."""
        today = date.today()
        
        # Group by expiration
        exp_volumes = {}
        total_volume = 0
        
        for contract in contracts:
            volume = int(contract['session_volume'])
            exp_date = contract['expiration_date']
            
            if isinstance(exp_date, str):
                exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
            
            days_to_exp = (exp_date - today).days
            total_volume += volume
            
            if days_to_exp <= 7:  # This week
                exp_volumes['this_week'] = exp_volumes.get('this_week', 0) + volume
            
            if days_to_exp <= 21:  # Short-term (includes this week)
                exp_volumes['short_term'] = exp_volumes.get('short_term', 0) + volume
        
        if total_volume == 0:
            return 0.0
        
        this_week_ratio = float(exp_volumes.get('this_week', 0)) / float(total_volume)
        short_term_ratio = float(exp_volumes.get('short_term', 0)) / float(total_volume)
        
        # High scores for concentration in near-term expirations
        score = (this_week_ratio * 1.2) + (short_term_ratio * 0.8)  # Max 2.0
        return min(score, 2.0)
    
    def _compute_cross_sectional_rank(self, all_anomalies: List[Dict]) -> Dict[str, float]:
        """
        Compute cross-sectional percentile rank for each symbol's volume z-score.
        
        Args:
            all_anomalies: List of all anomaly data dicts for the day
        
        Returns:
            Dict mapping symbol to percentile rank (0-100)
        """
        if not all_anomalies:
            return {}
        
        z_scores = []
        for a in all_anomalies:
            z = a.get('details', {}).get('z_score', 0)
            z_scores.append((a['symbol'], z))
        
        z_scores.sort(key=lambda x: x[1])
        n = len(z_scores)
        
        ranks = {}
        for i, (symbol, _) in enumerate(z_scores):
            ranks[symbol] = (i / (n - 1)) * 100 if n > 1 else 50.0
        
        return ranks
    
    def _get_historical_tp100_rates(self, symbols: List[str], lookback_days: int = 90) -> Dict[str, Dict]:
        """
        Get historical TP100 hit rates for symbols.
        
        Args:
            symbols: List of symbols to check
            lookback_days: Number of days to look back
        
        Returns:
            Dict mapping symbol to {rate, signal_count}
        """
        if not symbols:
            return {}
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                placeholders = ','.join(['%s'] * len(symbols))
                
                cur.execute(f"""
                    WITH historical_signals AS (
                        SELECT 
                            a.symbol,
                            a.event_date,
                            a.recommended_option,
                            o_entry.close_price AS entry_price,
                            (
                                SELECT MAX(o_future.close_price)
                                FROM daily_option_snapshot o_future
                                WHERE o_future.contract_ticker = a.recommended_option
                                  AND o_future.date > a.event_date
                            ) AS max_future_price
                        FROM daily_anomaly_snapshot a
                        LEFT JOIN daily_option_snapshot o_entry
                            ON a.recommended_option = o_entry.contract_ticker
                            AND a.event_date = o_entry.date
                        WHERE a.symbol IN ({placeholders})
                          AND a.event_date >= CURRENT_DATE - INTERVAL '%s days'
                          AND a.event_date < CURRENT_DATE
                          AND a.is_high_conviction = TRUE
                          AND a.recommended_option IS NOT NULL
                    )
                    SELECT 
                        symbol,
                        COUNT(*) AS signal_count,
                        SUM(CASE WHEN max_future_price >= 2.0 * entry_price THEN 1 ELSE 0 END) AS tp100_count
                    FROM historical_signals
                    WHERE entry_price IS NOT NULL AND entry_price > 0
                    GROUP BY symbol
                """, (*symbols, lookback_days))
                
                results = {}
                for row in cur.fetchall():
                    symbol, signal_count, tp100_count = row
                    rate = tp100_count / signal_count if signal_count > 0 else None
                    results[symbol] = {
                        'rate': rate,
                        'signal_count': signal_count,
                        'tp100_count': tp100_count
                    }
                
                for symbol in symbols:
                    if symbol not in results:
                        results[symbol] = {'rate': None, 'signal_count': 0, 'tp100_count': 0}
                
                return results
        finally:
            conn.close()
    
    def _get_contract_features(self, contract_ticker: str, event_date) -> Dict[str, Any]:
        """
        Get additional features for a specific contract.
        
        Args:
            contract_ticker: The option contract ticker
            event_date: The event date
        
        Returns:
            Dict with moneyness, days_to_expiry, iv_percentile, gamma_theta_ratio
        """
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        oc.strike_price,
                        oc.expiration_date,
                        o.implied_volatility,
                        o.greeks_gamma,
                        o.greeks_theta,
                        COALESCE(s.close, s.weighted_average_price) AS underlying_price
                    FROM option_contracts oc
                    LEFT JOIN daily_option_snapshot o 
                        ON oc.contract_ticker = o.contract_ticker AND o.date = %s
                    LEFT JOIN daily_stock_snapshot s
                        ON oc.underlying_ticker = s.symbol AND s.date = %s
                    WHERE oc.contract_ticker = %s
                """, (event_date, event_date, contract_ticker))
                
                row = cur.fetchone()
                if not row:
                    return {}
                
                strike, expiry, iv, gamma, theta, underlying = row
                
                features = {}
                
                if underlying and underlying > 0 and strike:
                    features['moneyness'] = float(strike) / float(underlying)
                    features['underlying_price'] = float(underlying)
                
                if expiry:
                    if isinstance(expiry, str):
                        from datetime import datetime
                        expiry = datetime.strptime(expiry, '%Y-%m-%d').date()
                    features['days_to_expiry'] = (expiry - event_date).days
                
                if gamma is not None and theta is not None and abs(theta) > 0.001:
                    features['gamma_theta_ratio'] = float(gamma) / abs(float(theta))
                
                if iv is not None:
                    cur.execute("""
                        SELECT 
                            PERCENT_RANK() OVER (ORDER BY implied_volatility) * 100 AS iv_pct
                        FROM daily_option_snapshot
                        WHERE contract_ticker = %s
                          AND date <= %s
                          AND implied_volatility IS NOT NULL
                        ORDER BY date DESC
                        LIMIT 1
                    """, (contract_ticker, event_date))
                    iv_row = cur.fetchone()
                    if iv_row:
                        features['iv_percentile'] = iv_row[0]
                
                return features
        finally:
            conn.close()
    
    def enrich_anomalies_with_features(self, anomalies_data: List[Dict]) -> List[Dict]:
        """
        Enrich anomaly data with additional computed features.
        
        Args:
            anomalies_data: List of anomaly dicts to enrich
        
        Returns:
            Enriched list with additional feature columns
        """
        if not anomalies_data:
            return anomalies_data
        
        volume_ranks = self._compute_cross_sectional_rank(anomalies_data)
        
        symbols = [a['symbol'] for a in anomalies_data]
        historical_rates = self._get_historical_tp100_rates(symbols)
        
        for anomaly in anomalies_data:
            symbol = anomaly['symbol']
            
            anomaly['volume_rank_percentile'] = volume_ranks.get(symbol)
            
            hist = historical_rates.get(symbol, {})
            anomaly['historical_tp100_rate'] = hist.get('rate')
            anomaly['historical_signal_count'] = hist.get('signal_count', 0)
            
            if anomaly.get('recommended_option'):
                contract_features = self._get_contract_features(
                    anomaly['recommended_option'], 
                    self.current_date
                )
                anomaly['moneyness'] = contract_features.get('moneyness')
                anomaly['days_to_expiry'] = contract_features.get('days_to_expiry')
                anomaly['iv_percentile'] = contract_features.get('iv_percentile')
                anomaly['gamma_theta_ratio'] = contract_features.get('gamma_theta_ratio')
                anomaly['underlying_price'] = contract_features.get('underlying_price')
        
        # Add model predictions if model is available
        if self._tp100_model is not None:
            logger.info("Adding TP100 model predictions...")
            for anomaly in anomalies_data:
                prob = self.predict_tp100_probability(anomaly)
                anomaly['predicted_tp100_prob'] = prob
        
        return anomalies_data

    def _store_anomalies_bulk(self, anomalies_data: List[Dict]) -> int:
        """Store multiple anomaly records in bulk using execute_values for efficiency."""
        if not anomalies_data:
            return 0
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Prepare data for bulk insert
                bulk_data = []
                
                for anomaly_data in anomalies_data:
                    details = anomaly_data['details']
                    
                    # Extract volume data
                    call_volume = details.get('call_volume', 0)
                    put_volume = details.get('put_volume', 0)
                    total_volume = call_volume + put_volume
                    
                    # Extract baseline data
                    call_baseline_avg = details.get('call_baseline_avg', 0)
                    put_baseline_avg = details.get('put_baseline_avg', 0)
                    
                    # Calculate multipliers
                    call_multiplier = call_volume / call_baseline_avg if call_baseline_avg > 0 else 0
                    put_multiplier = put_volume / put_baseline_avg if put_baseline_avg > 0 else 0
                    
                    # Determine direction
                    if call_volume > put_volume * 2:
                        direction = 'call_heavy'
                    elif put_volume > call_volume * 2:
                        direction = 'put_heavy'
                    else:
                        direction = 'mixed'
                    
                    # Create pattern description
                    pattern_description = f"{call_multiplier:.1f}x call volume, {call_volume/(total_volume)*100:.0f}% calls"
                    if details.get('otm_call_percentage', 0) > 80:
                        pattern_description += ", Heavy OTM calls"
                    if details.get('short_term_percentage', 0) > 70:
                        pattern_description += ", Short-term focus"
                    
                    # Add volume:OI ratio information
                    volume_oi_ratio_score = details.get('volume_oi_ratio_score', 0)
                    if volume_oi_ratio_score >= 1.5:
                        pattern_description += ", High volume:OI ratio"
                    elif volume_oi_ratio_score >= 1.0:
                        pattern_description += ", Elevated volume:OI ratio"
                    
                    # Calculate call/put ratio (cap at 9999.9999 to avoid database overflow)
                    original_ratio = call_volume / put_volume if put_volume > 0 else 9999.9999
                    call_put_ratio = min(original_ratio, 9999.9999)
                    if original_ratio > 9999.9999:
                        logger.warning(f"Capped call/put ratio for {anomaly_data['symbol']}: {original_ratio:.2f} -> 9999.9999")
                    
                    
                    # Convert contract_candidates to JSON string
                    import json
                    contract_candidates_json = json.dumps(anomaly_data.get('contract_candidates')) if anomaly_data.get('contract_candidates') else None
                    
                    # Prepare row data (including high conviction, multi-contract, and feature columns)
                    row_data = (
                        self.current_date,
                        anomaly_data['symbol'],
                        anomaly_data['composite_score'],
                        details.get('volume_score', 0),
                        details.get('volume_oi_ratio_score', 0),
                        details.get('otm_score', 0),
                        details.get('directional_score', 0),
                        details.get('time_score', 0),
                        call_volume,
                        put_volume,
                        total_volume,
                        details.get('call_open_interest', 0) + details.get('put_open_interest', 0),  # total open_interest
                        call_baseline_avg,
                        put_baseline_avg,
                        call_multiplier,
                        put_multiplier,
                        direction,
                        pattern_description,
                        details.get('z_score', 0),
                        details.get('otm_call_percentage', 0),
                        details.get('short_term_percentage', 0),
                        call_put_ratio,
                        details.get('call_open_interest', 0),
                        details.get('put_open_interest', 0),
                        details.get('call_volume_oi_ratio', 0),
                        details.get('put_volume_oi_ratio', 0),
                        details.get('call_volume_oi_z_score', 0),
                        details.get('put_volume_oi_z_score', 0),
                        details.get('call_volume_oi_avg', 0),
                        details.get('put_volume_oi_avg', 0),
                        details.get('call_magnitude', 0),  # call_magnitude
                        details.get('put_magnitude', 0),  # put_magnitude
                        details.get('total_magnitude', 0),  # total_magnitude
                        anomaly_data.get('high_conviction_score', 0),  # high conviction score (0-4)
                        anomaly_data.get('is_high_conviction', False),  # flag for score >= 3
                        anomaly_data.get('recommended_option'),  # best tradeable option ticker
                        anomaly_data.get('greeks_theta_met', False),  # individual component flags
                        anomaly_data.get('greeks_gamma_met', False),
                        anomaly_data.get('greeks_vega_met', False),
                        anomaly_data.get('greeks_otm_met', False),
                        anomaly_data.get('greeks_theta_value'),  # actual greek values
                        anomaly_data.get('greeks_gamma_value'),
                        anomaly_data.get('greeks_vega_value'),
                        anomaly_data.get('greeks_otm_value'),
                        anomaly_data.get('greeks_theta_percentile'),  # percentiles (0-100)
                        anomaly_data.get('greeks_gamma_percentile'),
                        anomaly_data.get('greeks_vega_percentile'),
                        anomaly_data.get('greeks_otm_percentile'),
                        # Multi-contract scoring columns
                        contract_candidates_json,  # JSON array of top N contracts
                        anomaly_data.get('best_gamma_contract'),
                        anomaly_data.get('best_theta_contract'),
                        anomaly_data.get('best_rr_contract'),
                        anomaly_data.get('contract_selection_strategy', 'max_volume'),
                        # Feature engineering columns
                        anomaly_data.get('volume_rank_percentile'),
                        anomaly_data.get('historical_tp100_rate'),
                        anomaly_data.get('historical_signal_count', 0),
                        anomaly_data.get('moneyness'),
                        anomaly_data.get('days_to_expiry'),
                        anomaly_data.get('iv_percentile'),
                        anomaly_data.get('gamma_theta_ratio'),
                        anomaly_data.get('underlying_price'),
                        # Model prediction
                        anomaly_data.get('predicted_tp100_prob'),
                        datetime.now(self.est_tz)
                    )
                    bulk_data.append(row_data)
                
                # Bulk insert with upsert logic
                from psycopg2.extras import execute_values
                
                insert_query = """
                    INSERT INTO daily_anomaly_snapshot (
                        event_date, symbol, total_score, volume_score, volume_oi_ratio_score, otm_score, 
                        directional_score, time_score, call_volume, put_volume, 
                        total_volume, open_interest, call_baseline_avg, put_baseline_avg, 
                        call_multiplier, put_multiplier, direction, pattern_description,
                        z_score, otm_call_percentage, short_term_percentage, call_put_ratio,
                        call_open_interest, put_open_interest, call_volume_oi_ratio, put_volume_oi_ratio,
                        call_volume_oi_z_score, put_volume_oi_z_score, call_volume_oi_avg, put_volume_oi_avg,
                        call_magnitude, put_magnitude, total_magnitude,
                        high_conviction_score, is_high_conviction, recommended_option,
                        greeks_theta_met, greeks_gamma_met, greeks_vega_met, greeks_otm_met,
                        greeks_theta_value, greeks_gamma_value, greeks_vega_value, greeks_otm_value,
                        greeks_theta_percentile, greeks_gamma_percentile, greeks_vega_percentile, greeks_otm_percentile,
                        contract_candidates, best_gamma_contract, best_theta_contract, best_rr_contract,
                        contract_selection_strategy,
                        volume_rank_percentile, historical_tp100_rate, historical_signal_count,
                        moneyness, days_to_expiry, iv_percentile, gamma_theta_ratio, underlying_price,
                        predicted_tp100_prob, as_of_timestamp
                    ) VALUES %s
                    ON CONFLICT (event_date, symbol)
                    DO UPDATE SET
                        total_score = EXCLUDED.total_score,
                        volume_score = EXCLUDED.volume_score,
                        volume_oi_ratio_score = EXCLUDED.volume_oi_ratio_score,
                        otm_score = EXCLUDED.otm_score,
                        directional_score = EXCLUDED.directional_score,
                        time_score = EXCLUDED.time_score,
                        call_volume = EXCLUDED.call_volume,
                        put_volume = EXCLUDED.put_volume,
                        total_volume = EXCLUDED.total_volume,
                        open_interest = EXCLUDED.open_interest,
                        call_baseline_avg = EXCLUDED.call_baseline_avg,
                        put_baseline_avg = EXCLUDED.put_baseline_avg,
                        call_multiplier = EXCLUDED.call_multiplier,
                        put_multiplier = EXCLUDED.put_multiplier,
                        direction = EXCLUDED.direction,
                        pattern_description = EXCLUDED.pattern_description,
                        z_score = EXCLUDED.z_score,
                        otm_call_percentage = EXCLUDED.otm_call_percentage,
                        short_term_percentage = EXCLUDED.short_term_percentage,
                        call_put_ratio = EXCLUDED.call_put_ratio,
                        call_open_interest = EXCLUDED.call_open_interest,
                        put_open_interest = EXCLUDED.put_open_interest,
                        call_volume_oi_ratio = EXCLUDED.call_volume_oi_ratio,
                        put_volume_oi_ratio = EXCLUDED.put_volume_oi_ratio,
                        call_volume_oi_z_score = EXCLUDED.call_volume_oi_z_score,
                        put_volume_oi_z_score = EXCLUDED.put_volume_oi_z_score,
                        call_volume_oi_avg = EXCLUDED.call_volume_oi_avg,
                        put_volume_oi_avg = EXCLUDED.put_volume_oi_avg,
                        call_magnitude = EXCLUDED.call_magnitude,
                        put_magnitude = EXCLUDED.put_magnitude,
                        total_magnitude = EXCLUDED.total_magnitude,
                        high_conviction_score = EXCLUDED.high_conviction_score,
                        is_high_conviction = EXCLUDED.is_high_conviction,
                        recommended_option = EXCLUDED.recommended_option,
                        greeks_theta_met = EXCLUDED.greeks_theta_met,
                        greeks_gamma_met = EXCLUDED.greeks_gamma_met,
                        greeks_vega_met = EXCLUDED.greeks_vega_met,
                        greeks_otm_met = EXCLUDED.greeks_otm_met,
                        greeks_theta_value = EXCLUDED.greeks_theta_value,
                        greeks_gamma_value = EXCLUDED.greeks_gamma_value,
                        greeks_vega_value = EXCLUDED.greeks_vega_value,
                        greeks_otm_value = EXCLUDED.greeks_otm_value,
                        greeks_theta_percentile = EXCLUDED.greeks_theta_percentile,
                        greeks_gamma_percentile = EXCLUDED.greeks_gamma_percentile,
                        greeks_vega_percentile = EXCLUDED.greeks_vega_percentile,
                        greeks_otm_percentile = EXCLUDED.greeks_otm_percentile,
                        contract_candidates = EXCLUDED.contract_candidates,
                        best_gamma_contract = EXCLUDED.best_gamma_contract,
                        best_theta_contract = EXCLUDED.best_theta_contract,
                        best_rr_contract = EXCLUDED.best_rr_contract,
                        contract_selection_strategy = EXCLUDED.contract_selection_strategy,
                        volume_rank_percentile = EXCLUDED.volume_rank_percentile,
                        historical_tp100_rate = EXCLUDED.historical_tp100_rate,
                        historical_signal_count = EXCLUDED.historical_signal_count,
                        moneyness = EXCLUDED.moneyness,
                        days_to_expiry = EXCLUDED.days_to_expiry,
                        iv_percentile = EXCLUDED.iv_percentile,
                        gamma_theta_ratio = EXCLUDED.gamma_theta_ratio,
                        underlying_price = EXCLUDED.underlying_price,
                        predicted_tp100_prob = EXCLUDED.predicted_tp100_prob,
                        as_of_timestamp = EXCLUDED.as_of_timestamp,
                        updated_at = CURRENT_TIMESTAMP
                """
                
                execute_values(cur, insert_query, bulk_data, template=None, page_size=1000)
                conn.commit()
                
                logger.info(f"Bulk inserted/updated {len(bulk_data)} anomaly records")
                return len(bulk_data)
                
        except Exception as e:
            logger.error(f"Failed to bulk store anomalies: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def store_enrichment_data(self, symbol: str, event_date, enrichment: Dict[str, Any]) -> bool:
        """
        Store enrichment results (novelty, news, EDGAR, conviction modifier)
        into the daily_anomaly_snapshot row for this symbol/event_date.

        Called after detection, only for high-conviction alerts.
        """
        import json
        conn = self.db.connect()
        try:
            with conn.cursor() as cur:
                news = enrichment.get('news', {})
                edgar = enrichment.get('edgar', {})
                novelty = enrichment.get('novelty', {})
                modifiers = enrichment.get('conviction_modifiers', {})

                cur.execute("""
                    UPDATE daily_anomaly_snapshot SET
                        enrichment_novelty_is_first = %s,
                        enrichment_novelty_count_30d = %s,
                        enrichment_novelty_score = %s,
                        enrichment_news_has_news = %s,
                        enrichment_news_count = %s,
                        enrichment_news_has_catalyst = %s,
                        enrichment_edgar_has_filings = %s,
                        enrichment_edgar_filing_count = %s,
                        enrichment_edgar_alignment = %s,
                        enrichment_conviction_modifier = %s,
                        enrichment_enriched_at = NOW(),
                        enrichment_raw_json = %s
                    WHERE event_date = %s AND symbol = %s
                """, (
                    novelty.get('is_first_trigger'),
                    novelty.get('trigger_count_30d'),
                    novelty.get('novelty_score'),
                    news.get('has_news'),
                    news.get('news_count'),
                    news.get('has_catalyst_news'),
                    edgar.get('has_filings'),
                    edgar.get('filing_count'),
                    edgar.get('insider_alignment'),
                    modifiers.get('net_modifier'),
                    json.dumps(enrichment, default=str),
                    event_date,
                    symbol,
                ))
                conn.commit()
                logger.info(f"Stored enrichment for {symbol}: modifier={modifiers.get('net_modifier', 'N/A')}")
                return True
        except Exception as e:
            logger.error(f"Failed to store enrichment for {symbol}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def check_signal_persistence(self, symbol: str, min_persistence: int = 2) -> Dict[str, Any]:
        """
        Check if a symbol has had high conviction signals in consecutive snapshots.
        
        Args:
            symbol: Stock symbol to check
            min_persistence: Minimum number of consecutive snapshots required (default: 2)
        
        Returns:
            Dict with persistence_count and whether it meets threshold
        """
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as snapshot_count,
                        MAX(signal_persistence_count) as max_persistence,
                        MAX(as_of_timestamp) as latest_timestamp
                    FROM daily_anomaly_snapshot
                    WHERE symbol = %s
                      AND event_date = %s
                      AND is_high_conviction = TRUE
                """, (symbol, self.current_date))
                
                row = cur.fetchone()
                if row and row[0] > 0:
                    return {
                        'symbol': symbol,
                        'snapshot_count': row[0],
                        'persistence_count': row[1] or 1,
                        'latest_timestamp': row[2],
                        'meets_threshold': (row[1] or 1) >= min_persistence
                    }
                
                return {
                    'symbol': symbol,
                    'snapshot_count': 0,
                    'persistence_count': 0,
                    'latest_timestamp': None,
                    'meets_threshold': False
                }
        finally:
            conn.close()
    
    def update_signal_persistence(self, symbols: List[str]) -> Dict[str, int]:
        """
        Update signal persistence counts for symbols with high conviction signals.
        
        For each symbol, check if there was a high conviction signal in the previous
        snapshot (same day, earlier timestamp). If so, increment the persistence count.
        
        Args:
            symbols: List of symbols that have high conviction signals in current snapshot
        
        Returns:
            Dict mapping symbol to updated persistence count
        """
        if not symbols:
            return {}
        
        conn = db.connect()
        persistence_counts = {}
        
        try:
            with conn.cursor() as cur:
                for symbol in symbols:
                    cur.execute("""
                        WITH current_snapshot AS (
                            SELECT as_of_timestamp
                            FROM daily_anomaly_snapshot
                            WHERE symbol = %s
                              AND event_date = %s
                              AND is_high_conviction = TRUE
                            ORDER BY as_of_timestamp DESC
                            LIMIT 1
                        ),
                        prior_snapshot AS (
                            SELECT signal_persistence_count
                            FROM daily_anomaly_snapshot a
                            WHERE a.symbol = %s
                              AND a.event_date = %s
                              AND a.is_high_conviction = TRUE
                              AND a.as_of_timestamp < (SELECT as_of_timestamp FROM current_snapshot)
                            ORDER BY a.as_of_timestamp DESC
                            LIMIT 1
                        )
                        SELECT COALESCE((SELECT signal_persistence_count FROM prior_snapshot), 0) + 1 AS new_count
                    """, (symbol, self.current_date, symbol, self.current_date))
                    
                    row = cur.fetchone()
                    new_count = row[0] if row else 1
                    persistence_counts[symbol] = new_count
                    
                    cur.execute("""
                        UPDATE daily_anomaly_snapshot
                        SET signal_persistence_count = %s
                        WHERE symbol = %s
                          AND event_date = %s
                          AND as_of_timestamp = (
                              SELECT MAX(as_of_timestamp)
                              FROM daily_anomaly_snapshot
                              WHERE symbol = %s AND event_date = %s
                          )
                    """, (new_count, symbol, self.current_date, symbol, self.current_date))
                
                conn.commit()
                logger.info(f"Updated persistence counts for {len(persistence_counts)} symbols")
                
        except Exception as e:
            logger.error(f"Failed to update signal persistence: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        return persistence_counts
    
    def get_persistent_signals(self, min_persistence: int = 2) -> List[Dict]:
        """
        Get all high conviction signals that have persisted for at least min_persistence snapshots.
        
        Args:
            min_persistence: Minimum number of consecutive snapshots (default: 2)
        
        Returns:
            List of anomaly records that meet persistence threshold
        """
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        symbol,
                        total_score,
                        high_conviction_score,
                        signal_persistence_count,
                        total_magnitude,
                        recommended_option,
                        contract_candidates,
                        best_gamma_contract,
                        as_of_timestamp
                    FROM daily_anomaly_snapshot
                    WHERE event_date = %s
                      AND is_high_conviction = TRUE
                      AND signal_persistence_count >= %s
                    ORDER BY signal_persistence_count DESC, high_conviction_score DESC
                """, (self.current_date, min_persistence))
                
                rows = cur.fetchall()
                columns = ['symbol', 'total_score', 'high_conviction_score', 'signal_persistence_count',
                          'total_magnitude', 'recommended_option', 'contract_candidates', 
                          'best_gamma_contract', 'as_of_timestamp']
                
                return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()


if __name__ == '__main__':
    # Run high-conviction detection with default settings
    detector = InsiderAnomalyDetector()
    results = detector.run_detection()
    print(f"High-conviction detection results: {results}")
