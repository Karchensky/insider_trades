#!/usr/bin/env python3
"""
High-Conviction Insider Trading Anomaly Detection System

This system identifies potential insider trading activity using statistical analysis
of options trading patterns against 30-day baselines.

Scoring System (1-10 scale):
- Volume Anomaly (0-3): Z-score analysis vs baseline
- OTM Call Concentration (0-3): Short-term out-of-money calls  
- Directional Bias (0-2): Strong call/put preference
- Time Pressure (0-2): Near-term expiration clustering

Alert Threshold: Score >= 7.0 (high-conviction only)
"""

import logging
import time
import json
from datetime import date, datetime, timedelta
from typing import Dict, List, Any
from database.core.connection import db

logger = logging.getLogger(__name__)

class InsiderAnomalyDetector:
    def __init__(self, baseline_days: int = 30):
        self.baseline_days = baseline_days
        self.current_date = date.today()
    
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
            
            # NEW APPROACH: High-conviction insider trading detection
            # Focus on statistical anomalies, not absolute volumes
            high_conviction_anomalies = self._detect_high_conviction_insider_activity(intraday_data, baseline_stats)
            
            # Store results in database
            stored_count = self._store_anomalies(high_conviction_anomalies)
            
            execution_time = time.time() - start_time
            logger.info(f"High-conviction anomaly detection completed in {execution_time:.2f}s. Detected {stored_count} high-conviction anomalies")
            
            return {
                'success': True,
                'anomalies_detected': stored_count,
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
                        COALESCE(s.day_close, s.day_vwap, o.underlying_price, 0) as underlying_price,
                        o.as_of_timestamp
                    FROM latest_temp_option o
                    LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                    WHERE COALESCE(s.day_close, s.day_vwap, o.underlying_price, 0) > 0
                      AND o.contract_type IN ('call', 'put')
                      AND o.expiration_date > CURRENT_DATE
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
                # Now using option_contracts table for accurate contract metadata
                cur.execute("""
                    WITH daily_volumes AS (
                        SELECT 
                            dos.date,
                            dos.symbol,
                            oc.contract_type,
                            SUM(dos.volume) as total_volume,
                            COUNT(*) as total_contracts,
                            -- IV metrics
                            SUM(CASE WHEN dos.implied_volatility IS NOT NULL AND dos.implied_volatility > 0 THEN 1 ELSE 0 END) as iv_contracts,
                            SUM(CASE WHEN dos.implied_volatility IS NOT NULL AND dos.implied_volatility > 0 THEN dos.implied_volatility * dos.volume ELSE 0 END) / 
                            NULLIF(SUM(CASE WHEN dos.implied_volatility IS NOT NULL AND dos.implied_volatility > 0 THEN dos.volume ELSE 0 END), 0) as weighted_avg_iv,
                            -- Short-term options (<=21 days from date)
                            SUM(CASE WHEN oc.expiration_date <= dos.date + INTERVAL '21 days' THEN dos.volume ELSE 0 END) as short_term_volume,
                            COUNT(CASE WHEN oc.expiration_date <= dos.date + INTERVAL '21 days' THEN 1 END) as short_term_contracts,
                            -- OTM options using accurate contract metadata
                            SUM(CASE WHEN oc.contract_type = 'call' AND oc.strike_price > dss.close * 1.05 THEN dos.volume ELSE 0 END) as otm_call_volume,
                            COUNT(CASE WHEN oc.contract_type = 'call' AND oc.strike_price > dss.close * 1.05 THEN 1 END) as otm_call_contracts,
                            SUM(CASE WHEN oc.contract_type = 'put' AND oc.strike_price < dss.close * 0.95 THEN dos.volume ELSE 0 END) as otm_put_volume,
                            COUNT(CASE WHEN oc.contract_type = 'put' AND oc.strike_price < dss.close * 0.95 THEN 1 END) as otm_put_contracts
                        FROM daily_option_snapshot dos
                        INNER JOIN option_contracts oc ON dos.symbol = oc.symbol AND dos.contract_ticker = oc.contract_ticker
                        LEFT JOIN daily_stock_snapshot dss ON dos.symbol = dss.symbol AND dos.date = dss.date
                        WHERE dos.date BETWEEN %s AND %s
                          AND dos.volume > 0
                          AND oc.contract_type IN ('call', 'put')
                        GROUP BY dos.date, dos.symbol, oc.contract_type
                    )
                    SELECT 
                        symbol,
                        contract_type,
                        COUNT(DISTINCT date) as baseline_days_count,
                        -- Overall volume metrics
                        SUM(total_volume) / COUNT(DISTINCT date) as avg_daily_volume,
                        CASE WHEN COUNT(DISTINCT date) > 1 THEN STDDEV(total_volume) ELSE 0 END as stddev_daily_volume,
                        SUM(total_contracts) / COUNT(DISTINCT date) as avg_daily_contracts,
                        CASE WHEN COUNT(DISTINCT date) > 1 THEN STDDEV(total_contracts) ELSE 0 END as stddev_daily_contracts,
                        -- IV metrics
                        COUNT(DISTINCT CASE WHEN iv_contracts > 0 THEN date END) as baseline_iv_days_count,
                        SUM(weighted_avg_iv * iv_contracts) / NULLIF(SUM(iv_contracts), 0) as avg_daily_iv,
                        CASE WHEN COUNT(DISTINCT CASE WHEN iv_contracts > 0 THEN date END) > 1 THEN STDDEV(weighted_avg_iv) ELSE 0 END as stddev_daily_iv,
                        -- Short-term metrics
                        SUM(short_term_volume) / COUNT(DISTINCT date) as avg_daily_short_term_volume,
                        CASE WHEN COUNT(DISTINCT date) > 1 THEN STDDEV(short_term_volume) ELSE 0 END as stddev_daily_short_term_volume,
                        SUM(short_term_contracts) / COUNT(DISTINCT date) as avg_daily_short_term_contracts,
                        -- OTM metrics
                        SUM(otm_call_volume) / COUNT(DISTINCT date) as avg_daily_otm_call_volume,
                        CASE WHEN COUNT(DISTINCT date) > 1 THEN STDDEV(otm_call_volume) ELSE 0 END as stddev_daily_otm_call_volume,
                        SUM(otm_put_volume) / COUNT(DISTINCT date) as avg_daily_otm_put_volume,
                        CASE WHEN COUNT(DISTINCT date) > 1 THEN STDDEV(otm_put_volume) ELSE 0 END as stddev_daily_otm_put_volume
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
                        'avg_daily_contracts': float(data['avg_daily_contracts']) if data['avg_daily_contracts'] else 0,
                        'stddev_daily_contracts': float(data['stddev_daily_contracts']) if data['stddev_daily_contracts'] else 0,
                        'avg_daily_short_term_volume': float(data['avg_daily_short_term_volume']) if data['avg_daily_short_term_volume'] else 0,
                        'stddev_daily_short_term_volume': float(data['stddev_daily_short_term_volume']) if data['stddev_daily_short_term_volume'] else 0,
                        'avg_daily_otm_call_volume': float(data['avg_daily_otm_call_volume']) if data['avg_daily_otm_call_volume'] else 0,
                        'stddev_daily_otm_call_volume': float(data['stddev_daily_otm_call_volume']) if data['stddev_daily_otm_call_volume'] else 0,
                        'avg_daily_otm_put_volume': float(data['avg_daily_otm_put_volume']) if data['avg_daily_otm_put_volume'] else 0,
                        'stddev_daily_otm_put_volume': float(data['stddev_daily_otm_put_volume']) if data['stddev_daily_otm_put_volume'] else 0
                    }
                    
                    iv_stats[key] = {
                        'symbol': symbol,
                        'contract_type': contract_type,
                        'baseline_iv_days_count': data['baseline_iv_days_count'],
                        'avg_iv': float(data['avg_daily_iv']) if data['avg_daily_iv'] else 0,
                        'stddev_iv': float(data['stddev_daily_iv']) if data['stddev_daily_iv'] else 0
                    }
                
                return {
                    'volume_stats': volume_stats,
                    'iv_stats': iv_stats,
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
        
        for symbol, contracts in symbol_data.items():
            # Calculate symbol-level metrics
            call_volume = sum(c['session_volume'] for c in contracts if c['contract_type'] == 'call')
            put_volume = sum(c['session_volume'] for c in contracts if c['contract_type'] == 'put')
            total_volume = call_volume + put_volume
            
            # Skip low-activity symbols
            if total_volume < 500:  # Minimum volume threshold
                continue
            
            # Get baseline data for this symbol
            call_key = f"{symbol}_call"
            put_key = f"{symbol}_put"
            call_baseline = baseline.get('volume_stats', {}).get(call_key, {})
            put_baseline = baseline.get('volume_stats', {}).get(put_key, {})
            
            if not call_baseline and not put_baseline:
                continue  # No baseline data available
            
            # Calculate individual test scores (each 0-3 points max)
            volume_score = self._calculate_volume_anomaly_score_v2(call_volume, put_volume, call_baseline, put_baseline)
            otm_score = self._calculate_otm_call_score_v2(contracts)
            directional_score = self._calculate_directional_bias_score_v2(call_volume, put_volume, total_volume)
            time_pressure_score = self._calculate_time_pressure_score_v2(contracts)
            
            # Composite score (0-10 scale)
            composite_score = min(volume_score + otm_score + directional_score + time_pressure_score, 10.0)
            
            # Only flag high-conviction cases (score >= 7.0)
            if composite_score >= 7.0:
                high_conviction_symbols[symbol] = {
                    'symbol': symbol,
                    'composite_score': round(composite_score, 1),
                    'anomaly_types': ['high_conviction_insider_activity'],
                    'total_anomalies': 1,
                    'details': {
                        'volume_score': round(volume_score, 1),
                        'otm_call_score': round(otm_score, 1),
                        'directional_score': round(directional_score, 1),
                        'time_pressure_score': round(time_pressure_score, 1),
                        'call_volume': call_volume,
                        'put_volume': put_volume,
                        'total_volume': total_volume,
                        'call_baseline_avg': call_baseline.get('avg_daily_volume', 0),
                        'put_baseline_avg': put_baseline.get('avg_daily_volume', 0)
                    },
                    'max_individual_score': composite_score
                }
        
        logger.info(f"High-conviction analysis: {len(high_conviction_symbols)} symbols scored >= 7.0 out of {len(symbol_data)} analyzed")
        return high_conviction_symbols
    
    def _calculate_volume_anomaly_score_v2(self, call_volume: int, put_volume: int, call_baseline: Dict, put_baseline: Dict) -> float:
        """Calculate volume anomaly score (0-3 points)."""
        score = 0.0
        
        # Call volume z-score
        call_avg = call_baseline.get('avg_daily_volume', 0)
        call_std = call_baseline.get('stddev_daily_volume', 1)
        if call_std > 0 and call_avg > 0:
            call_z = abs(call_volume - call_avg) / call_std
            score += min(call_z / 3.0, 1.5)  # Max 1.5 points for calls
        
        # Put volume z-score  
        put_avg = put_baseline.get('avg_daily_volume', 0)
        put_std = put_baseline.get('stddev_daily_volume', 1)
        if put_std > 0 and put_avg > 0:
            put_z = abs(put_volume - put_avg) / put_std
            score += min(put_z / 3.0, 1.5)  # Max 1.5 points for puts
        
        return min(score, 3.0)
    
    def _calculate_otm_call_score_v2(self, contracts: List[Dict]) -> float:
        """Calculate out-of-the-money call concentration score (0-3 points)."""
        call_contracts = [c for c in contracts if c['contract_type'] == 'call']
        if not call_contracts:
            return 0.0
        
        # Get underlying price
        underlying_price = 0
        for contract in contracts:
            if contract.get('underlying_price'):
                underlying_price = float(contract['underlying_price'])
                break
        
        if underlying_price == 0:
            return 0.0
        
        # Calculate OTM metrics
        otm_call_volume = 0
        total_call_volume = 0
        short_term_otm_volume = 0
        
        today = date.today()
        
        for contract in call_contracts:
            volume = contract['session_volume']
            strike = float(contract['strike_price']) if contract['strike_price'] else 0
            exp_date = contract['expiration_date']
            
            total_call_volume += volume
            
            # OTM calls (strike > underlying * 1.05)
            if strike > underlying_price * 1.05:
                otm_call_volume += volume
                
                # Short-term OTM calls (highest conviction)
                if isinstance(exp_date, str):
                    exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
                
                days_to_exp = (exp_date - today).days
                if days_to_exp <= 21:  # 3 weeks or less
                    short_term_otm_volume += volume
        
        if total_call_volume == 0:
            return 0.0
        
        otm_ratio = otm_call_volume / total_call_volume
        short_term_ratio = short_term_otm_volume / total_call_volume
        
        # Scoring: Heavy weight on short-term OTM calls (classic insider pattern)
        score = (otm_ratio * 1.5) + (short_term_ratio * 1.5)  # Max 3.0
        return min(score, 3.0)
    
    def _calculate_directional_bias_score_v2(self, call_volume: int, put_volume: int, total_volume: int) -> float:
        """Calculate directional bias score (0-2 points)."""
        if total_volume == 0:
            return 0.0
        
        call_ratio = call_volume / total_volume
        
        # Strong call bias (potential bullish insider info)
        if call_ratio > 0.8:  # 80%+ calls
            return 2.0
        elif call_ratio > 0.7:  # 70%+ calls
            return 1.5
        elif call_ratio > 0.6:  # 60%+ calls
            return 1.0
        elif call_ratio < 0.2:  # 80%+ puts (bearish insider info)
            return 1.5
        else:
            return 0.0
    
    def _calculate_time_pressure_score_v2(self, contracts: List[Dict]) -> float:
        """Calculate time pressure score based on expiration clustering (0-2 points)."""
        today = date.today()
        
        # Group by expiration
        exp_volumes = {}
        total_volume = 0
        
        for contract in contracts:
            volume = contract['session_volume']
            exp_date = contract['expiration_date']
            
            if isinstance(exp_date, str):
                exp_date = datetime.strptime(exp_date, '%Y-%m-%d').date()
            
            days_to_exp = (exp_date - today).days
            total_volume += volume
            
            if days_to_exp <= 7:  # This week
                exp_volumes['this_week'] = exp_volumes.get('this_week', 0) + volume
            elif days_to_exp <= 21:  # Next 3 weeks
                exp_volumes['short_term'] = exp_volumes.get('short_term', 0) + volume
        
        if total_volume == 0:
            return 0.0
        
        this_week_ratio = exp_volumes.get('this_week', 0) / total_volume
        short_term_ratio = exp_volumes.get('short_term', 0) / total_volume
        
        # High scores for concentration in near-term expirations
        score = (this_week_ratio * 1.2) + (short_term_ratio * 0.8)  # Max 2.0
        return min(score, 2.0)

    def _store_anomalies(self, anomalies: Dict[str, Dict]) -> int:
        """Store detected anomalies in the temp_anomaly table."""
        if not anomalies:
            return 0
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                stored_count = 0
                
                for symbol, data in anomalies.items():
                    # Store the symbol-level anomaly
                    cur.execute("""
                        INSERT INTO temp_anomaly (
                            event_date, symbol, direction, score, anomaly_types, 
                            total_individual_anomalies, max_individual_score,
                            details, as_of_timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_date, symbol) 
                        DO UPDATE SET
                            direction = EXCLUDED.direction,
                            score = EXCLUDED.score,
                            anomaly_types = EXCLUDED.anomaly_types,
                            total_individual_anomalies = EXCLUDED.total_individual_anomalies,
                            max_individual_score = EXCLUDED.max_individual_score,
                            details = EXCLUDED.details,
                            as_of_timestamp = EXCLUDED.as_of_timestamp,
                            updated_at = CURRENT_TIMESTAMP
                    """, (
                        self.current_date,
                        symbol,
                        'mixed',  # Direction is determined by the composite analysis
                        data['composite_score'],
                        data['anomaly_types'],
                        data['total_anomalies'],
                        data['max_individual_score'],
                        json.dumps(data['details'], default=str),
                        datetime.now()
                    ))
                    stored_count += 1
                
                conn.commit()
                return stored_count
                
        except Exception as e:
            logger.error(f"Failed to store anomalies: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()


if __name__ == '__main__':
    # Run high-conviction detection with default settings
    detector = InsiderAnomalyDetector()
    results = detector.run_detection()
    print(f"High-conviction detection results: {results}")
