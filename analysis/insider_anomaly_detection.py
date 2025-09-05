"""
Insider Trading Anomaly Detection System

This module implements a comprehensive anomaly detection system designed to identify
high-conviction, potentially lucrative options trading patterns that may indicate
insider trading activity.

Key Focus Areas:
- High-volume, out-of-norm activity
- Short expiration, out-of-money call options
- Volume concentration across multiple contracts
- Strike coordination and directional bias
- Volatility and timing patterns

All anomalies are rolled up to the stock symbol level for actionable insights.
"""

import logging
import time
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Tuple, Optional
from decimal import Decimal
import json

from database.connection import db

logger = logging.getLogger(__name__)


class InsiderAnomalyDetector:
    """
    Advanced anomaly detection system for identifying potential insider trading patterns
    in options data.
    """
    
    def __init__(self, baseline_days: int = 30):
        """
        Initialize the anomaly detector.
        
        Args:
            baseline_days: Number of days to use for baseline calculations (default: 30)
        """
        self.baseline_days = baseline_days
        self.current_date = date.today()
        self.detection_timestamp = datetime.now()
        
    def run_detection(self) -> Dict[str, Any]:
        """
        Run the complete anomaly detection process for the current intraday data.
        
        Returns:
            Dict containing detection results and statistics
        """
        start_time = time.time()
        logger.info("Starting insider trading anomaly detection...")
        
        try:
            # Get current intraday options data
            intraday_data = self._get_current_intraday_data()
            if not intraday_data:
                logger.warning("No intraday options data found for analysis")
                return {'success': True, 'anomalies_detected': 0, 'message': 'No data to analyze'}
            
            logger.info(f"Analyzing {len(intraday_data)} option contracts")
            
            # Get baseline statistics for comparison
            baseline_stats = self._calculate_baseline_statistics()
            
            # NEW: High-conviction insider trading detection (1-10 scoring)
            # Focus on statistical anomalies vs baseline, not absolute volumes
            symbol_anomalies = self._detect_high_conviction_insider_activity(intraday_data, baseline_stats)
            
            # Store results in database
            stored_count = self._store_anomalies(symbol_anomalies)
            
            execution_time = time.time() - start_time
            logger.info(f"Anomaly detection completed in {execution_time:.2f}s. Detected {stored_count} symbol-level anomalies")
            
            return {
                'success': True,
                'anomalies_detected': stored_count,
                'execution_time': execution_time,
                'contracts_analyzed': len(intraday_data),
                'symbols_with_anomalies': len(symbol_anomalies)
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
                            underlying_price, as_of_timestamp
                        FROM temp_option
                        WHERE session_volume > 0
                        ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                    )
                    SELECT 
                        o.symbol,
                        o.contract_ticker,
                        -- Use option_contracts for accurate contract metadata
                        oc.contract_type,
                        oc.strike_price,
                        oc.expiration_date,
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
                    INNER JOIN option_contracts oc ON o.symbol = oc.symbol AND o.contract_ticker = oc.contract_ticker
                    LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                    WHERE COALESCE(s.day_close, s.day_vwap, o.underlying_price, 0) > 0
                      AND oc.contract_type IN ('call', 'put')
                      AND oc.expiration_date > CURRENT_DATE
                    ORDER BY o.symbol, o.contract_ticker
                """)
                
                rows = cur.fetchall()
                
                # Handle both RealDictRow and tuple formats
                if rows and hasattr(rows[0], 'keys'):
                    # Already RealDictRow objects, convert to regular dicts
                    data = [dict(row) for row in rows]
                else:
                    # Tuple format, convert to dicts
                    columns = [desc[0] for desc in cur.description]
                    data = [dict(zip(columns, row)) for row in rows]
                
                # Calculate derived fields for each contract
                for contract in data:
                    self._calculate_derived_fields(contract)
                
                return data
        finally:
            conn.close()
    
    def _calculate_derived_fields(self, contract: Dict[str, Any]) -> None:
        """Calculate derived fields like break-even price and underlying change."""
        try:
            strike_price = contract.get('strike_price')
            option_price = contract.get('session_close')
            underlying_price = contract.get('underlying_price')
            contract_type = contract.get('contract_type')
            
            if strike_price and option_price and underlying_price and contract_type:
                # Calculate break-even price
                if contract_type.lower() == 'call':
                    break_even = float(strike_price) + float(option_price)
                else:  # put
                    break_even = float(strike_price) - float(option_price)
                
                contract['calculated_break_even'] = break_even
                
                # Calculate underlying change to break even
                underlying_change = break_even - float(underlying_price)
                contract['calculated_underlying_change_to_break_even'] = underlying_change
                
                # Calculate moneyness (how far in/out of the money)
                if contract_type.lower() == 'call':
                    moneyness = float(underlying_price) - float(strike_price)
                else:  # put
                    moneyness = float(strike_price) - float(underlying_price)
                
                contract['moneyness'] = moneyness
                contract['is_otm'] = moneyness < 0  # Out of the money
                
        except (ValueError, TypeError) as e:
            # If we can't calculate derived fields, that's ok - just continue without them
            pass
    
    def _calculate_baseline_statistics(self) -> Dict[str, Any]:
        """Calculate baseline statistics from historical data for comparison."""
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Calculate baseline statistics from the last 30 days
                baseline_start = self.current_date - timedelta(days=self.baseline_days)
                
                # Check what historical data we have available
                cur.execute("""
                    SELECT 
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date,
                        COUNT(DISTINCT date) as total_days
                    FROM daily_option_snapshot
                    WHERE volume > 0
                """)
                data_range = cur.fetchone()
                if hasattr(data_range, 'keys'):
                    earliest = data_range['earliest_date']
                    latest = data_range['latest_date']
                    total_days = data_range['total_days']
                else:
                    earliest = data_range[0]
                    latest = data_range[1]
                    total_days = data_range[2]
                
                logger.info(f"Available daily_option_snapshot data: {earliest} to {latest} ({total_days} days)")
                
                # Use available date range instead of fixed 30 days if we don't have enough data
                actual_start_date = max(baseline_start, earliest) if earliest else baseline_start
                actual_end_date = self.current_date - timedelta(days=1)
                
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
                
                baseline_stats = cur.fetchall()
                logger.info(f"Retrieved {len(baseline_stats)} comprehensive baseline records")
                
                # Format comprehensive baseline statistics for easy lookup
                baseline = {
                    'volume_stats': {},
                    'iv_stats': {},
                    'short_term_stats': {},
                    'otm_stats': {}
                }
                
                for row in baseline_stats:
                    try:
                        # Handle both RealDictRow and tuple formats
                        if hasattr(row, 'keys'):  # RealDictRow
                            symbol = row['symbol']
                            contract_type = row['contract_type']
                            baseline_days_count = row['baseline_days_count']
                            avg_daily_volume = row['avg_daily_volume']
                            stddev_daily_volume = row['stddev_daily_volume']
                            avg_daily_contracts = row['avg_daily_contracts']
                            stddev_daily_contracts = row['stddev_daily_contracts']
                            baseline_iv_days_count = row['baseline_iv_days_count']
                            avg_daily_iv = row['avg_daily_iv']
                            stddev_daily_iv = row['stddev_daily_iv']
                            avg_daily_short_term_volume = row['avg_daily_short_term_volume']
                            stddev_daily_short_term_volume = row['stddev_daily_short_term_volume']
                            avg_daily_short_term_contracts = row['avg_daily_short_term_contracts']
                            avg_daily_otm_call_volume = row['avg_daily_otm_call_volume']
                            stddev_daily_otm_call_volume = row['stddev_daily_otm_call_volume']
                            avg_daily_otm_put_volume = row['avg_daily_otm_put_volume']
                            stddev_daily_otm_put_volume = row['stddev_daily_otm_put_volume']
                        else:  # Tuple - handle by index
                            symbol = row[0]
                            contract_type = row[1]
                            baseline_days_count = row[2]
                            avg_daily_volume = row[3]
                            stddev_daily_volume = row[4]
                            avg_daily_contracts = row[5]
                            stddev_daily_contracts = row[6]
                            baseline_iv_days_count = row[7]
                            avg_daily_iv = row[8]
                            stddev_daily_iv = row[9]
                            avg_daily_short_term_volume = row[10]
                            stddev_daily_short_term_volume = row[11]
                            avg_daily_short_term_contracts = row[12]
                            avg_daily_otm_call_volume = row[13]
                            stddev_daily_otm_call_volume = row[14]
                            avg_daily_otm_put_volume = row[15]
                            stddev_daily_otm_put_volume = row[16]
                        
                        key = f"{symbol}_{contract_type}"
                        
                        # Volume statistics
                        baseline['volume_stats'][key] = {
                            'days_count': int(baseline_days_count or 0),
                            'avg_daily_volume': float(avg_daily_volume or 0),
                            'stddev_daily_volume': float(stddev_daily_volume or 0),
                            'avg_daily_contracts': float(avg_daily_contracts or 0),
                            'stddev_daily_contracts': float(stddev_daily_contracts or 0)
                        }
                        
                        # IV statistics
                        baseline['iv_stats'][key] = {
                            'days_count': int(baseline_iv_days_count or 0),
                            'avg_iv': float(avg_daily_iv or 0),
                            'stddev_iv': float(stddev_daily_iv or 0)
                        }
                        
                        # Short-term statistics
                        baseline['short_term_stats'][key] = {
                            'days_count': int(baseline_days_count or 0),
                            'avg_daily_volume': float(avg_daily_short_term_volume or 0),
                            'stddev_daily_volume': float(stddev_daily_short_term_volume or 0),
                            'avg_daily_contracts': float(avg_daily_short_term_contracts or 0)
                        }
                        
                        # OTM statistics
                        baseline['otm_stats'][key] = {
                            'days_count': int(baseline_days_count or 0),
                            'avg_daily_call_volume': float(avg_daily_otm_call_volume or 0),
                            'stddev_daily_call_volume': float(stddev_daily_otm_call_volume or 0),
                            'avg_daily_put_volume': float(avg_daily_otm_put_volume or 0),
                            'stddev_daily_put_volume': float(stddev_daily_otm_put_volume or 0)
                        }
                        
                    except (KeyError, IndexError, TypeError) as e:
                        logger.warning(f"Error processing baseline stats row {row}: {e}")
                        logger.warning(f"Row keys: {list(row.keys()) if hasattr(row, 'keys') else 'tuple format'}")
                
                return baseline
        finally:
            conn.close()
    
    def _detect_volume_concentration(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect unusual volume concentration patterns."""
        anomalies = []
        
        # Group by symbol and analyze volume concentration
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(contract)
        
        for symbol, contracts in symbol_data.items():
            total_volume = sum(c['session_volume'] for c in contracts)
            if total_volume == 0:
                continue
            
            # Check against baseline
            call_key = f"{symbol}_call"
            put_key = f"{symbol}_put"
            
            call_baseline = baseline['volume_stats'].get(call_key, {})
            put_baseline = baseline['volume_stats'].get(put_key, {})
            
            call_volume = sum(c['session_volume'] for c in contracts if c['contract_type'] == 'call')
            put_volume = sum(c['session_volume'] for c in contracts if c['contract_type'] == 'put')
            
            # Calculate anomaly scores
            call_score_dict = self._calculate_volume_anomaly_score(call_volume, call_baseline)
            put_score_dict = self._calculate_volume_anomaly_score(put_volume, put_baseline)
            
            # Extract composite scores for comparison
            call_score = call_score_dict.get('composite_score', 0.0) if call_score_dict else 0.0
            put_score = put_score_dict.get('composite_score', 0.0) if put_score_dict else 0.0
            
            # High-volume concentration anomaly
            if call_score > 3.0 or put_score > 3.0:  # 3+ standard deviations
                anomalies.append({
                    'symbol': symbol,
                    'anomaly_type': 'volume_concentration',
                    'score': max(call_score, put_score),
                    'details': {
                        'call_volume': call_volume,
                        'put_volume': put_volume,
                        'call_scores': call_score_dict,
                        'put_scores': put_score_dict,
                        'call_composite_score': call_score,
                        'put_composite_score': put_score,
                        'total_contracts': len(contracts)
                    }
                })
        
        return anomalies
    
    def _detect_strike_coordination(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect coordinated activity across multiple strike prices."""
        anomalies = []
        
        # Group by symbol only (not by expiration)
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(contract)
        
        for symbol, contracts in symbol_data.items():
            # Look for multiple strikes with significant volume across all expirations
            high_volume_strikes = [c for c in contracts if c['session_volume'] > 100]  # Threshold
            
            if len(high_volume_strikes) >= 3:  # 3+ strikes with high volume
                total_volume = sum(c['session_volume'] for c in high_volume_strikes)
                
                # Get unique strikes and expirations involved
                unique_strikes = set(float(c['strike_price']) for c in high_volume_strikes)
                unique_expirations = set(c['expiration_date'] for c in high_volume_strikes)
                
                strike_spread = max(unique_strikes) - min(unique_strikes)
                
                # Calculate coordination score based on volume distribution and strike diversity
                avg_volume = float(total_volume) / len(high_volume_strikes)
                volume_consistency = 1.0 - (max(c['session_volume'] for c in high_volume_strikes) - min(c['session_volume'] for c in high_volume_strikes)) / float(total_volume)
                
                # Score based on: number of strikes, volume consistency, total volume, and expiration diversity
                strike_diversity = min(len(unique_strikes) / 5.0, 2.0)  # Cap at 2.0 for 5+ strikes
                expiration_diversity = min(len(unique_expirations) / 3.0, 1.5)  # Cap at 1.5 for 3+ expirations
                volume_factor = min(float(total_volume) / 5000, 3.0)  # Cap at 3.0 for 5000+ volume
                
                coordination_score = strike_diversity * volume_consistency * volume_factor * expiration_diversity
                
                if coordination_score > 2.0:  # Lowered threshold since we're not double-counting
                    anomalies.append({
                        'symbol': symbol,
                        'anomaly_type': 'strike_coordination',
                        'score': coordination_score,
                        'details': {
                            'unique_strikes': len(unique_strikes),
                            'unique_expirations': len(unique_expirations),
                            'coordinated_contracts': len(high_volume_strikes),
                            'total_volume': total_volume,
                            'strike_spread': float(strike_spread),
                            'volume_consistency': volume_consistency,
                            'strike_diversity': strike_diversity,
                            'expiration_diversity': expiration_diversity
                        }
                    })
        
        return anomalies
    
    def _detect_directional_bias(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect strong directional bias indicating conviction."""
        anomalies = []
        
        # Group by symbol
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = {'calls': [], 'puts': []}
            
            if contract['contract_type'] == 'call':
                symbol_data[symbol]['calls'].append(contract)
            else:
                symbol_data[symbol]['puts'].append(contract)
        
        for symbol, contracts in symbol_data.items():
            call_volume = sum(c['session_volume'] for c in contracts['calls'])
            put_volume = sum(c['session_volume'] for c in contracts['puts'])
            total_volume = call_volume + put_volume
            
            if total_volume < 50:  # Minimum volume threshold
                continue
            
            # Calculate call/put ratio
            if put_volume > 0:
                call_put_ratio = call_volume / put_volume
            else:
                call_put_ratio = float('inf') if call_volume > 0 else 1.0
            
            # Strong directional bias (heavily skewed toward calls or puts)
            bias_score = 0
            direction = 'neutral'
            
            if call_put_ratio > 5.0:  # Heavy call bias
                bias_score = min(10.0, call_put_ratio / 2)
                direction = 'bullish'
            elif call_put_ratio < 0.2 and call_put_ratio > 0:  # Heavy put bias
                bias_score = min(10.0, 5.0 / call_put_ratio)
                direction = 'bearish'
            elif call_put_ratio == 0:  # Only puts, no calls
                bias_score = 10.0
                direction = 'bearish'
            
            if bias_score > 3.0:
                anomalies.append({
                    'symbol': symbol,
                    'anomaly_type': 'directional_bias',
                    'score': bias_score,
                    'details': {
                        'direction': direction,
                        'call_volume': call_volume,
                        'put_volume': put_volume,
                        'call_put_ratio': call_put_ratio if call_put_ratio != float('inf') else 999.0,
                        'total_volume': total_volume
                    }
                })
        
        return anomalies
    
    def _detect_expiration_clustering(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect coordinated timing across expirations, with focus on OTM calls."""
        anomalies = []
        
        # Group by symbol
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = {}
            
            exp_date = contract['expiration_date']
            if exp_date not in symbol_data[symbol]:
                symbol_data[symbol][exp_date] = []
            symbol_data[symbol][exp_date].append(contract)
        
        for symbol, exp_data in symbol_data.items():
            # Look for short-term expirations with high activity, especially OTM calls
            short_term_exps = []
            otm_call_volume = 0
            
            for exp_date, contracts in exp_data.items():
                days_to_exp = (exp_date - self.current_date).days
                if days_to_exp <= 14:  # 2 weeks or less
                    total_volume = sum(c['session_volume'] for c in contracts)
                    
                    # Count OTM call volume specifically
                    exp_otm_call_volume = sum(
                        c['session_volume'] for c in contracts 
                        if c.get('contract_type', '').lower() == 'call' 
                        and c.get('is_otm', False)
                    )
                    
                    if total_volume > 100:
                        short_term_exps.append({
                            'exp_date': exp_date,
                            'days_to_exp': days_to_exp,
                            'volume': total_volume,
                            'otm_call_volume': exp_otm_call_volume,
                            'contracts': len(contracts)
                        })
                        otm_call_volume += exp_otm_call_volume
            
            # Score based on volume concentration in short-term expirations
            if len(short_term_exps) >= 1:  # Even one short-term exp can be significant
                total_short_volume = sum(exp['volume'] for exp in short_term_exps)
                total_all_volume = sum(sum(c['session_volume'] for c in contracts) for contracts in exp_data.values())
                
                if total_all_volume > 0:
                    short_term_ratio = total_short_volume / total_all_volume
                    otm_call_ratio = otm_call_volume / total_short_volume if total_short_volume > 0 else 0
                    
                    # Higher score for more volume concentrated in short expirations, especially OTM calls
                    clustering_score = (
                        short_term_ratio * 
                        len(short_term_exps) * 
                        min(5.0, total_short_volume / 300) *  # Lower threshold for sensitivity
                        (1 + otm_call_ratio)  # Bonus for OTM calls
                    )
                    
                    if clustering_score > 1.5:  # Lower threshold for better detection
                        anomalies.append({
                            'symbol': symbol,
                            'anomaly_type': 'expiration_clustering',
                            'score': clustering_score,
                            'details': {
                                'short_term_expirations': len(short_term_exps),
                                'short_term_volume': total_short_volume,
                                'otm_call_volume': otm_call_volume,
                                'short_term_ratio': short_term_ratio,
                                'otm_call_ratio': otm_call_ratio,
                                'avg_days_to_exp': sum(exp['days_to_exp'] for exp in short_term_exps) / len(short_term_exps)
                            }
                        })
        
        return anomalies
    
    def _detect_volatility_patterns(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect selective strike targeting based on implied volatility."""
        anomalies = []
        
        # Group by symbol
        symbol_data = {}
        for contract in data:
            if contract['implied_volatility'] and contract['implied_volatility'] > 0:
                symbol = contract['symbol']
                if symbol not in symbol_data:
                    symbol_data[symbol] = []
                symbol_data[symbol].append(contract)
        
        for symbol, contracts in symbol_data.items():
            if len(contracts) < 3:  # Need multiple contracts for pattern
                continue
            
            # Look for volume concentration in specific IV ranges
            high_volume_contracts = [c for c in contracts if c['session_volume'] > 50]
            
            if len(high_volume_contracts) >= 2:
                # Calculate IV statistics for high-volume contracts
                iv_values = [c['implied_volatility'] for c in high_volume_contracts]
                avg_iv = sum(iv_values) / len(iv_values)
                
                # Check against baseline IV
                call_key = f"{symbol}_call"
                put_key = f"{symbol}_put"
                
                call_baseline_iv = baseline['iv_stats'].get(call_key, {}).get('avg_iv', 0)
                put_baseline_iv = baseline['iv_stats'].get(put_key, {}).get('avg_iv', 0)
                baseline_iv = (call_baseline_iv + put_baseline_iv) / 2 if call_baseline_iv and put_baseline_iv else max(call_baseline_iv, put_baseline_iv)
                
                if baseline_iv > 0:
                    # Convert Decimal to float to avoid type errors
                    avg_iv_float = float(avg_iv) if avg_iv is not None else 0.0
                    baseline_iv_float = float(baseline_iv)
                    iv_deviation = abs(avg_iv_float - baseline_iv_float) / baseline_iv_float
                    
                    # Score based on IV deviation and volume concentration
                    total_volume = sum(c['session_volume'] for c in high_volume_contracts)
                    volatility_score = iv_deviation * min(5.0, total_volume / 200) * len(high_volume_contracts)
                    
                    if volatility_score > 2.0:
                        anomalies.append({
                            'symbol': symbol,
                            'anomaly_type': 'volatility_pattern',
                            'score': volatility_score,
                            'details': {
                                'high_volume_contracts': len(high_volume_contracts),
                                'avg_iv': float(avg_iv),
                                'baseline_iv': float(baseline_iv),
                                'iv_deviation': iv_deviation,
                                'total_volume': total_volume
                            }
                        })
        
        return anomalies
    
    def _detect_contract_anomalies(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect anomalous ratios of high-volume to normal contracts."""
        anomalies = []
        
        # Group by symbol
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(contract)
        
        for symbol, contracts in symbol_data.items():
            if len(contracts) < 5:  # Need sufficient contracts for ratio analysis
                continue
            
            # Define volume thresholds
            volumes = [c['session_volume'] for c in contracts]
            volumes.sort(reverse=True)
            
            # High volume threshold (top 20% or volume > 100)
            high_volume_threshold = max(100, volumes[max(0, len(volumes) // 5 - 1)])
            
            high_volume_contracts = [c for c in contracts if c['session_volume'] >= high_volume_threshold]
            normal_contracts = [c for c in contracts if c['session_volume'] < high_volume_threshold and c['session_volume'] > 0]
            
            if len(normal_contracts) > 0:
                high_to_normal_ratio = len(high_volume_contracts) / len(normal_contracts)
                total_high_volume = sum(c['session_volume'] for c in high_volume_contracts)
                total_normal_volume = sum(c['session_volume'] for c in normal_contracts)
                
                volume_concentration = total_high_volume / (total_high_volume + total_normal_volume) if (total_high_volume + total_normal_volume) > 0 else 0
                
                # Score based on contract ratio and volume concentration
                contract_score = high_to_normal_ratio * volume_concentration * min(3.0, len(high_volume_contracts))
                
                if contract_score > 1.5:
                    anomalies.append({
                        'symbol': symbol,
                        'anomaly_type': 'contract_anomaly',
                        'score': contract_score,
                        'details': {
                            'high_volume_contracts': len(high_volume_contracts),
                            'normal_contracts': len(normal_contracts),
                            'high_to_normal_ratio': high_to_normal_ratio,
                            'volume_concentration': volume_concentration,
                            'high_volume_threshold': high_volume_threshold
                        }
                    })
        
        return anomalies
    
    def _detect_otm_call_patterns(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect patterns specific to out-of-the-money calls with short expirations - classic insider trading pattern."""
        anomalies = []
        
        # Group by symbol
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(contract)
        
        for symbol, contracts in symbol_data.items():
            # Filter for OTM calls with short expirations
            otm_short_calls = []
            for contract in contracts:
                if (contract.get('contract_type', '').lower() == 'call' and
                    contract.get('is_otm', False) and
                    contract.get('expiration_date')):
                    
                    days_to_exp = (contract['expiration_date'] - self.current_date).days
                    if days_to_exp <= 21:  # 3 weeks or less
                        otm_short_calls.append(contract)
            
            if len(otm_short_calls) < 2:  # Need at least 2 contracts for pattern
                continue
            
            # Calculate pattern metrics
            total_otm_volume = sum(c['session_volume'] for c in otm_short_calls)
            total_all_volume = sum(c['session_volume'] for c in contracts)
            
            if total_otm_volume < 50:  # Minimum volume threshold
                continue
            
            # Calculate average days to expiration
            avg_days_to_exp = sum(
                (c['expiration_date'] - self.current_date).days for c in otm_short_calls
            ) / len(otm_short_calls)
            
            # Calculate how far out of the money on average
            avg_moneyness = sum(
                abs(c.get('moneyness', 0)) for c in otm_short_calls 
                if c.get('moneyness') is not None and c.get('moneyness') < 0
            ) / max(1, len([c for c in otm_short_calls if c.get('moneyness', 0) < 0]))
            
            # Score based on classic insider trading indicators
            otm_ratio = total_otm_volume / total_all_volume if total_all_volume > 0 else 0
            time_pressure_factor = max(1.0, (21 - avg_days_to_exp) / 21)  # Higher score for shorter time
            volume_factor = min(3.0, total_otm_volume / 100)  # Volume scaling
            otm_factor = min(2.0, avg_moneyness / 5.0) if avg_moneyness > 0 else 1.0  # How far OTM
            
            insider_score = (
                otm_ratio * 
                time_pressure_factor * 
                volume_factor * 
                otm_factor * 
                len(otm_short_calls)  # More contracts = higher conviction
            )
            
            if insider_score > 2.0:
                anomalies.append({
                    'symbol': symbol,
                    'anomaly_type': 'otm_call_insider_pattern',
                    'score': insider_score,
                    'details': {
                        'otm_call_contracts': len(otm_short_calls),
                        'total_otm_volume': total_otm_volume,
                        'otm_volume_ratio': otm_ratio,
                        'avg_days_to_exp': avg_days_to_exp,
                        'avg_moneyness': avg_moneyness,
                        'time_pressure_factor': time_pressure_factor,
                        'strikes_involved': list(set(c['strike_price'] for c in otm_short_calls))
                    }
                })
        
        return anomalies
    
    def _calculate_volume_anomaly_score(self, volume: int, baseline: Dict, 
                                      short_term_volume: int = 0, otm_call_volume: int = 0, 
                                      otm_put_volume: int = 0, contract_type: str = 'mixed') -> Dict[str, float]:
        """Calculate comprehensive anomaly scores based on volume vs baseline statistics."""
        scores = {
            'overall_volume_score': 0.0,
            'short_term_score': 0.0,
            'otm_call_score': 0.0,
            'otm_put_score': 0.0,
            'composite_score': 0.0
        }
        
        if not baseline or volume == 0:
            return scores
        
        # Overall volume anomaly score
        volume_stats = baseline.get('volume_stats', {})
        if volume_stats:
            avg_volume = volume_stats.get('avg_daily_volume', 0)
            stddev_volume = volume_stats.get('stddev_daily_volume', 0)
            
            if avg_volume > 0 and stddev_volume > 0:
                z_score = (volume - avg_volume) / stddev_volume
                scores['overall_volume_score'] = max(0.0, z_score)
        
        # Short-term volume anomaly score
        short_term_stats = baseline.get('short_term_stats', {})
        if short_term_stats and short_term_volume > 0:
            avg_short_term = short_term_stats.get('avg_daily_volume', 0)
            stddev_short_term = short_term_stats.get('stddev_daily_volume', 0)
            
            if avg_short_term > 0 and stddev_short_term > 0:
                z_score = (short_term_volume - avg_short_term) / stddev_short_term
                scores['short_term_score'] = max(0.0, z_score)
        
        # OTM call volume anomaly score
        otm_stats = baseline.get('otm_stats', {})
        if otm_stats and otm_call_volume > 0:
            avg_otm_call = otm_stats.get('avg_daily_call_volume', 0)
            stddev_otm_call = otm_stats.get('stddev_daily_call_volume', 0)
            
            if avg_otm_call > 0 and stddev_otm_call > 0:
                z_score = (otm_call_volume - avg_otm_call) / stddev_otm_call
                scores['otm_call_score'] = max(0.0, z_score)
        
        # OTM put volume anomaly score
        if otm_stats and otm_put_volume > 0:
            avg_otm_put = otm_stats.get('avg_daily_put_volume', 0)
            stddev_otm_put = otm_stats.get('stddev_daily_put_volume', 0)
            
            if avg_otm_put > 0 and stddev_otm_put > 0:
                z_score = (otm_put_volume - avg_otm_put) / stddev_otm_put
                scores['otm_put_score'] = max(0.0, z_score)
        
        # Calculate composite score with weighted components
        scores['composite_score'] = (
            scores['overall_volume_score'] * 0.4 +  # Overall volume baseline
            scores['short_term_score'] * 0.3 +      # Short-term focus (insider timing)
            scores['otm_call_score'] * 0.2 +        # OTM calls (classic insider pattern)
            scores['otm_put_score'] * 0.1           # OTM puts (less common but relevant)
        )
        
        return scores
    
    def _rollup_to_symbol_level(self, anomalies: List[Dict]) -> Dict[str, Dict]:
        """Roll up individual anomalies to symbol level with composite scoring."""
        symbol_anomalies = {}
        
        for anomaly in anomalies:
            symbol = anomaly['symbol']
            
            if symbol not in symbol_anomalies:
                symbol_anomalies[symbol] = {
                    'symbol': symbol,
                    'composite_score': 0.0,
                    'anomaly_types': [],
                    'total_anomalies': 0,
                    'details': {},
                    'max_individual_score': 0.0
                }
            
            # Add to composite score with diminishing returns
            current_score = symbol_anomalies[symbol]['composite_score']
            new_contribution = anomaly['score'] * (0.8 ** symbol_anomalies[symbol]['total_anomalies'])
            symbol_anomalies[symbol]['composite_score'] = current_score + new_contribution
            
            symbol_anomalies[symbol]['anomaly_types'].append(anomaly['anomaly_type'])
            symbol_anomalies[symbol]['total_anomalies'] += 1
            symbol_anomalies[symbol]['max_individual_score'] = max(symbol_anomalies[symbol]['max_individual_score'], anomaly['score'])
            symbol_anomalies[symbol]['details'][anomaly['anomaly_type']] = anomaly['details']
        
        # Filter to only high-conviction anomalies
        filtered_anomalies = {
            symbol: data for symbol, data in symbol_anomalies.items()
            if data['composite_score'] > 3.0 or data['max_individual_score'] > 4.0
        }
        
        return filtered_anomalies
    
    def _store_anomalies(self, anomalies: Dict[str, Dict]) -> int:
        """Store detected anomalies in the temp_anomaly table."""
        if not anomalies:
            return 0
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                stored_count = 0
                
                for symbol, data in anomalies.items():
                    # Determine primary direction based on anomaly details
                    direction = 'mixed'
                    if 'directional_bias' in data['details']:
                        direction = data['details']['directional_bias'].get('direction', 'mixed')
                    
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
                        direction,
                        float(data['composite_score']),
                        data['anomaly_types'],  # This should be a Python list for TEXT[] column
                        data['total_anomalies'],
                        float(data['max_individual_score']),
                        json.dumps(data['details'], default=str),
                        self.detection_timestamp
                    ))
                    
                    stored_count += 1
                
                conn.commit()
                return stored_count
                
        except Exception as e:
            logger.error(f"Failed to store anomalies: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def _detect_greeks_anomalies(self, data: List[Dict], baseline: Dict) -> List[Dict]:
        """Detect anomalies based on Greeks values (delta, gamma, theta, vega)."""
        anomalies = []
        symbol_data = {}
        for contract in data:
            symbol = contract['symbol']
            if symbol not in symbol_data:
                symbol_data[symbol] = []
            symbol_data[symbol].append(contract)
        
        for symbol, contracts in symbol_data.items():
            # Focus on contracts with significant volume and valid Greeks
            valid_contracts = [
                c for c in contracts 
                if (c.get('session_volume', 0) > 50 and 
                    c.get('greeks_delta') is not None and 
                    abs(float(c.get('greeks_delta', 0))) > 0.01)
            ]
            
            if len(valid_contracts) < 3:
                continue
            
            # Calculate Greeks metrics
            total_volume = sum(c['session_volume'] for c in valid_contracts)
            
            # Delta concentration (high delta calls suggest directional bets)
            high_delta_calls = [
                c for c in valid_contracts 
                if c.get('contract_type', '').lower() == 'call' and float(c.get('greeks_delta', 0)) > 0.7
            ]
            
            high_delta_volume = sum(c['session_volume'] for c in high_delta_calls)
            delta_concentration = high_delta_volume / total_volume if total_volume > 0 else 0
            
            # Gamma risk (high gamma = high sensitivity to price moves)
            high_gamma_contracts = [
                c for c in valid_contracts 
                if c.get('greeks_gamma') is not None and float(c.get('greeks_gamma', 0)) > 0.1
            ]
            
            gamma_volume = sum(c['session_volume'] for c in high_gamma_contracts)
            gamma_concentration = gamma_volume / total_volume if total_volume > 0 else 0
            
            # Theta decay risk (short-term options with high theta)
            high_theta_risk = [
                c for c in valid_contracts 
                if (c.get('greeks_theta') is not None and 
                    abs(float(c.get('greeks_theta', 0))) > 0.05 and
                    (c.get('expiration_date', self.current_date) - self.current_date).days <= 14)
            ]
            
            theta_volume = sum(c['session_volume'] for c in high_theta_risk)
            theta_concentration = theta_volume / total_volume if total_volume > 0 else 0
            
            # Calculate composite Greeks anomaly score
            greeks_score = 0
            
            # High delta concentration suggests strong directional conviction
            if delta_concentration > 0.6:
                greeks_score += delta_concentration * 3
            
            # High gamma concentration suggests volatility plays
            if gamma_concentration > 0.4:
                greeks_score += gamma_concentration * 2
                
            # High theta risk suggests time-sensitive trades
            if theta_concentration > 0.5:
                greeks_score += theta_concentration * 4
            
            # Volume factor
            volume_factor = min(2.0, total_volume / 1000)
            greeks_score *= volume_factor
            
            if greeks_score > 2.0:
                anomalies.append({
                    'symbol': symbol,
                    'anomaly_type': 'greeks_anomaly',
                    'score': greeks_score,
                    'details': {
                        'total_volume': total_volume,
                        'contracts_analyzed': len(valid_contracts),
                        'delta_concentration': delta_concentration,
                        'gamma_concentration': gamma_concentration,
                        'theta_concentration': theta_concentration,
                        'high_delta_calls': len(high_delta_calls),
                        'high_gamma_contracts': len(high_gamma_contracts),
                        'high_theta_risk_contracts': len(high_theta_risk)
                    }
                })
        
        return anomalies

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
        
        from datetime import date, datetime
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
        from datetime import date, datetime
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


def run_insider_anomaly_detection(baseline_days: int = 30) -> Dict[str, Any]:
    """
    Main entry point for running insider trading anomaly detection.
    
    Args:
        baseline_days: Number of days to use for baseline calculations
        
    Returns:
        Dict containing detection results
    """
    detector = InsiderAnomalyDetector(baseline_days=baseline_days)
    return detector.run_detection()


if __name__ == '__main__':
    # Run detection with default settings
    results = run_insider_anomaly_detection()
    print(f"Detection results: {results}")
