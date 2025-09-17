#!/usr/bin/env python3
"""
High-Conviction Insider Trading Anomaly Detection System

This system identifies potential insider trading activity using statistical analysis
of options trading patterns against 30-day baselines.

Scoring System (1-10 scale):
- Volume Anomaly (0-3): High volume z-score analysis vs baseline (only rewards above-average volume)
- Volume:Open Interest Ratio (0-2): Volume:OI ratio z-score vs historical baseline (only rewards above-average ratios)
- OTM Call Concentration (0-2): Short-term out-of-money calls  
- Directional Bias (0-1): Strong call/put preference
- Time Pressure (0-2): Near-term expiration clustering

Alert Threshold: Score >= 7.5 (high-conviction only)
"""

import logging
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Any
import pytz
from database.core.connection import db

logger = logging.getLogger(__name__)

class InsiderAnomalyDetector:
    def __init__(self, baseline_days: int = 30):
        self.baseline_days = baseline_days
        # Use EST timezone for all date/time operations
        self.est_tz = pytz.timezone('US/Eastern')
        self.current_date = datetime.now(self.est_tz).date()
    
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
            
            # Store ALL scores in database for historical tracking
            anomaly_data = {
                'symbol': symbol,
                'composite_score': rounded_composite_score,
                'total_volume': total_volume,  # Add total_volume at top level for email filtering
                'total_magnitude': total_magnitude,  # Add total_magnitude for filtering
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
            
            # Only flag high-conviction cases (score >= 7.5 AND magnitude >= $20,000) for notifications
            if rounded_composite_score >= 7.5 and total_magnitude >= 20000:
                high_conviction_symbols[symbol] = anomaly_data
        
        # Bulk store all anomaly data
        if all_anomaly_data:
            stored_count = self._store_anomalies_bulk(all_anomaly_data)
            logger.info(f"Bulk stored {stored_count} anomaly records in database")
        
        logger.info(f"Analysis complete: {len(high_conviction_symbols)} symbols scored >= 7.5 out of {len(symbol_data)} analyzed. All scores stored in database.")
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
                    
                    
                    # Prepare row data
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
                        as_of_timestamp
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


if __name__ == '__main__':
    # Run high-conviction detection with default settings
    detector = InsiderAnomalyDetector()
    results = detector.run_detection()
    print(f"High-conviction detection results: {results}")
