"""
Enhanced Anomaly Detection System for Insider Trading Patterns

This module provides a comprehensive anomaly detection system designed to identify
potential insider trading activity through unusual options trading patterns.

Key Features:
- Multi-signal scoring combining volume, OI, Greeks, and price movements
- Optimized for 10,000+ ticker scanning with efficient SQL queries
- Focus on patterns suggesting non-public information trading
- Advanced statistical models with adaptive thresholds
- Real-time intraday detection with email alerts
"""

import os
import json
import logging
import smtplib
from typing import Optional, Dict, Any, List, Tuple, Set
from datetime import datetime, timedelta, timezone, date
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from database.connection import db

logger = logging.getLogger(__name__)


class EnhancedAnomalyDetector:
    """Enhanced anomaly detection system with multiple detection algorithms."""
    
    def __init__(self):
        self.alert_min_score = float(os.getenv('ANOMALY_ALERT_MIN_SCORE', '7.0'))
        self.email_enabled = bool(os.getenv('ANOMALY_EMAIL_ENABLED', 'true').lower() == 'true')
        
    def detect_intraday_anomalies(self, 
                                 min_volume_spike_z: float = 4.0,
                                 min_oi_jump_ratio: float = 2.0,
                                 otm_threshold: float = 0.05,
                                 short_term_days: int = 14,
                                 min_baseline_days: int = 30) -> Dict[str, int]:
        """
        Comprehensive intraday anomaly detection focusing on insider trading patterns.
        
        Returns: Dictionary with counts of detected anomalies by type
        """
        now_ts = datetime.now(timezone.utc)
        
        # CRITICAL: Use most recent trading day as event_date
        # This ensures that when daily process runs Friday morning for Thursday's data,
        # anomalies are still associated with Thursday (the trading day they represent)
        event_date = self._get_most_recent_trading_day()
        
        results = {
            'unusual_volume_patterns': 0,
            'suspicious_oi_spikes': 0,
            'coordinated_strikes': 0,
            'pre_move_positioning': 0,
            'low_volume_high_conviction': 0,
            'total_high_score_alerts': 0
        }
        
        high_score_alerts = []
        
        # 1. Unusual Volume Patterns with Greeks Context
        volume_anomalies = self._detect_volume_anomalies_with_greeks(
            event_date, now_ts, min_volume_spike_z, otm_threshold, short_term_days
        )
        results['unusual_volume_patterns'] = len(volume_anomalies)
        high_score_alerts.extend([a for a in volume_anomalies if a['score'] >= self.alert_min_score])
        
        # 2. Suspicious Open Interest Spikes
        oi_anomalies = self._detect_suspicious_oi_spikes(
            event_date, now_ts, min_oi_jump_ratio, short_term_days
        )
        results['suspicious_oi_spikes'] = len(oi_anomalies)
        high_score_alerts.extend([a for a in oi_anomalies if a['score'] >= self.alert_min_score])
        
        # 3. Coordinated Multi-Strike Activity
        coordinated_anomalies = self._detect_coordinated_strikes(
            event_date, now_ts, otm_threshold, short_term_days
        )
        results['coordinated_strikes'] = len(coordinated_anomalies)
        high_score_alerts.extend([a for a in coordinated_anomalies if a['score'] >= self.alert_min_score])
        
        # 4. Pre-Move Positioning (Volume spike before price movement)
        premove_anomalies = self._detect_premove_positioning(event_date, now_ts)
        results['pre_move_positioning'] = len(premove_anomalies)
        high_score_alerts.extend([a for a in premove_anomalies if a['score'] >= self.alert_min_score])
        
        # 5. Low Volume Stocks with High Conviction Bets
        conviction_anomalies = self._detect_low_volume_high_conviction(
            event_date, now_ts, otm_threshold, short_term_days
        )
        results['low_volume_high_conviction'] = len(conviction_anomalies)
        high_score_alerts.extend([a for a in conviction_anomalies if a['score'] >= self.alert_min_score])
        
        # Send consolidated alerts
        results['total_high_score_alerts'] = len(high_score_alerts)
        if high_score_alerts and self.email_enabled:
            try:
                self._send_enhanced_email_alerts(high_score_alerts, now_ts)
                logger.info(f"Sent {len(high_score_alerts)} high-score anomaly alerts")
            except Exception as e:
                logger.error(f"Failed to send email alerts: {e}")
        
        return results
    
    def _detect_volume_anomalies_with_greeks(self, event_date: str, now_ts: datetime, 
                                           min_z: float, otm_threshold: float, 
                                           short_term_days: int) -> List[Dict[str, Any]]:
        """
        Detect unusual volume patterns with Greeks context for better insight.
        Focus on high-delta options with unusual volume (directional bets).
        """
        anomalies = []
        
        # Query for options with unusual volume and favorable Greeks
        sql = """
        WITH current_snapshot AS (
            SELECT 
                symbol,
                contract_ticker,
                session_volume,
                underlying_price,
                strike_price,
                contract_type,
                expiration_date,
                greeks_delta,
                greeks_gamma,
                implied_volatility,
                ABS(underlying_price - strike_price) / underlying_price AS moneyness
            FROM temp_option
            WHERE session_volume IS NOT NULL 
                AND session_volume > 0
                AND expiration_date <= CURRENT_DATE + INTERVAL %s
                AND underlying_price IS NOT NULL 
                AND strike_price IS NOT NULL
                AND greeks_delta IS NOT NULL
        ),
        baseline AS (
            SELECT 
                contract_ticker,
                AVG(volume)::float AS avg_volume,
                STDDEV_POP(volume)::float AS std_volume,
                COUNT(*) AS sample_size
            FROM daily_option_snapshot
            WHERE date >= CURRENT_DATE - INTERVAL '90 days'
                AND date < CURRENT_DATE
            GROUP BY contract_ticker
            HAVING COUNT(*) >= 10
        )
        SELECT 
            c.symbol,
            c.contract_ticker,
            c.session_volume,
            c.underlying_price,
            c.strike_price,
            c.contract_type,
            c.expiration_date,
            c.greeks_delta,
            c.greeks_gamma,
            c.implied_volatility,
            c.moneyness,
            b.avg_volume,
            b.std_volume,
            CASE 
                WHEN b.std_volume > 0 THEN (c.session_volume - b.avg_volume) / b.std_volume
                ELSE NULL 
            END AS z_score
        FROM current_snapshot c
        JOIN baseline b ON c.contract_ticker = b.contract_ticker
        WHERE (c.session_volume - b.avg_volume) / NULLIF(b.std_volume, 0) >= %s
            AND c.session_volume >= GREATEST(100, b.avg_volume * 3)
            AND (
                (c.contract_type = 'call' AND c.moneyness >= %s) OR
                (c.contract_type = 'put' AND c.moneyness >= %s)
            )
        ORDER BY z_score DESC, c.session_volume DESC
        """
        
        rows = db.execute_query(sql, (f'{short_term_days} days', min_z, otm_threshold, otm_threshold))
        
        for row in rows:
            # Calculate enhanced score based on multiple factors
            z_score = float(row['z_score'] or 0)
            delta = abs(float(row['greeks_delta'] or 0))
            gamma = float(row['greeks_gamma'] or 0)
            volume = int(row['session_volume'] or 0)
            
            # Enhanced scoring: Z-score + delta sensitivity + gamma convexity + volume magnitude
            base_score = z_score
            delta_bonus = delta * 2  # High delta = more directional
            gamma_bonus = min(gamma * 100, 2)  # Gamma convexity bonus (capped)
            volume_bonus = min(volume / 1000, 3)  # Volume magnitude bonus (capped)
            
            final_score = base_score + delta_bonus + gamma_bonus + volume_bonus
            
            details = {
                'contract_ticker': row['contract_ticker'],
                'volume': volume,
                'z_score': z_score,
                'delta': delta,
                'gamma': gamma,
                'moneyness': float(row['moneyness'] or 0),
                'iv': float(row['implied_volatility'] or 0),
                'baseline_avg': float(row['avg_volume'] or 0),
                'underlying_price': float(row['underlying_price'] or 0),
                'strike': float(row['strike_price'] or 0),
                'expiry': str(row['expiration_date']),
                'days_to_expiry': (row['expiration_date'] - now_ts.date()).days
            }
            
            self._insert_anomaly_temp(
                event_date, row['symbol'], 'enhanced_volume_with_greeks', 
                final_score, details, 
                direction=row['contract_type'], 
                expiry_date=str(row['expiration_date']),
                as_of_ts=now_ts
            )
            
            anomalies.append({
                'symbol': row['symbol'],
                'score': final_score,
                'type': 'volume_greeks',
                'details': details
            })
        
        return anomalies
    
    def _detect_suspicious_oi_spikes(self, event_date: str, now_ts: datetime,
                                   min_ratio: float, short_term_days: int) -> List[Dict[str, Any]]:
        """
        Detect suspicious open interest spikes that suggest informed positioning.
        """
        anomalies = []
        
        sql = """
        WITH current_oi AS (
            SELECT 
                symbol,
                contract_ticker,
                open_interest,
                session_volume,
                expiration_date,
                contract_type,
                underlying_price,
                strike_price
            FROM temp_option
            WHERE open_interest IS NOT NULL 
                AND open_interest > 0
                AND expiration_date <= CURRENT_DATE + INTERVAL %s
        ),
        previous_oi AS (
            SELECT 
                contract_ticker,
                open_interest AS prev_oi,
                date
            FROM daily_option_snapshot
            WHERE date = (
                SELECT MAX(date) 
                FROM daily_option_snapshot 
                WHERE date < CURRENT_DATE
            )
        )
        SELECT 
            c.symbol,
            c.contract_ticker,
            c.open_interest,
            c.session_volume,
            c.expiration_date,
            c.contract_type,
            c.underlying_price,
            c.strike_price,
            p.prev_oi,
            CASE 
                WHEN p.prev_oi > 0 THEN c.open_interest::float / p.prev_oi::float
                ELSE NULL 
            END AS oi_ratio
        FROM current_oi c
        INNER JOIN previous_oi p ON c.contract_ticker = p.contract_ticker
        WHERE p.prev_oi > 0 
        AND c.open_interest::float / p.prev_oi::float >= %s
        AND c.open_interest >= 500
        ORDER BY oi_ratio DESC NULLS LAST, c.open_interest DESC
        """
        
        rows = db.execute_query(sql, (f'{short_term_days} days', min_ratio))
        
        for row in rows:
            oi_ratio = float(row['oi_ratio'] or 1)
            current_oi = int(row['open_interest'] or 0)
            volume = int(row['session_volume'] or 0)
            
            # Score based on OI jump magnitude and volume confirmation
            base_score = min(oi_ratio, 10)  # Cap at 10x
            volume_confirmation = min(volume / max(current_oi, 1), 2)  # Volume/OI ratio bonus
            new_position_bonus = 3 if row['prev_oi'] is None else 0
            
            final_score = base_score + volume_confirmation + new_position_bonus
            
            details = {
                'contract_ticker': row['contract_ticker'],
                'current_oi': current_oi,
                'previous_oi': int(row['prev_oi'] or 0),
                'oi_ratio': oi_ratio,
                'volume': volume,
                'volume_oi_ratio': volume / max(current_oi, 1),
                'expiry': str(row['expiration_date']),
                'strike': float(row['strike_price'] or 0),
                'underlying': float(row['underlying_price'] or 0)
            }
            
            self._insert_anomaly_temp(
                event_date, row['symbol'], 'suspicious_oi_spike',
                final_score, details,
                direction=row['contract_type'],
                expiry_date=str(row['expiration_date']),
                as_of_ts=now_ts
            )
            
            anomalies.append({
                'symbol': row['symbol'],
                'score': final_score,
                'type': 'oi_spike',
                'details': details
            })
        
        return anomalies
    
    def _detect_coordinated_strikes(self, event_date: str, now_ts: datetime,
                                  otm_threshold: float, short_term_days: int) -> List[Dict[str, Any]]:
        """
        Detect coordinated activity across multiple strikes suggesting ladder strategies.
        """
        anomalies = []
        
        sql = """
        WITH strike_activity AS (
            SELECT 
                symbol,
                contract_type,
                strike_price,
                underlying_price,
                expiration_date,
                SUM(session_volume) AS total_volume,
                COUNT(*) AS contract_count,
                AVG(implied_volatility) AS avg_iv
            FROM temp_option
            WHERE expiration_date <= CURRENT_DATE + INTERVAL %s
                AND session_volume > 100
                AND underlying_price IS NOT NULL
                AND strike_price IS NOT NULL
            GROUP BY symbol, contract_type, strike_price, underlying_price, expiration_date
        ),
        symbol_ladders AS (
            SELECT 
                symbol,
                contract_type,
                underlying_price,
                expiration_date,
                COUNT(DISTINCT strike_price) AS strike_count,
                SUM(total_volume) AS ladder_volume,
                ARRAY_AGG(strike_price ORDER BY strike_price) AS strikes,
                ARRAY_AGG(total_volume ORDER BY strike_price) AS volumes
            FROM strike_activity
            WHERE (
                (contract_type = 'call' AND strike_price >= underlying_price * (1 + %s)) OR
                (contract_type = 'put' AND strike_price <= underlying_price * (1 - %s))
            )
            GROUP BY symbol, contract_type, underlying_price, expiration_date
            HAVING COUNT(DISTINCT strike_price) >= 4
                AND SUM(total_volume) >= 1000
        )
        SELECT *
        FROM symbol_ladders
        ORDER BY ladder_volume DESC, strike_count DESC
        """
        
        rows = db.execute_query(sql, (f'{short_term_days} days', otm_threshold, otm_threshold))
        
        for row in rows:
            strike_count = int(row['strike_count'])
            ladder_volume = int(row['ladder_volume'])
            
            # Score based on strike coordination and volume concentration
            base_score = min(strike_count * 0.8, 8)  # Strike diversity
            volume_score = min(ladder_volume / 1000, 5)  # Volume magnitude
            
            final_score = base_score + volume_score
            
            details = {
                'strike_count': strike_count,
                'total_volume': ladder_volume,
                'strikes': row['strikes'],
                'volumes': row['volumes'],
                'expiry': str(row['expiration_date']),
                'underlying_price': float(row['underlying_price'])
            }
            
            self._insert_anomaly_temp(
                event_date, row['symbol'], 'coordinated_strike_ladder',
                final_score, details,
                direction=row['contract_type'],
                expiry_date=str(row['expiration_date']),
                as_of_ts=now_ts
            )
            
            anomalies.append({
                'symbol': row['symbol'],
                'score': final_score,
                'type': 'coordinated_strikes',
                'details': details
            })
        
        return anomalies
    
    def _detect_premove_positioning(self, event_date: str, now_ts: datetime) -> List[Dict[str, Any]]:
        """
        Detect options positioning that preceded significant stock price movements.
        """
        anomalies = []
        
        sql = """
        WITH stock_moves AS (
            SELECT 
                symbol,
                day_open,
                day_close,
                CASE 
                    WHEN day_open > 0 THEN (day_close - day_open) / day_open
                    ELSE 0 
                END AS pct_change
            FROM temp_stock
            WHERE day_open IS NOT NULL 
                AND day_close IS NOT NULL
                AND day_open > 0
                AND ABS((day_close - day_open) / day_open) >= 0.03  -- 3% minimum move
        ),
        option_activity AS (
            SELECT 
                t.symbol,
                t.contract_type,
                t.strike_price,
                t.underlying_price,
                t.session_volume,
                t.expiration_date,
                CASE 
                    WHEN t.contract_type = 'call' AND t.strike_price > t.underlying_price THEN 'otm_call'
                    WHEN t.contract_type = 'put' AND t.strike_price < t.underlying_price THEN 'otm_put'
                    ELSE 'itm'
                END AS option_position
            FROM temp_option t
            WHERE t.session_volume >= 200
                AND t.expiration_date <= CURRENT_DATE + INTERVAL '30 days'
        )
        SELECT 
            s.symbol,
            s.pct_change,
            o.contract_type,
            o.option_position,
            o.strike_price,
            o.underlying_price,
            o.session_volume,
            o.expiration_date,
            CASE 
                WHEN (s.pct_change > 0 AND o.option_position = 'otm_call') THEN 'profitable_call'
                WHEN (s.pct_change < 0 AND o.option_position = 'otm_put') THEN 'profitable_put'
                ELSE 'neutral'
            END AS positioning_outcome
        FROM stock_moves s
        JOIN option_activity o ON s.symbol = o.symbol
        WHERE (
            (s.pct_change > 0.03 AND o.option_position = 'otm_call') OR
            (s.pct_change < -0.03 AND o.option_position = 'otm_put')
        )
        ORDER BY ABS(s.pct_change) DESC, o.session_volume DESC
        """
        
        rows = db.execute_query(sql)
        
        for row in rows:
            pct_change = float(row['pct_change'])
            volume = int(row['session_volume'])
            
            # Score based on magnitude of move and volume
            move_score = min(abs(pct_change) * 20, 8)  # 20x multiplier, cap at 8
            volume_score = min(volume / 500, 4)  # Volume bonus, cap at 4
            timing_bonus = 2  # Bonus for correct directional timing
            
            final_score = move_score + volume_score + timing_bonus
            
            details = {
                'stock_move_pct': pct_change,
                'volume': volume,
                'positioning': row['positioning_outcome'],
                'strike': float(row['strike_price']),
                'underlying': float(row['underlying_price']),
                'expiry': str(row['expiration_date'])
            }
            
            self._insert_anomaly_temp(
                event_date, row['symbol'], 'premove_positioning',
                final_score, details,
                direction=row['contract_type'],
                expiry_date=str(row['expiration_date']),
                as_of_ts=now_ts
            )
            
            anomalies.append({
                'symbol': row['symbol'],
                'score': final_score,
                'type': 'premove_positioning',
                'details': details
            })
        
        return anomalies
    
    def _detect_low_volume_high_conviction(self, event_date: str, now_ts: datetime,
                                         otm_threshold: float, short_term_days: int) -> List[Dict[str, Any]]:
        """
        Detect high-conviction bets on low-volume stocks (potential insider activity).
        """
        anomalies = []
        
        sql = """
        WITH low_volume_stocks AS (
            SELECT symbol, day_volume
            FROM temp_stock
            WHERE day_volume IS NOT NULL 
                AND day_volume > 0
                AND day_volume < 1000000  -- Less than 1M shares daily volume
        ),
        unusual_options AS (
            SELECT 
                t.symbol,
                t.contract_ticker,
                t.contract_type,
                t.session_volume,
                t.strike_price,
                t.underlying_price,
                t.expiration_date,
                b.avg_volume,
                CASE 
                    WHEN b.avg_volume > 0 THEN t.session_volume / b.avg_volume
                    ELSE NULL 
                END AS volume_ratio
            FROM temp_option t
            JOIN (
                SELECT 
                    contract_ticker,
                    AVG(volume) AS avg_volume
                FROM daily_option_snapshot
                WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                    AND date < CURRENT_DATE
                GROUP BY contract_ticker
                HAVING AVG(volume) > 0
            ) b ON t.contract_ticker = b.contract_ticker
            WHERE t.session_volume >= 100
                AND t.expiration_date <= CURRENT_DATE + INTERVAL %s
        )
        SELECT 
            l.symbol,
            l.day_volume AS stock_volume,
            u.contract_ticker,
            u.contract_type,
            u.session_volume,
            u.strike_price,
            u.underlying_price,
            u.expiration_date,
            u.volume_ratio
        FROM low_volume_stocks l
        JOIN unusual_options u ON l.symbol = u.symbol
        WHERE u.volume_ratio >= 5  -- 5x normal volume
            AND (
                (u.contract_type = 'call' AND u.strike_price >= u.underlying_price * (1 + %s)) OR
                (u.contract_type = 'put' AND u.strike_price <= u.underlying_price * (1 - %s))
            )
        ORDER BY u.volume_ratio DESC, u.session_volume DESC
        """
        
        rows = db.execute_query(sql, (f'{short_term_days} days', otm_threshold, otm_threshold))
        
        for row in rows:
            volume_ratio = float(row['volume_ratio'] or 1)
            option_volume = int(row['session_volume'])
            stock_volume = int(row['stock_volume'])
            
            # Higher score for low-volume stocks with unusual option activity
            base_score = min(volume_ratio * 0.8, 8)
            low_volume_bonus = max(3 - (stock_volume / 500000), 0)  # Bonus for very low stock volume
            concentration_bonus = min(option_volume / (stock_volume / 100), 2)  # Option/Stock volume ratio
            
            final_score = base_score + low_volume_bonus + concentration_bonus
            
            details = {
                'contract_ticker': row['contract_ticker'],
                'option_volume': option_volume,
                'volume_ratio': volume_ratio,
                'stock_volume': stock_volume,
                'strike': float(row['strike_price']),
                'underlying': float(row['underlying_price']),
                'expiry': str(row['expiration_date']),
                'option_stock_ratio': option_volume / max(stock_volume / 100, 1)
            }
            
            self._insert_anomaly_temp(
                event_date, row['symbol'], 'low_volume_high_conviction',
                final_score, details,
                direction=row['contract_type'],
                expiry_date=str(row['expiration_date']),
                as_of_ts=now_ts
            )
            
            anomalies.append({
                'symbol': row['symbol'],
                'score': final_score,
                'type': 'low_volume_conviction',
                'details': details
            })
        
        return anomalies
    
    def _insert_anomaly_temp(self, event_date: str, symbol: str, kind: str, score: float,
                           details: Dict[str, Any], direction: Optional[str] = None,
                           expiry_date: Optional[str] = None, as_of_ts: Optional[datetime] = None) -> None:
        """Insert anomaly into temp table with upsert logic."""
        sql = """
        INSERT INTO temp_anomaly (event_date, symbol, direction, expiry_date, as_of_timestamp, kind, score, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (event_date, symbol, direction, expiry_date, kind)
        DO UPDATE SET 
            score = EXCLUDED.score, 
            details = EXCLUDED.details, 
            updated_at = CURRENT_TIMESTAMP,
            as_of_timestamp = EXCLUDED.as_of_timestamp
        """
        db.execute_command(sql, (
            event_date, symbol, direction, expiry_date, as_of_ts, 
            kind, float(score), json.dumps(details)
        ))
    
    def _get_most_recent_trading_day(self) -> str:
        """
        Get the most recent trading day (business day, excluding weekends).
        
        Returns:
            Date string in YYYY-MM-DD format representing the most recent trading day
        """
        current_date = date.today()
        
        # Go back up to 7 days to find the most recent trading day (Mon-Fri)
        for i in range(7):
            check_date = current_date - timedelta(days=i)
            # Monday=0, Tuesday=1, ..., Sunday=6
            # Trading days are Monday(0) through Friday(4)
            if check_date.weekday() < 5:  # 0-4 are Mon-Fri
                return check_date.isoformat()
        
        # Fallback to 5 days ago if somehow no trading day found
        fallback_date = current_date - timedelta(days=5)
        return fallback_date.isoformat()
    
    def _send_enhanced_email_alerts(self, alerts: List[Dict[str, Any]], timestamp: datetime) -> None:
        """Send enhanced email alerts with detailed analysis."""
        if not self._validate_email_config():
            logger.warning("Email configuration incomplete; skipping alerts")
            return
        
        # Group alerts by symbol for better organization
        by_symbol = {}
        for alert in alerts:
            symbol = alert['symbol']
            if symbol not in by_symbol:
                by_symbol[symbol] = []
            by_symbol[symbol].append(alert)
        
        # Create enhanced email content
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"🚨 Enhanced Insider Trading Alerts - {timestamp.strftime('%Y-%m-%d %H:%M UTC')} ({len(alerts)} signals)"
        msg['From'] = os.getenv('ALERT_EMAIL_FROM')
        msg['To'] = os.getenv('ALERT_EMAIL_TO')
        
        # Create both text and HTML versions
        text_content = self._create_text_alert_content(by_symbol, timestamp)
        html_content = self._create_html_alert_content(by_symbol, timestamp)
        
        msg.attach(MIMEText(text_content, 'plain'))
        msg.attach(MIMEText(html_content, 'html'))
        
        # Send email
        self._send_email(msg)
    
    def _create_text_alert_content(self, by_symbol: Dict[str, List[Dict]], timestamp: datetime) -> str:
        """Create text version of alert email."""
        lines = [
            f"Enhanced Insider Trading Alerts - {timestamp.strftime('%Y-%m-%d %H:%M UTC')}",
            "=" * 60,
            ""
        ]
        
        for symbol, symbol_alerts in sorted(by_symbol.items()):
            lines.append(f"SYMBOL: {symbol}")
            lines.append("-" * 20)
            
            max_score = max(a['score'] for a in symbol_alerts)
            lines.append(f"Maximum Score: {max_score:.2f}")
            lines.append(f"Alert Count: {len(symbol_alerts)}")
            lines.append("")
            
            for alert in sorted(symbol_alerts, key=lambda x: x['score'], reverse=True):
                details = alert['details']
                lines.append(f"  • {alert['type'].upper()}: Score {alert['score']:.2f}")
                
                if alert['type'] == 'volume_greeks':
                    lines.append(f"    Contract: {details.get('contract_ticker', 'N/A')}")
                    lines.append(f"    Volume: {details.get('volume', 0):,} (Z-score: {details.get('z_score', 0):.1f})")
                    lines.append(f"    Delta: {details.get('delta', 0):.3f}, Gamma: {details.get('gamma', 0):.6f}")
                    
                elif alert['type'] == 'oi_spike':
                    lines.append(f"    Contract: {details.get('contract_ticker', 'N/A')}")
                    lines.append(f"    OI Jump: {details.get('previous_oi', 0):,} → {details.get('current_oi', 0):,} ({details.get('oi_ratio', 1):.1f}x)")
                    
                elif alert['type'] == 'premove_positioning':
                    lines.append(f"    Stock Move: {details.get('stock_move_pct', 0)*100:+.1f}%")
                    lines.append(f"    Option Volume: {details.get('volume', 0):,}")
                    
                lines.append("")
            
            lines.append("")
        
        return "\n".join(lines)
    
    def _create_html_alert_content(self, by_symbol: Dict[str, List[Dict]], timestamp: datetime) -> str:
        """Create HTML version of alert email."""
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #ff4444; color: white; padding: 15px; border-radius: 5px; }}
                .symbol-block {{ background-color: #f5f5f5; margin: 15px 0; padding: 15px; border-radius: 5px; }}
                .alert-item {{ margin: 10px 0; padding: 10px; background-color: white; border-left: 4px solid #ff4444; }}
                .score {{ font-weight: bold; color: #cc0000; }}
                .details {{ font-size: 0.9em; color: #666; margin-top: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚨 Enhanced Insider Trading Alerts</h1>
                <p>{timestamp.strftime('%Y-%m-%d %H:%M UTC')} - {sum(len(alerts) for alerts in by_symbol.values())} total signals</p>
            </div>
        """
        
        for symbol, symbol_alerts in sorted(by_symbol.items()):
            max_score = max(a['score'] for a in symbol_alerts)
            html += f"""
            <div class="symbol-block">
                <h2>{symbol} <span class="score">(Max Score: {max_score:.2f})</span></h2>
                <p><strong>{len(symbol_alerts)} alerts detected</strong></p>
            """
            
            for alert in sorted(symbol_alerts, key=lambda x: x['score'], reverse=True):
                details = alert['details']
                html += f"""
                <div class="alert-item">
                    <h3>{alert['type'].replace('_', ' ').title()} <span class="score">Score: {alert['score']:.2f}</span></h3>
                """
                
                if alert['type'] == 'volume_greeks':
                    html += f"""
                    <div class="details">
                        <strong>Contract:</strong> {details.get('contract_ticker', 'N/A')}<br>
                        <strong>Volume:</strong> {details.get('volume', 0):,} (Z-score: {details.get('z_score', 0):.1f})<br>
                        <strong>Greeks:</strong> Δ={details.get('delta', 0):.3f}, Γ={details.get('gamma', 0):.6f}<br>
                        <strong>Strike/Underlying:</strong> ${details.get('strike', 0):.2f} / ${details.get('underlying_price', 0):.2f}<br>
                        <strong>Expiry:</strong> {details.get('expiry', 'N/A')} ({details.get('days_to_expiry', 0)} days)
                    </div>
                    """
                
                elif alert['type'] == 'oi_spike':
                    html += f"""
                    <div class="details">
                        <strong>Contract:</strong> {details.get('contract_ticker', 'N/A')}<br>
                        <strong>OI Change:</strong> {details.get('previous_oi', 0):,} → {details.get('current_oi', 0):,} ({details.get('oi_ratio', 1):.1f}x)<br>
                        <strong>Volume:</strong> {details.get('volume', 0):,}<br>
                        <strong>Strike/Underlying:</strong> ${details.get('strike', 0):.2f} / ${details.get('underlying', 0):.2f}
                    </div>
                    """
                
                elif alert['type'] == 'premove_positioning':
                    html += f"""
                    <div class="details">
                        <strong>Stock Movement:</strong> {details.get('stock_move_pct', 0)*100:+.1f}%<br>
                        <strong>Option Volume:</strong> {details.get('volume', 0):,}<br>
                        <strong>Positioning:</strong> {details.get('positioning', 'N/A')}<br>
                        <strong>Strike/Underlying:</strong> ${details.get('strike', 0):.2f} / ${details.get('underlying', 0):.2f}
                    </div>
                    """
                
                html += "</div>"
            
            html += "</div>"
        
        html += "</body></html>"
        return html
    
    def _validate_email_config(self) -> bool:
        """Validate email configuration."""
        required_vars = ['SMTP_HOST', 'ALERT_EMAIL_FROM', 'ALERT_EMAIL_TO']
        return all(os.getenv(var) for var in required_vars)
    
    def _send_email(self, msg: MIMEMultipart) -> None:
        """Send email using configured SMTP settings."""
        host = os.getenv('SMTP_HOST')
        port = int(os.getenv('SMTP_PORT', '587'))
        user = os.getenv('SMTP_USER')
        password = os.getenv('SMTP_PASS') or os.getenv('SMTP_PASSWORD')
        use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
        
        server = smtplib.SMTP(host, port)
        if use_tls:
            server.starttls()
        if user and password:
            server.login(user, password)
        
        server.send_message(msg)
        server.quit()


# Convenience functions for backward compatibility and integration
def run_enhanced_intraday_detection(**kwargs) -> Dict[str, int]:
    """Run enhanced intraday anomaly detection."""
    detector = EnhancedAnomalyDetector()
    return detector.detect_intraday_anomalies(**kwargs)
