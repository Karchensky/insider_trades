#!/usr/bin/env python3
"""
Earnings Calendar Scraper

Fetches upcoming earnings dates using yfinance (free, no API key).
Used to identify pre-earnings volume spikes (speculation vs insider trading).

Simplified version - only stores essential data:
- symbol
- earnings_date
- time_of_day (optional)
"""

import os
import sys
import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


class EarningsCalendarScraper:
    """Fetches earnings dates using yfinance."""
    
    def __init__(self):
        try:
            import yfinance as yf
            self.yf = yf
            self.available = True
        except ImportError:
            logger.warning("yfinance not installed. Run: pip install yfinance")
            self.available = False
    
    def fetch_earnings_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Fetch next earnings date for a single symbol."""
        if not self.available:
            return None
        
        try:
            ticker = self.yf.Ticker(symbol)
            calendar = ticker.calendar
            
            if calendar is None or (isinstance(calendar, dict) and not calendar):
                return None
            
            if isinstance(calendar, dict):
                earnings_dates = calendar.get('Earnings Date', [])
                if not earnings_dates:
                    return None
                
                next_earnings = earnings_dates[0] if earnings_dates else None
                if next_earnings is None:
                    return None
                
                return {
                    'symbol': symbol,
                    'earnings_date': next_earnings if isinstance(next_earnings, date) else next_earnings.date() if hasattr(next_earnings, 'date') else None,
                }
            return None
                
        except Exception as e:
            logger.debug(f"Could not fetch earnings for {symbol}: {e}")
            return None
    
    def fetch_earnings_for_symbols(self, symbols: List[str], max_workers: int = 10) -> List[Dict]:
        """Fetch earnings for multiple symbols in parallel."""
        if not self.available:
            return []
        
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.fetch_earnings_for_symbol, s): s for s in symbols}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result and result.get('earnings_date'):
                        results.append(result)
                except Exception:
                    pass
        
        logger.info(f"Fetched earnings for {len(results)}/{len(symbols)} symbols")
        return results
    
    def store_earnings(self, earnings_data: List[Dict]) -> Dict[str, Any]:
        """Store earnings data in database."""
        if not earnings_data:
            return {'success': True, 'records_stored': 0}
        
        conn = db.connect()
        stored = 0
        
        try:
            with conn.cursor() as cur:
                for record in earnings_data:
                    earnings_date = record.get('earnings_date')
                    if not earnings_date:
                        continue
                    
                    if isinstance(earnings_date, datetime):
                        earnings_date = earnings_date.date()
                    
                    cur.execute("""
                        INSERT INTO earnings_calendar (symbol, earnings_date, date_status)
                        VALUES (%s, %s, 'projected')
                        ON CONFLICT (symbol, earnings_date) 
                        DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                    """, (record['symbol'], earnings_date))
                    stored += 1
                
                conn.commit()
            
            return {'success': True, 'records_stored': stored}
            
        except Exception as e:
            logger.error(f"Failed to store earnings: {e}")
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def fetch_and_store_for_active_symbols(self, max_symbols: int = None) -> Dict[str, Any]:
        """Fetch earnings for symbols with recent anomaly activity."""
        if not self.available:
            return {'success': False, 'error': 'yfinance not available'}
        
        conn = db.connect()
        try:
            import psycopg2.extras
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT DISTINCT symbol 
                FROM daily_anomaly_snapshot 
                WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY symbol
            """)
            symbols = [row['symbol'] for row in cur.fetchall()]
            cur.close()
            
            if max_symbols:
                symbols = symbols[:max_symbols]
            
            logger.info(f"Fetching earnings for {len(symbols)} active symbols")
        finally:
            conn.close()
        
        if not symbols:
            return {'success': True, 'records_stored': 0}
        
        earnings_data = self.fetch_earnings_for_symbols(symbols)
        return self.store_earnings(earnings_data)


def update_intraday_price_flags(days_back: int = 30, threshold_pct: float = 5.0) -> Dict[str, Any]:
    """
    Update is_bot_driven flag based on intraday price movement.
    
    If stock moved >threshold_pct on trigger day, mark as bot-driven.
    """
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH price_moves AS (
                    SELECT 
                        a.id,
                        CASE 
                            WHEN s.open > 0 THEN ((s.close - s.open) / s.open) * 100
                            ELSE 0
                        END as intraday_pct
                    FROM daily_anomaly_snapshot a
                    INNER JOIN daily_stock_snapshot s 
                        ON a.symbol = s.symbol AND a.event_date = s.date
                    WHERE a.event_date >= CURRENT_DATE - INTERVAL %s
                )
                UPDATE daily_anomaly_snapshot a
                SET 
                    intraday_price_move_pct = pm.intraday_pct,
                    is_bot_driven = CASE 
                        WHEN ABS(pm.intraday_pct) >= %s THEN TRUE
                        ELSE FALSE
                    END,
                    updated_at = CURRENT_TIMESTAMP
                FROM price_moves pm
                WHERE a.id = pm.id
            """, (f'{days_back} days', threshold_pct))
            
            updated = cur.rowcount
            conn.commit()
            return {'success': True, 'records_updated': updated}
            
    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


def update_earnings_proximity_flags(days_back: int = 30, earnings_window_days: int = 4) -> Dict[str, Any]:
    """
    Update earnings_proximity_days and is_earnings_related flags.
    
    Args:
        days_back: How many days of anomalies to update
        earnings_window_days: Triggers within this many days of earnings are marked as earnings-related (default: 4)
    """
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Update earnings_proximity_days using the get_earnings_proximity function
            cur.execute("""
                UPDATE daily_anomaly_snapshot
                SET 
                    earnings_proximity_days = get_earnings_proximity(symbol, event_date),
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_date >= CURRENT_DATE - INTERVAL %s
            """, (f'{days_back} days',))
            
            # Update is_earnings_related flag based on proximity
            cur.execute("""
                UPDATE daily_anomaly_snapshot
                SET 
                    is_earnings_related = CASE 
                        WHEN earnings_proximity_days IS NOT NULL 
                             AND earnings_proximity_days >= 0 
                             AND earnings_proximity_days <= %s
                        THEN TRUE
                        ELSE FALSE
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_date >= CURRENT_DATE - INTERVAL %s
            """, (earnings_window_days, f'{days_back} days'))
            
            updated = cur.rowcount
            conn.commit()
            return {'success': True, 'records_updated': updated}
            
    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


def update_actionability_flags(days_back: int = 30) -> Dict[str, Any]:
    """
    Update is_actionable flag.
    
    Actionable = NOT bot_driven AND NOT earnings_related
    """
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE daily_anomaly_snapshot
                SET 
                    is_actionable = CASE 
                        WHEN COALESCE(is_earnings_related, FALSE) = FALSE
                             AND COALESCE(is_bot_driven, FALSE) = FALSE
                        THEN TRUE
                        ELSE FALSE
                    END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE event_date >= CURRENT_DATE - INTERVAL %s
            """, (f'{days_back} days',))
            
            updated = cur.rowcount
            conn.commit()
            return {'success': True, 'records_updated': updated}
            
    except Exception as e:
        conn.rollback()
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


if __name__ == '__main__':
    import argparse
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    parser = argparse.ArgumentParser(description='Earnings Calendar Scraper')
    parser.add_argument('--days-back', type=int, default=30)
    parser.add_argument('--intraday-threshold', type=float, default=5.0)
    parser.add_argument('--earnings-window', type=int, default=4, help='Days before earnings to flag as earnings-related (default: 4)')
    parser.add_argument('--max-symbols', type=int, default=100)
    args = parser.parse_args()
    
    # Update intraday flags
    print("Updating intraday price flags...")
    result1 = update_intraday_price_flags(args.days_back, args.intraday_threshold)
    print(f"  Updated: {result1.get('records_updated', 0)}")
    
    # Update earnings proximity flags
    print(f"Updating earnings proximity flags (window: {args.earnings_window} days)...")
    result_earnings = update_earnings_proximity_flags(args.days_back, args.earnings_window)
    print(f"  Updated: {result_earnings.get('records_updated', 0)}")
    
    # Update actionability
    print("Updating actionability flags...")
    result2 = update_actionability_flags(args.days_back)
    print(f"  Updated: {result2.get('records_updated', 0)}")
    
    # Optionally fetch earnings
    print(f"\nFetching earnings for up to {args.max_symbols} symbols...")
    scraper = EarningsCalendarScraper()
    result3 = scraper.fetch_and_store_for_active_symbols(max_symbols=args.max_symbols)
    print(f"  Stored: {result3.get('records_stored', 0)} earnings records")
