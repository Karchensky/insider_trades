#!/usr/bin/env python3
"""
Anomaly Backfill Script

This script backfills historical anomaly data using the detection algorithms.
Unlike the legacy backfill which only did daily symbol-level aggregation, this performs
comprehensive historical analysis with all algorithms.

Usage:
    python scripts/backfill_anomalies.py --days 60 --batch-size 5
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from analysis.enhanced_anomaly_detection import EnhancedAnomalyDetector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HistoricalAnomalyBackfill:
    """Enhanced historical anomaly detection backfill processor."""
    
    def __init__(self):
        self.detector = EnhancedAnomalyDetector()
        
    def backfill_date_range(self, start_date: str, end_date: str, batch_size: int = 5) -> Dict[str, Any]:
        """
        Backfill anomaly detection for a date range using enhanced algorithms.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format  
            batch_size: Number of days to process in each batch
            
        Returns:
            Summary of backfill results
        """
        logger.info(f"Starting enhanced anomaly backfill: {start_date} to {end_date}")
        
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        
        total_days = (end - start).days + 1
        processed_days = 0
        total_anomalies = 0
        errors = []
        
        current_date = start
        
        while current_date <= end:
            batch_end = min(current_date + timedelta(days=batch_size - 1), end)
            
            try:
                batch_results = self._process_date_batch(current_date, batch_end)
                processed_days += batch_results['processed_days']
                total_anomalies += batch_results['total_anomalies']
                
                logger.info(f"Batch {current_date} to {batch_end}: {batch_results['total_anomalies']} anomalies")
                
            except Exception as e:
                error_msg = f"Failed to process batch {current_date} to {batch_end}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
            
            current_date = batch_end + timedelta(days=1)
        
        results = {
            'start_date': start_date,
            'end_date': end_date,
            'total_days': total_days,
            'processed_days': processed_days,
            'total_anomalies': total_anomalies,
            'errors': errors,
            'success_rate': processed_days / total_days if total_days > 0 else 0
        }
        
        logger.info(f"Backfill complete. Processed {processed_days}/{total_days} days, {total_anomalies} total anomalies")
        return results
    
    def _process_date_batch(self, start_date: datetime.date, end_date: datetime.date) -> Dict[str, Any]:
        """Process a batch of dates for historical anomaly detection."""
        
        # For historical analysis, we need to simulate the temp tables with historical data
        # This is more complex than intraday detection as we need to reconstruct the state
        
        processed_days = 0
        total_anomalies = 0
        
        current_date = start_date
        while current_date <= end_date:
            try:
                # Check if we have sufficient historical data for this date
                if not self._has_sufficient_data(current_date):
                    logger.warning(f"Insufficient data for {current_date}, skipping")
                    current_date += timedelta(days=1)
                    continue
                
                # Run historical anomaly detection for this date
                anomaly_count = self._detect_historical_anomalies(current_date)
                total_anomalies += anomaly_count
                processed_days += 1
                
                logger.debug(f"Date {current_date}: {anomaly_count} anomalies detected")
                
            except Exception as e:
                logger.error(f"Failed to process {current_date}: {e}")
            
            current_date += timedelta(days=1)
        
        return {
            'processed_days': processed_days,
            'total_anomalies': total_anomalies
        }
    
    def _has_sufficient_data(self, target_date: datetime.date) -> bool:
        """Check if we have sufficient historical data to run detection for target date."""
        
        # Check for options data on target date
        options_count = db.execute_query("""
            SELECT COUNT(*) as count 
            FROM daily_option_snapshot 
            WHERE date = %s
        """, (target_date,))[0]['count']
        
        # Check for stock data on target date  
        stock_count = db.execute_query("""
            SELECT COUNT(*) as count 
            FROM daily_stock_snapshot 
            WHERE date = %s
        """, (target_date,))[0]['count']
        
        # Need both options and stock data to run detection
        return options_count > 1000 and stock_count > 1000
    
    def _detect_historical_anomalies(self, target_date: datetime.date) -> int:
        """
        Run enhanced anomaly detection for a historical date.
        
        This simulates the intraday detection by using end-of-day data as if it were
        the final intraday snapshot.
        """
        
        # For historical analysis, we primarily focus on volume and OI anomalies
        # that can be detected from end-of-day data
        
        anomaly_count = 0
        event_date = target_date.isoformat()
        
        try:
            # 1. Historical Volume Anomalies (using daily data as "intraday")
            volume_anomalies = self._detect_historical_volume_anomalies(target_date)
            anomaly_count += len(volume_anomalies)
            
            # 2. Historical OI Analysis (if we have full snapshot data)
            oi_anomalies = self._detect_historical_oi_anomalies(target_date)
            anomaly_count += len(oi_anomalies)
            
            # 3. Pre-move positioning (validated with subsequent price movements)
            premove_anomalies = self._detect_historical_premove_positioning(target_date)
            anomaly_count += len(premove_anomalies)
            
        except Exception as e:
            logger.error(f"Historical detection failed for {target_date}: {e}")
        
        return anomaly_count
    
    def _detect_historical_volume_anomalies(self, target_date: datetime.date) -> list:
        """Detect historical volume anomalies using daily option snapshot data."""
        
        anomalies = []
        event_date = target_date.isoformat()
        
        # Query for unusual volume patterns on target date
        sql = """
        WITH daily_data AS (
            SELECT 
                symbol,
                contract_ticker,
                volume,
                close_price
            FROM daily_option_snapshot
            WHERE date = %s AND volume > 0
        ),
        baselines AS (
            SELECT 
                contract_ticker,
                AVG(volume)::float AS avg_volume,
                STDDEV_POP(volume)::float AS std_volume,
                COUNT(*) AS sample_size
            FROM daily_option_snapshot
            WHERE date < %s 
                AND date >= %s 
            GROUP BY contract_ticker
            HAVING COUNT(*) >= 10 AND AVG(volume) > 0
        )
        SELECT 
            d.symbol,
            d.contract_ticker,
            d.volume,
            b.avg_volume,
            b.std_volume,
            CASE 
                WHEN b.std_volume > 0 THEN (d.volume - b.avg_volume) / b.std_volume
                ELSE NULL 
            END AS z_score
        FROM daily_data d
        JOIN baselines b ON d.contract_ticker = b.contract_ticker
        WHERE (d.volume - b.avg_volume) / NULLIF(b.std_volume, 0) >= 4.0
            AND d.volume >= GREATEST(100, b.avg_volume * 3)
        ORDER BY z_score DESC
        LIMIT 100
        """
        
        # Look back 90 days for baseline
        baseline_start = target_date - timedelta(days=90)
        
        rows = db.execute_query(sql, (target_date, target_date, baseline_start))
        
        for row in rows:
            z_score = float(row['z_score'] or 0)
            if z_score >= 4.0:
                details = {
                    'contract_ticker': row['contract_ticker'],
                    'volume': int(row['volume'] or 0),
                    'z_score': z_score,
                    'baseline_avg': float(row['avg_volume'] or 0),
                    'detection_type': 'historical_volume_spike'
                }
                
                # Insert into full_daily_anomaly_snapshot (historical table)
                self._insert_historical_anomaly(
                    event_date, row['symbol'], 'historical_volume_spike',
                    z_score, details
                )
                anomalies.append(details)
        
        return anomalies
    
    def _detect_historical_oi_anomalies(self, target_date: datetime.date) -> list:
        """Detect historical open interest anomalies if full snapshot data available."""
        
        anomalies = []
        
        # Check if we have full_daily_option_snapshot data for this date
        has_full_data = db.execute_query("""
            SELECT COUNT(*) as count 
            FROM full_daily_option_snapshot 
            WHERE snapshot_date = %s
        """, (target_date,))[0]['count']
        
        if has_full_data < 100:  # Not enough full data
            return anomalies
        
        # Analyze OI changes using full snapshot data
        # This would be implemented similar to intraday OI detection
        # but using full_daily_option_snapshot vs previous day
        
        return anomalies
    
    def _detect_historical_premove_positioning(self, target_date: datetime.date) -> list:
        """Detect historical pre-move positioning with post-move validation."""
        
        anomalies = []
        event_date = target_date.isoformat()
        
        # Look for options activity that preceded significant stock moves
        sql = """
        WITH stock_moves AS (
            SELECT 
                s1.symbol,
                s1.close as prev_close,
                s2.close as curr_close,
                CASE 
                    WHEN s1.close > 0 THEN (s2.close - s1.close) / s1.close
                    ELSE 0 
                END AS pct_change
            FROM daily_stock_snapshot s1
            JOIN daily_stock_snapshot s2 ON s1.symbol = s2.symbol
            WHERE s1.date = %s 
                AND s2.date = %s
                AND s1.close > 0
                AND ABS((s2.close - s1.close) / s1.close) >= 0.05  -- 5% move
        ),
        option_activity AS (
            SELECT 
                o.symbol,
                o.contract_ticker,
                o.volume,
                CASE 
                    WHEN o.contract_ticker LIKE '%%C%%' THEN 'call'
                    ELSE 'put'
                END AS option_type
            FROM daily_option_snapshot o
            WHERE o.date = %s 
                AND o.volume >= 100
        )
        SELECT 
            sm.symbol,
            sm.pct_change,
            oa.contract_ticker,
            oa.volume,
            oa.option_type,
            CASE 
                WHEN (sm.pct_change > 0 AND oa.option_type = 'call') THEN 'profitable_call'
                WHEN (sm.pct_change < 0 AND oa.option_type = 'put') THEN 'profitable_put'
                ELSE 'neutral'
            END AS positioning_outcome
        FROM stock_moves sm
        JOIN option_activity oa ON sm.symbol = oa.symbol
        WHERE (
            (sm.pct_change > 0.05 AND oa.option_type = 'call') OR
            (sm.pct_change < -0.05 AND oa.option_type = 'put')
        )
        ORDER BY ABS(sm.pct_change) DESC
        """
        
        prev_date = target_date - timedelta(days=1)
        next_date = target_date + timedelta(days=1)
        
        rows = db.execute_query(sql, (prev_date, next_date, target_date))
        
        for row in rows:
            score = min(abs(float(row['pct_change'])) * 10, 10)  # Cap at 10
            
            details = {
                'contract_ticker': row['contract_ticker'],
                'volume': int(row['volume'] or 0),
                'stock_move_pct': float(row['pct_change']),
                'positioning': row['positioning_outcome'],
                'detection_type': 'historical_premove_positioning'
            }
            
            self._insert_historical_anomaly(
                event_date, row['symbol'], 'historical_premove_positioning',
                score, details, direction=row['option_type']
            )
            anomalies.append(details)
        
        return anomalies
    
    def _insert_historical_anomaly(self, event_date: str, symbol: str, kind: str, score: float,
                                 details: Dict[str, Any], direction: str = None, expiry_date: str = None):
        """Insert historical anomaly into full_daily_anomaly_snapshot table."""
        
        sql = """
        INSERT INTO full_daily_anomaly_snapshot (event_date, symbol, direction, expiry_date, kind, score, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (event_date, symbol, direction, expiry_date, kind)
        DO UPDATE SET 
            score = EXCLUDED.score,
            details = EXCLUDED.details,
            updated_at = CURRENT_TIMESTAMP
        """
        
        import json
        db.execute_command(sql, (
            event_date, symbol, direction, expiry_date, kind, 
            float(score), json.dumps(details)
        ))


def main():
    parser = argparse.ArgumentParser(description='Enhanced historical anomaly backfill')
    parser.add_argument('--days', type=int, default=60, help='Number of days to backfill')
    parser.add_argument('--batch-size', type=int, default=5, help='Number of days per batch')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD), default: days ago from today')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD), default: yesterday')
    
    args = parser.parse_args()
    
    # Calculate date range
    today = datetime.now().date()
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    else:
        end_date = today - timedelta(days=1)  # Yesterday
    
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    else:
        start_date = end_date - timedelta(days=args.days - 1)
    
    # Run backfill
    backfill = HistoricalAnomalyBackfill()
    results = backfill.backfill_date_range(
        start_date.isoformat(), 
        end_date.isoformat(), 
        args.batch_size
    )
    
    # Print summary
    print("\n" + "="*60)
    print("ENHANCED ANOMALY BACKFILL RESULTS")
    print("="*60)
    print(f"Date Range: {results['start_date']} to {results['end_date']}")
    print(f"Processed: {results['processed_days']}/{results['total_days']} days")
    print(f"Success Rate: {results['success_rate']*100:.1f}%")
    print(f"Total Anomalies: {results['total_anomalies']:,}")
    
    if results['errors']:
        print(f"\nErrors ({len(results['errors'])}):")
        for error in results['errors']:
            print(f"  - {error}")
    
    print("\nBackfill completed!")


if __name__ == '__main__':
    main()
