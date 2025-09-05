"""
Detailed Anomaly Detection Analysis Script

This script runs the anomaly detection system and creates detailed tables showing:
1. Current day volume vs baseline comparison for each symbol
2. Component scores for each anomaly type
3. Total scores by symbol, ordered by score descending
4. Baseline statistics (days, total volume, standard deviation, etc.)

Usage:
    python scripts/detailed_anomaly_analysis.py [--baseline-days 30] [--output results.csv]
"""

import os
import sys
import logging
import argparse
import pandas as pd
import json
from datetime import datetime, date, timedelta
from typing import Dict, Any, List

# Add the parent directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from analysis.insider_anomaly_detection import run_insider_anomaly_detection

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_current_day_volumes() -> Dict[str, Dict]:
    """Get current day volume statistics by symbol and contract type."""
    logger.info("Getting current day volume statistics...")
    
    conn = db.connect()
    current_volumes = {}
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                WITH latest_temp_option AS (
                    SELECT DISTINCT ON (symbol, contract_ticker) 
                        symbol, contract_ticker, contract_type, session_volume, as_of_timestamp
                    FROM temp_option
                    WHERE session_volume > 0 
                      AND strike_price > 0
                      AND expiration_date > CURRENT_DATE
                    ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                )
                SELECT 
                    symbol,
                    CASE 
                        WHEN contract_type = 'call' THEN 'call'
                        WHEN contract_type = 'put' THEN 'put'
                        ELSE 'unknown'
                    END as contract_type,
                    COUNT(*) as contract_count,
                    SUM(session_volume) as total_volume,
                    AVG(session_volume) as avg_volume_per_contract,
                    MAX(session_volume) as max_volume
                FROM latest_temp_option
                WHERE contract_type IN ('call', 'put')
                GROUP BY symbol, 
                        CASE 
                            WHEN contract_type = 'call' THEN 'call'
                            WHEN contract_type = 'put' THEN 'put'
                            ELSE 'unknown'
                        END
                ORDER BY symbol, contract_type
            """)
            
            rows = cur.fetchall()
            for row in rows:
                if hasattr(row, 'keys'):
                    symbol = row['symbol']
                    contract_type = row['contract_type']
                    if symbol not in current_volumes:
                        current_volumes[symbol] = {}
                    current_volumes[symbol][contract_type] = {
                        'contract_count': row['contract_count'],
                        'total_volume': float(row['total_volume']),
                        'avg_volume_per_contract': float(row['avg_volume_per_contract']),
                        'max_volume': float(row['max_volume'])
                    }
                
    finally:
        conn.close()
    
    return current_volumes


def get_baseline_statistics(baseline_days: int = 30) -> Dict[str, Dict]:
    """Get baseline statistics for comparison."""
    logger.info(f"Getting baseline statistics for {baseline_days} days...")
    
    conn = db.connect()
    baseline_stats = {}
    
    try:
        with conn.cursor() as cur:
            current_date = date.today()
            baseline_start = current_date - timedelta(days=baseline_days)
            actual_end_date = current_date - timedelta(days=1)
            
            cur.execute("""
                WITH daily_volumes AS (
                    SELECT 
                        symbol,
                        CASE 
                            WHEN contract_ticker LIKE '%C%' THEN 'call'
                            WHEN contract_ticker LIKE '%P%' THEN 'put'
                            ELSE 'unknown'
                        END as contract_type,
                        date,
                        SUM(volume) as daily_volume,
                        COUNT(*) as daily_contracts
                    FROM daily_option_snapshot
                    WHERE date BETWEEN %s AND %s
                      AND volume > 0
                    GROUP BY symbol, 
                            CASE 
                                WHEN contract_ticker LIKE '%C%' THEN 'call'
                                WHEN contract_ticker LIKE '%P%' THEN 'put'
                                ELSE 'unknown'
                            END, 
                            date
                )
                SELECT 
                    symbol,
                    contract_type,
                    COUNT(*) as days_count,
                    SUM(daily_volume) as total_volume,
                    SUM(daily_volume) / COUNT(*) as avg_daily_volume,
                    CASE 
                        WHEN COUNT(*) > 1 THEN SQRT(SUM(POWER(daily_volume - (SUM(daily_volume) / COUNT(*)), 2)) / (COUNT(*) - 1))
                        ELSE 0
                    END as stddev_daily_volume,
                    SUM(daily_contracts) / COUNT(*) as avg_daily_contracts,
                    CASE 
                        WHEN COUNT(*) > 1 THEN SQRT(SUM(POWER(daily_contracts - (SUM(daily_contracts) / COUNT(*)), 2)) / (COUNT(*) - 1))
                        ELSE 0
                    END as stddev_daily_contracts,
                    MAX(daily_volume) as max_daily_volume,
                    MIN(daily_volume) as min_daily_volume
                FROM daily_volumes
                WHERE contract_type != 'unknown'
                GROUP BY symbol, contract_type
                HAVING COUNT(*) >= 1
                ORDER BY symbol, contract_type
            """, (baseline_start, actual_end_date))
            
            rows = cur.fetchall()
            for row in rows:
                if hasattr(row, 'keys'):
                    symbol = row['symbol']
                    contract_type = row['contract_type']
                    if symbol not in baseline_stats:
                        baseline_stats[symbol] = {}
                    baseline_stats[symbol][contract_type] = {
                        'days_count': row['days_count'],
                        'total_volume': float(row['total_volume']),
                        'avg_daily_volume': float(row['avg_daily_volume']),
                        'stddev_daily_volume': float(row['stddev_daily_volume']),
                        'avg_daily_contracts': float(row['avg_daily_contracts']),
                        'stddev_daily_contracts': float(row['stddev_daily_contracts']),
                        'max_daily_volume': float(row['max_daily_volume']),
                        'min_daily_volume': float(row['min_daily_volume'])
                    }
                
    finally:
        conn.close()
    
    return baseline_stats


def get_anomaly_details() -> List[Dict]:
    """Get detailed anomaly results from the database."""
    logger.info("Getting anomaly detection results...")
    
    conn = db.connect()
    anomalies = []
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    symbol,
                    direction,
                    score,
                    anomaly_types,
                    total_individual_anomalies,
                    max_individual_score,
                    details,
                    as_of_timestamp
                FROM temp_anomaly
                WHERE event_date = CURRENT_DATE
                ORDER BY score DESC
            """)
            
            rows = cur.fetchall()
            for row in rows:
                if hasattr(row, 'keys'):
                    anomalies.append({
                        'symbol': row['symbol'],
                        'direction': row['direction'],
                        'score': float(row['score']),
                        'anomaly_types': row['anomaly_types'],
                        'total_individual_anomalies': row['total_individual_anomalies'],
                        'max_individual_score': float(row['max_individual_score']),
                        'details': row['details'],
                        'as_of_timestamp': row['as_of_timestamp']
                    })
                
    finally:
        conn.close()
    
    return anomalies


def calculate_volume_anomaly_scores(current_volumes: Dict, baseline_stats: Dict) -> List[Dict]:
    """Calculate volume anomaly scores comparing current vs baseline."""
    volume_comparisons = []
    
    for symbol in current_volumes:
        for contract_type in current_volumes[symbol]:
            current = current_volumes[symbol][contract_type]
            
            # Get baseline for comparison
            baseline_key = f"{symbol}_{contract_type}"
            baseline = None
            if symbol in baseline_stats and contract_type in baseline_stats[symbol]:
                baseline = baseline_stats[symbol][contract_type]
            
            comparison = {
                'symbol': symbol,
                'contract_type': contract_type,
                'current_total_volume': current['total_volume'],
                'current_contract_count': current['contract_count'],
                'current_avg_volume': current['avg_volume_per_contract'],
                'current_max_volume': current['max_volume']
            }
            
            if baseline:
                comparison.update({
                    'baseline_days_count': baseline['days_count'],
                    'baseline_total_volume': baseline['total_volume'],
                    'baseline_avg_daily_volume': baseline['avg_daily_volume'],
                    'baseline_stddev_daily_volume': baseline['stddev_daily_volume'],
                    'baseline_max_daily_volume': baseline['max_daily_volume'],
                    'baseline_min_daily_volume': baseline['min_daily_volume'],
                    'volume_vs_avg_ratio': current['total_volume'] / baseline['avg_daily_volume'] if baseline['avg_daily_volume'] > 0 else 0,
                    'volume_z_score': (current['total_volume'] - baseline['avg_daily_volume']) / baseline['stddev_daily_volume'] if baseline['stddev_daily_volume'] > 0 else 0,
                    'is_volume_anomaly': current['total_volume'] > baseline['avg_daily_volume'] + (2 * baseline['stddev_daily_volume'])
                })
            else:
                comparison.update({
                    'baseline_days_count': 0,
                    'baseline_total_volume': 0,
                    'baseline_avg_daily_volume': 0,
                    'baseline_stddev_daily_volume': 0,
                    'baseline_max_daily_volume': 0,
                    'baseline_min_daily_volume': 0,
                    'volume_vs_avg_ratio': 0,
                    'volume_z_score': 0,
                    'is_volume_anomaly': False,
                    'note': 'No baseline data available'
                })
            
            volume_comparisons.append(comparison)
    
    return volume_comparisons


def create_detailed_analysis_tables(baseline_days: int = 30, output_file: str = None) -> Dict[str, pd.DataFrame]:
    """Create detailed analysis tables."""
    logger.info("Starting detailed anomaly analysis...")
    
    # Run anomaly detection
    logger.info("Running anomaly detection...")
    detection_results = run_insider_anomaly_detection(baseline_days=baseline_days)
    
    if not detection_results.get('success'):
        logger.error(f"Anomaly detection failed: {detection_results.get('error')}")
        return {}
    
    # Get current day volumes
    current_volumes = get_current_day_volumes()
    
    # Get baseline statistics
    baseline_stats = get_baseline_statistics(baseline_days)
    
    # Get anomaly details
    anomalies = get_anomaly_details()
    
    # Calculate volume comparisons
    volume_comparisons = calculate_volume_anomaly_scores(current_volumes, baseline_stats)
    
    # Create DataFrames
    tables = {}
    
    # 1. Volume Comparison Table
    if volume_comparisons:
        tables['volume_comparison'] = pd.DataFrame(volume_comparisons)
        tables['volume_comparison'] = tables['volume_comparison'].sort_values('volume_z_score', ascending=False)
    
    # 2. Anomaly Summary Table
    if anomalies:
        anomaly_summary = []
        for anomaly in anomalies:
            # Break down anomaly types and their individual scores
            anomaly_types = anomaly['anomaly_types']
            details = anomaly.get('details', {})
            
            summary_row = {
                'symbol': anomaly['symbol'],
                'total_score': anomaly['score'],
                'direction': anomaly['direction'],
                'total_individual_anomalies': anomaly['total_individual_anomalies'],
                'max_individual_score': anomaly['max_individual_score'],
                'anomaly_types_count': len(anomaly_types),
                'anomaly_types': ', '.join(anomaly_types)
            }
            
            # Add component scores for each anomaly type
            for anomaly_type in set(anomaly_types):
                count = anomaly_types.count(anomaly_type)
                summary_row[f'{anomaly_type}_count'] = count
                
                # Try to get specific scores from details
                if anomaly_type in details:
                    type_details = details[anomaly_type]
                    if isinstance(type_details, dict) and 'score' in type_details:
                        summary_row[f'{anomaly_type}_score'] = type_details['score']
            
            anomaly_summary.append(summary_row)
        
        tables['anomaly_summary'] = pd.DataFrame(anomaly_summary)
        tables['anomaly_summary'] = tables['anomaly_summary'].sort_values('total_score', ascending=False)
    
    # 3. Anomaly Type Breakdown
    if anomalies:
        type_breakdown = {}
        for anomaly in anomalies:
            for anomaly_type in anomaly['anomaly_types']:
                if anomaly_type not in type_breakdown:
                    type_breakdown[anomaly_type] = {
                        'count': 0,
                        'total_score': 0,
                        'symbols': []
                    }
                type_breakdown[anomaly_type]['count'] += 1
                type_breakdown[anomaly_type]['symbols'].append(anomaly['symbol'])
        
        breakdown_data = []
        for anomaly_type, data in type_breakdown.items():
            breakdown_data.append({
                'anomaly_type': anomaly_type,
                'occurrence_count': data['count'],
                'unique_symbols': len(set(data['symbols'])),
                'avg_occurrences_per_symbol': data['count'] / len(set(data['symbols'])),
                'sample_symbols': ', '.join(list(set(data['symbols']))[:5])
            })
        
        tables['type_breakdown'] = pd.DataFrame(breakdown_data)
        tables['type_breakdown'] = tables['type_breakdown'].sort_values('occurrence_count', ascending=False)
    
    # Save to files if requested
    if output_file:
        base_name = output_file.replace('.csv', '')
        for table_name, df in tables.items():
            filename = f"{base_name}_{table_name}.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Saved {table_name} to {filename}")
    
    return tables


def print_analysis_summary(tables: Dict[str, pd.DataFrame]):
    """Print a summary of the analysis results."""
    print("\n" + "="*80)
    print("DETAILED ANOMALY DETECTION ANALYSIS SUMMARY")
    print("="*80)
    
    if 'volume_comparison' in tables:
        vol_df = tables['volume_comparison']
        print(f"\nVOLUME COMPARISON ANALYSIS:")
        print(f"  Total symbol-contract pairs analyzed: {len(vol_df)}")
        print(f"  Volume anomalies detected: {len(vol_df[vol_df['is_volume_anomaly'] == True])}")
        print(f"  Highest volume Z-score: {vol_df['volume_z_score'].max():.2f}")
        
        print(f"\nTOP 10 VOLUME ANOMALIES:")
        top_vol = vol_df.head(10)
        for _, row in top_vol.iterrows():
            if row.get('is_volume_anomaly', False):
                print(f"  {row['symbol']:6s} {row['contract_type']:4s}: "
                      f"Current={row['current_total_volume']:>8.0f}, "
                      f"Baseline Avg={row['baseline_avg_daily_volume']:>8.0f}, "
                      f"Z-Score={row['volume_z_score']:>6.2f}")
    
    if 'anomaly_summary' in tables:
        anom_df = tables['anomaly_summary']
        print(f"\nANOMALY DETECTION RESULTS:")
        print(f"  Total symbols with anomalies: {len(anom_df)}")
        print(f"  Highest anomaly score: {anom_df['total_score'].max():.2f}")
        print(f"  Average anomaly score: {anom_df['total_score'].mean():.2f}")
        
        print(f"\nTOP 15 ANOMALIES BY SCORE:")
        for _, row in anom_df.head(15).iterrows():
            print(f"  {row['symbol']:6s}: Score={row['total_score']:>8.2f}, "
                  f"Types={row['total_individual_anomalies']:>2d}, "
                  f"Max={row['max_individual_score']:>6.2f}, "
                  f"Dir={row['direction']:>8s}")
    
    if 'type_breakdown' in tables:
        type_df = tables['type_breakdown']
        print(f"\nANOMALY TYPE BREAKDOWN:")
        for _, row in type_df.iterrows():
            print(f"  {row['anomaly_type']:25s}: {row['occurrence_count']:>4d} occurrences, "
                  f"{row['unique_symbols']:>4d} symbols, "
                  f"avg {row['avg_occurrences_per_symbol']:>4.1f} per symbol")
    
    print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(description='Run detailed anomaly detection analysis')
    parser.add_argument('--baseline-days', type=int, default=30, 
                        help='Number of days for baseline calculations (default: 30)')
    parser.add_argument('--output', type=str, help='Base filename for output CSV files (without extension)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Run the comprehensive analysis
        tables = create_detailed_analysis_tables(
            baseline_days=args.baseline_days,
            output_file=args.output
        )
        
        if not tables:
            logger.error("No analysis results generated")
            return 1
        
        # Print summary
        print_analysis_summary(tables)
        
        return 0
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
