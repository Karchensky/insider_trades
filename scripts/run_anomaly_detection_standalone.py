"""
Standalone Anomaly Detection Runner and Results Analyzer

This script runs the complete anomaly detection process against current data
and creates detailed results tables for verification and accuracy checking.

Usage:
    python scripts/run_anomaly_detection_standalone.py [--baseline-days 30] [--output results.json]
"""

import os
import sys
import logging
import argparse
import json
import time
from datetime import datetime, date
from typing import Dict, Any, List

# Add the parent directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from analysis.insider_anomaly_detection import run_insider_anomaly_detection

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('anomaly_detection_test.log')
    ]
)
logger = logging.getLogger(__name__)


def get_database_stats() -> Dict[str, Any]:
    """Get basic statistics about the database state."""
    logger.info("Gathering database statistics...")
    
    conn = db.connect()
    stats = {}
    
    try:
        with conn.cursor() as cur:
            # temp_option statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_contracts,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(DISTINCT contract_type) as contract_types,
                    MIN(DATE(as_of_timestamp)) as earliest_date,
                    MAX(DATE(as_of_timestamp)) as latest_date,
                    COUNT(DISTINCT DATE(as_of_timestamp)) as trading_days,
                    SUM(CASE WHEN session_volume > 0 THEN 1 ELSE 0 END) as contracts_with_volume,
                    AVG(session_volume) as avg_volume,
                    MAX(session_volume) as max_volume
                FROM temp_option
            """)
            temp_option_stats = cur.fetchone()
            
            if hasattr(temp_option_stats, 'keys'):
                stats['temp_option'] = dict(temp_option_stats)
            else:
                columns = ['total_contracts', 'unique_symbols', 'contract_types', 'earliest_date', 
                          'latest_date', 'trading_days', 'contracts_with_volume', 'avg_volume', 'max_volume']
                stats['temp_option'] = dict(zip(columns, temp_option_stats))
            
            # temp_stock statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_stocks,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    MIN(DATE(as_of_timestamp)) as earliest_date,
                    MAX(DATE(as_of_timestamp)) as latest_date,
                    COUNT(DISTINCT DATE(as_of_timestamp)) as trading_days,
                    AVG(day_volume) as avg_volume,
                    MAX(day_volume) as max_volume
                FROM temp_stock
            """)
            temp_stock_stats = cur.fetchone()
            
            if hasattr(temp_stock_stats, 'keys'):
                stats['temp_stock'] = dict(temp_stock_stats)
            else:
                columns = ['total_stocks', 'unique_symbols', 'earliest_date', 
                          'latest_date', 'trading_days', 'avg_volume', 'max_volume']
                stats['temp_stock'] = dict(zip(columns, temp_stock_stats))
            
            # temp_anomaly current state
            cur.execute("""
                SELECT 
                    COUNT(*) as total_anomalies,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(DISTINCT event_date) as anomaly_dates,
                    AVG(score) as avg_score,
                    MAX(score) as max_score,
                    MIN(score) as min_score
                FROM temp_anomaly
            """)
            temp_anomaly_stats = cur.fetchone()
            
            if hasattr(temp_anomaly_stats, 'keys'):
                stats['temp_anomaly'] = dict(temp_anomaly_stats)
            else:
                columns = ['total_anomalies', 'unique_symbols', 'anomaly_dates', 
                          'avg_score', 'max_score', 'min_score']
                stats['temp_anomaly'] = dict(zip(columns, temp_anomaly_stats))
            
    finally:
        conn.close()
    
    return stats


def get_sample_data() -> Dict[str, List[Dict]]:
    """Get sample data for verification."""
    logger.info("Getting sample data...")
    
    conn = db.connect()
    samples = {}
    
    try:
        with conn.cursor() as cur:
            # Sample high-volume option contracts
            cur.execute("""
                SELECT 
                    symbol, contract_ticker, contract_type, strike_price, 
                    expiration_date, session_volume, session_close,
                    implied_volatility, open_interest,
                    COALESCE(underlying_price, 0) as underlying_price,
                    (expiration_date - CURRENT_DATE) as days_to_exp
                FROM temp_option o
                WHERE session_volume > 100
                  AND DATE(as_of_timestamp) = CURRENT_DATE
                ORDER BY session_volume DESC
                LIMIT 20
            """)
            high_volume_options = cur.fetchall()
            
            if high_volume_options:
                if hasattr(high_volume_options[0], 'keys'):
                    samples['high_volume_options'] = [dict(row) for row in high_volume_options]
                else:
                    columns = ['symbol', 'contract_ticker', 'contract_type', 'strike_price', 
                              'expiration_date', 'session_volume', 'session_close',
                              'implied_volatility', 'open_interest', 'underlying_price', 'days_to_exp']
                    samples['high_volume_options'] = [dict(zip(columns, row)) for row in high_volume_options]
            else:
                samples['high_volume_options'] = []
            
            # Sample OTM calls with short expirations
            cur.execute("""
                SELECT 
                    o.symbol, o.contract_ticker, o.strike_price, 
                    o.expiration_date, o.session_volume,
                    COALESCE(o.underlying_price, s.day_close, s.day_vwap) as underlying_price,
                    (o.expiration_date - CURRENT_DATE) as days_to_exp,
                    CASE 
                        WHEN o.contract_type = 'call' AND COALESCE(o.underlying_price, s.day_close, s.day_vwap) < o.strike_price THEN true
                        WHEN o.contract_type = 'put' AND COALESCE(o.underlying_price, s.day_close, s.day_vwap) > o.strike_price THEN true
                        ELSE false
                    END as is_otm
                FROM temp_option o
                LEFT JOIN temp_stock s ON o.symbol = s.symbol 
                    AND DATE(o.as_of_timestamp) = DATE(s.as_of_timestamp)
                WHERE o.contract_type = 'call'
                  AND DATE(o.as_of_timestamp) = CURRENT_DATE
                  AND (o.expiration_date - CURRENT_DATE) <= 21
                  AND o.session_volume > 10
                  AND COALESCE(o.underlying_price, s.day_close, s.day_vwap) < o.strike_price
                ORDER BY o.session_volume DESC
                LIMIT 20
            """)
            otm_calls = cur.fetchall()
            
            if otm_calls:
                if hasattr(otm_calls[0], 'keys'):
                    samples['otm_short_calls'] = [dict(row) for row in otm_calls]
                else:
                    columns = ['symbol', 'contract_ticker', 'strike_price', 'expiration_date', 
                              'session_volume', 'underlying_price', 'days_to_exp', 'is_otm']
                    samples['otm_short_calls'] = [dict(zip(columns, row)) for row in otm_calls]
            else:
                samples['otm_short_calls'] = []
            
    finally:
        conn.close()
    
    return samples


def run_detection_and_analyze(baseline_days: int = 30) -> Dict[str, Any]:
    """Run the anomaly detection and analyze results."""
    logger.info("=" * 60)
    logger.info("RUNNING ANOMALY DETECTION ANALYSIS")
    logger.info("=" * 60)
    
    # Get pre-detection database stats
    logger.info("Phase 1: Pre-detection database analysis")
    pre_stats = get_database_stats()
    
    # Get sample data for analysis
    logger.info("Phase 2: Sample data collection")
    samples = get_sample_data()
    
    # Run the anomaly detection
    logger.info("Phase 3: Running anomaly detection algorithm")
    detection_start = time.time()
    detection_results = run_insider_anomaly_detection(baseline_days=baseline_days)
    detection_time = time.time() - detection_start
    
    # Get post-detection database stats
    logger.info("Phase 4: Post-detection database analysis")
    post_stats = get_database_stats()
    
    # Analyze the results
    logger.info("Phase 5: Results analysis")
    analysis_results = analyze_detection_results()
    
    # Compile comprehensive results
    results = {
        'timestamp': datetime.now().isoformat(),
        'detection_time': detection_time,
        'baseline_days': baseline_days,
        'pre_detection_stats': pre_stats,
        'post_detection_stats': post_stats,
        'sample_data': samples,
        'detection_results': detection_results,
        'analysis': analysis_results
    }
    
    return results


def analyze_detection_results() -> Dict[str, Any]:
    """Analyze the anomaly detection results in detail."""
    logger.info("Analyzing detection results...")
    
    conn = db.connect()
    analysis = {}
    
    try:
        with conn.cursor() as cur:
            # Get all detected anomalies
            cur.execute("""
                SELECT 
                    symbol, direction, score, anomaly_types,
                    total_individual_anomalies, max_individual_score,
                    details, as_of_timestamp
                FROM temp_anomaly
                WHERE event_date = CURRENT_DATE
                ORDER BY score DESC
            """)
            anomalies = cur.fetchall()
            
            if anomalies:
                if hasattr(anomalies[0], 'keys'):
                    analysis['detected_anomalies'] = [dict(row) for row in anomalies]
                else:
                    columns = ['symbol', 'direction', 'score', 'anomaly_types',
                              'total_individual_anomalies', 'max_individual_score',
                              'details', 'as_of_timestamp']
                    analysis['detected_anomalies'] = [dict(zip(columns, row)) for row in anomalies]
            else:
                analysis['detected_anomalies'] = []
            
            # Anomaly type breakdown
            cur.execute("""
                SELECT 
                    unnest(anomaly_types) as anomaly_type,
                    COUNT(*) as count,
                    AVG(score) as avg_score,
                    MAX(score) as max_score
                FROM temp_anomaly
                WHERE event_date = CURRENT_DATE
                GROUP BY unnest(anomaly_types)
                ORDER BY count DESC
            """)
            type_breakdown = cur.fetchall()
            
            if type_breakdown:
                if hasattr(type_breakdown[0], 'keys'):
                    analysis['anomaly_type_breakdown'] = [dict(row) for row in type_breakdown]
                else:
                    columns = ['anomaly_type', 'count', 'avg_score', 'max_score']
                    analysis['anomaly_type_breakdown'] = [dict(zip(columns, row)) for row in type_breakdown]
            else:
                analysis['anomaly_type_breakdown'] = []
            
            # Score distribution
            cur.execute("""
                SELECT 
                    CASE 
                        WHEN score >= 10 THEN '10+'
                        WHEN score >= 7 THEN '7-10'
                        WHEN score >= 5 THEN '5-7'
                        WHEN score >= 3 THEN '3-5'
                        ELSE '<3'
                    END as score_range,
                    COUNT(*) as count
                FROM temp_anomaly
                WHERE event_date = CURRENT_DATE
                GROUP BY 
                    score_range
                ORDER BY 
                    score_range desc
            """)
            score_dist = cur.fetchall()
            
            if score_dist:
                if hasattr(score_dist[0], 'keys'):
                    analysis['score_distribution'] = [dict(row) for row in score_dist]
                else:
                    columns = ['score_range', 'count']
                    analysis['score_distribution'] = [dict(zip(columns, row)) for row in score_dist]
            else:
                analysis['score_distribution'] = []
    
    finally:
        conn.close()
    
    return analysis


def print_results_summary(results: Dict[str, Any]):
    """Print a formatted summary of the results."""
    print("\n" + "=" * 80)
    print("ANOMALY DETECTION RESULTS SUMMARY")
    print("=" * 80)
    
    # Basic stats
    print(f"Detection Time: {results['detection_time']:.2f} seconds")
    print(f"Baseline Days: {results['baseline_days']}")
    print(f"Timestamp: {results['timestamp']}")
    
    # Detection results
    detection = results['detection_results']
    print(f"\nDETECTION RESULTS:")
    print(f"  Success: {detection.get('success', False)}")
    print(f"  Anomalies Detected: {detection.get('anomalies_detected', 0)}")
    print(f"  Contracts Analyzed: {detection.get('contracts_analyzed', 0)}")
    print(f"  Symbols with Anomalies: {detection.get('symbols_with_anomalies', 0)}")
    
    if not detection.get('success', False):
        print(f"  Error: {detection.get('error', 'Unknown error')}")
        return
    
    # Database stats comparison
    pre = results['pre_detection_stats']['temp_anomaly']
    post = results['post_detection_stats']['temp_anomaly']
    print(f"\nANOMALY TABLE CHANGES:")
    print(f"  Before: {pre.get('total_anomalies', 0)} anomalies")
    print(f"  After: {post.get('total_anomalies', 0)} anomalies")
    print(f"  New Anomalies: {post.get('total_anomalies', 0) - pre.get('total_anomalies', 0)}")
    
    # Analysis results
    analysis = results['analysis']
    anomalies = analysis.get('detected_anomalies', [])
    
    if anomalies:
        print(f"\nTOP 10 ANOMALIES:")
        for i, anomaly in enumerate(anomalies[:10], 1):
            print(f"  {i:2d}. {anomaly['symbol']:6s} - Score: {anomaly['score']:6.2f} - Types: {anomaly['anomaly_types']}")
        
        # Anomaly type breakdown
        type_breakdown = analysis.get('anomaly_type_breakdown', [])
        if type_breakdown:
            print(f"\nANOMALY TYPE BREAKDOWN:")
            for breakdown in type_breakdown:
                print(f"  {breakdown['anomaly_type']:25s}: {breakdown['count']:3d} occurrences (avg score: {breakdown['avg_score']:5.2f})")
        
        # Score distribution
        score_dist = analysis.get('score_distribution', [])
        if score_dist:
            print(f"\nSCORE DISTRIBUTION:")
            for dist in score_dist:
                print(f"  {dist['score_range']:6s}: {dist['count']:3d} anomalies")
    else:
        print("\nNo anomalies detected for today.")
    
    # Sample data info
    samples = results['sample_data']
    print(f"\nSAMPLE DATA:")
    print(f"  High Volume Options: {len(samples.get('high_volume_options', []))}")
    print(f"  OTM Short Calls: {len(samples.get('otm_short_calls', []))}")
    
    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description='Run standalone anomaly detection analysis')
    parser.add_argument('--baseline-days', type=int, default=30, 
                        help='Number of days for baseline calculations (default: 30)')
    parser.add_argument('--output', type=str, help='Output JSON file for detailed results')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Run the comprehensive analysis
        results = run_detection_and_analyze(baseline_days=args.baseline_days)
        
        # Print summary
        print_results_summary(results)
        
        # Save detailed results if requested
        if args.output:
            # Convert datetime objects to strings for JSON serialization
            def json_serializer(obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                elif hasattr(obj, '__dict__'):
                    return obj.__dict__
                else:
                    return str(obj)
            
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=json_serializer)
            print(f"\nDetailed results saved to: {args.output}")
        
        return 0 if results['detection_results'].get('success', False) else 1
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
