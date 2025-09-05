#!/usr/bin/env python3
"""
Anomaly Results Table Generator
Creates a comprehensive table showing anomaly test scores for each symbol.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from typing import Dict, List, Any
from database.connection import db
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_anomaly_results_table() -> pd.DataFrame:
    """Get comprehensive anomaly results from the database."""
    logger.info("Retrieving anomaly results from database...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Get all anomalies with detailed breakdown
            cur.execute("""
                SELECT 
                    symbol,
                    score,
                    anomaly_types,
                    total_individual_anomalies,
                    max_individual_score,
                    details,
                    event_date,
                    created_at
                FROM temp_anomaly 
                WHERE event_date = CURRENT_DATE
                ORDER BY score DESC
            """)
            
            results = cur.fetchall()
            
            if not results:
                logger.warning("No anomaly results found for today")
                return pd.DataFrame()
            
            # Convert to structured data
            data = []
            for row in results:
                symbol = row[0]
                total_score = float(row[1])
                anomaly_types = row[2] if row[2] else []
                total_anomalies = row[3]
                max_score = float(row[4]) if row[4] else 0.0
                details = row[5] if row[5] else {}
                
                # Parse anomaly types to get individual test scores
                test_scores = {
                    'volume_concentration': 0.0,
                    'strike_coordination': 0.0,
                    'directional_bias': 0.0,
                    'expiration_clustering': 0.0,
                    'volatility_pattern': 0.0,
                    'contract_anomaly': 0.0,
                    'otm_call_insider': 0.0,
                    'greeks_anomaly': 0.0
                }
                
                # Count occurrences of each anomaly type
                for anomaly_type in anomaly_types:
                    if anomaly_type in test_scores:
                        test_scores[anomaly_type] += 1.0
                
                # Extract additional details if available
                call_volume = details.get('volume_concentration', {}).get('call_volume', 0) if details else 0
                put_volume = details.get('volume_concentration', {}).get('put_volume', 0) if details else 0
                total_volume = call_volume + put_volume
                
                call_put_ratio = details.get('directional_bias', {}).get('call_put_ratio', 0) if details else 0
                otm_call_ratio = details.get('otm_call_insider_pattern', {}).get('otm_call_ratio', 0) if details else 0
                
                data.append({
                    'symbol': symbol,
                    'composite_score': total_score,
                    'max_individual_score': max_score,
                    'total_anomalies': total_anomalies,
                    'volume_score': test_scores['volume_concentration'],
                    'strike_score': test_scores['strike_coordination'],
                    'directional_score': test_scores['directional_bias'],
                    'expiration_score': test_scores['expiration_clustering'],
                    'volatility_score': test_scores['volatility_pattern'],
                    'contract_score': test_scores['contract_anomaly'],
                    'otm_call_score': test_scores['otm_call_insider'],
                    'greeks_score': test_scores['greeks_anomaly'],
                    'total_volume': total_volume,
                    'call_put_ratio': call_put_ratio,
                    'otm_call_ratio': otm_call_ratio,
                    'event_date': row[6],
                    'created_at': row[7]
                })
            
            df = pd.DataFrame(data)
            logger.info(f"Retrieved {len(df)} anomaly results")
            return df
            
    except Exception as e:
        logger.error(f"Failed to retrieve anomaly results: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def print_comprehensive_table(df: pd.DataFrame):
    """Print a comprehensive results table."""
    if df.empty:
        print("No anomaly results to display.")
        return
    
    print("\n" + "="*120)
    print("INSIDER TRADING ANOMALY DETECTION - COMPREHENSIVE RESULTS")
    print("="*120)
    print(f"Analysis Date: {df.iloc[0]['event_date']}")
    print(f"Total Symbols with Anomalies: {len(df)}")
    print(f"Highest Composite Score: {df['composite_score'].max():.2f}")
    print(f"Average Composite Score: {df['composite_score'].mean():.2f}")
    
    # Summary statistics
    print(f"\nANOMALY TYPE FREQUENCY:")
    print(f"  Strike Coordination:    {(df['strike_score'] > 0).sum():,} symbols")
    print(f"  OTM Call Insider:       {(df['otm_call_score'] > 0).sum():,} symbols")
    print(f"  Expiration Clustering:  {(df['expiration_score'] > 0).sum():,} symbols")
    print(f"  Volatility Patterns:    {(df['volatility_score'] > 0).sum():,} symbols")
    print(f"  Directional Bias:       {(df['directional_score'] > 0).sum():,} symbols")
    print(f"  Volume Concentration:   {(df['volume_score'] > 0).sum():,} symbols")
    print(f"  Greeks Anomalies:       {(df['greeks_score'] > 0).sum():,} symbols")
    print(f"  Contract Anomalies:     {(df['contract_score'] > 0).sum():,} symbols")
    
    print("\n" + "-"*120)
    print("TOP 20 ANOMALOUS SYMBOLS (Detailed Breakdown)")
    print("-"*120)
    
    # Header
    header = (
        f"{'Symbol':<8} {'Composite':<10} {'Vol':<5} {'Strike':<7} {'Direct':<7} {'Expiry':<7} "
        f"{'Volat':<6} {'OTM':<5} {'Greeks':<7} {'TotVol':<10} {'C/P':<6} {'OTM%':<6}"
    )
    print(header)
    print("-" * len(header))
    
    # Top 20 rows
    for idx, row in df.head(20).iterrows():
        print(
            f"{row['symbol']:<8} "
            f"{row['composite_score']:<10.1f} "
            f"{row['volume_score']:<5.0f} "
            f"{row['strike_score']:<7.0f} "
            f"{row['directional_score']:<7.0f} "
            f"{row['expiration_score']:<7.0f} "
            f"{row['volatility_score']:<6.0f} "
            f"{row['otm_call_score']:<5.0f} "
            f"{row['greeks_score']:<7.0f} "
            f"{row['total_volume']:<10,.0f} "
            f"{row['call_put_ratio']:<6.1f} "
            f"{row['otm_call_ratio']:<6.1%}"
        )
    
    # High-scoring symbols detail
    high_scorers = df[df['composite_score'] > 200.0].head(10)
    if not high_scorers.empty:
        print(f"\n{'='*80}")
        print("HIGH ANOMALY SCORE DETAILS (Score > 200)")
        print("="*80)
        
        for idx, row in high_scorers.iterrows():
            print(f"\n{row['symbol']} (Composite Score: {row['composite_score']:.1f})")
            print(f"  Volume Score:      {row['volume_score']:<6.0f} | Total Volume: {row['total_volume']:,.0f}")
            print(f"  Strike Score:      {row['strike_score']:<6.0f} | Total Anomalies: {row['total_anomalies']}")
            print(f"  Directional Score: {row['directional_score']:<6.0f} | Call/Put Ratio: {row['call_put_ratio']:.1f}")
            print(f"  Expiration Score:  {row['expiration_score']:<6.0f} | Max Individual: {row['max_individual_score']:.1f}")
            print(f"  Volatility Score:  {row['volatility_score']:<6.0f} | OTM Call %: {row['otm_call_ratio']:.1%}")
            print(f"  OTM Call Score:    {row['otm_call_score']:<6.0f} | Greeks Score: {row['greeks_score']:.0f}")

def export_to_csv(df: pd.DataFrame, filename: str = None):
    """Export results to CSV file."""
    if df.empty:
        return
    
    if not filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"anomaly_results_{timestamp}.csv"
    
    try:
        df.to_csv(filename, index=False)
        logger.info(f"Results exported to: {filename}")
        print(f"\nResults exported to: {filename}")
    except Exception as e:
        logger.error(f"Failed to export CSV: {e}")

def main():
    """Main execution function."""
    logger.info("Generating anomaly results table...")
    
    try:
        # Get results from database
        df = get_anomaly_results_table()
        
        if df.empty:
            print("No anomaly results found. Run the anomaly detection first.")
            return 1
        
        # Print comprehensive table
        print_comprehensive_table(df)
        
        # Export to CSV
        export_to_csv(df)
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to generate results table: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
