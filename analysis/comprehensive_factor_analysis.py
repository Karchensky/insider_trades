#!/usr/bin/env python3
"""
Comprehensive Factor Analysis

Analyze ALL available factors to find the optimal combination for TP100 prediction.
This is a fresh analysis that doesn't rely on previous assumptions.
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Any
from itertools import combinations

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_data_with_outcomes(lookback_days: int = 60, limit: int = 5000) -> List[Dict]:
    """
    Get historical signals with TP100 outcomes.
    
    Uses a simplified approach that processes efficiently.
    """
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            end_date = date.today() - timedelta(days=10)
            start_date = end_date - timedelta(days=lookback_days)
            
            logger.info(f"Fetching data from {start_date} to {end_date}...")
            
            # Get signals with all available factors
            cur.execute("""
                SELECT 
                    a.event_date,
                    a.symbol,
                    a.recommended_option,
                    -- Factors
                    COALESCE(a.greeks_gamma_value, 0) AS gamma,
                    COALESCE(a.greeks_vega_value, 0) AS vega,
                    COALESCE(ABS(a.greeks_theta_value), 0) AS theta,
                    COALESCE(a.total_magnitude, 0) AS magnitude,
                    COALESCE(a.volume_oi_ratio_score, 0) AS vol_oi_score,
                    COALESCE(a.otm_score, 0) AS otm_score,
                    COALESCE(a.z_score, 0) AS z_score,
                    COALESCE(a.total_score, 0) AS total_score,
                    COALESCE(a.volume_score, 0) AS volume_score,
                    COALESCE(a.directional_score, 0) AS directional_score,
                    COALESCE(a.time_score, 0) AS time_score,
                    -- Entry price
                    o.close_price AS entry_price,
                    -- Expiration
                    oc.expiration_date
                FROM daily_anomaly_snapshot a
                INNER JOIN daily_option_snapshot o 
                    ON a.recommended_option = o.contract_ticker 
                    AND a.event_date = o.date
                INNER JOIN option_contracts oc 
                    ON a.recommended_option = oc.contract_ticker
                WHERE a.event_date BETWEEN %s AND %s
                  AND a.total_magnitude >= 20000
                  AND COALESCE(a.is_bot_driven, FALSE) = FALSE
                  AND COALESCE(a.is_earnings_related, FALSE) = FALSE
                  AND a.recommended_option IS NOT NULL
                  AND o.close_price BETWEEN 0.05 AND 5.00
                  AND o.volume > 50
                ORDER BY a.event_date
                LIMIT %s
            """, (start_date, end_date, limit))
            
            signals = [dict(row) for row in cur.fetchall()]
            logger.info(f"Fetched {len(signals)} signals")
            
            # Now get outcomes for each signal
            logger.info("Computing outcomes...")
            results = []
            
            for i, sig in enumerate(signals):
                if i % 100 == 0:
                    logger.info(f"Processing {i}/{len(signals)}...")
                
                cur.execute("""
                    SELECT MAX(close_price) AS max_price
                    FROM daily_option_snapshot
                    WHERE contract_ticker = %s
                      AND date > %s
                      AND date <= %s
                """, (sig['recommended_option'], sig['event_date'], sig['expiration_date']))
                
                max_row = cur.fetchone()
                max_price = float(max_row['max_price']) if max_row and max_row['max_price'] else 0
                entry_price = float(sig['entry_price'])
                
                hit_tp100 = max_price >= 2.0 * entry_price if entry_price > 0 else False
                
                sig['max_price'] = max_price
                sig['hit_tp100'] = hit_tp100
                results.append(sig)
            
            return results
            
    finally:
        conn.close()


def analyze_factors(data: List[Dict]) -> Dict:
    """Analyze each factor individually."""
    import numpy as np
    
    factors = ['gamma', 'vega', 'theta', 'magnitude', 'vol_oi_score', 
               'otm_score', 'z_score', 'total_score', 'volume_score',
               'directional_score', 'time_score']
    
    hit_tp100 = np.array([d['hit_tp100'] for d in data])
    baseline_rate = hit_tp100.mean() * 100
    
    results = {
        'baseline_rate': round(baseline_rate, 1),
        'total_samples': len(data),
        'factors': {}
    }
    
    for factor in factors:
        values = np.array([float(d[factor]) for d in data])
        
        # Skip if all zeros
        if values.max() == 0:
            continue
        
        # Test different percentile thresholds
        best_pctl = None
        best_lift = 0
        best_rate = 0
        best_count = 0
        
        for pctl in [80, 85, 90, 93, 95, 97]:
            threshold = np.percentile(values[values > 0], pctl) if (values > 0).sum() > 10 else 0
            if threshold == 0:
                continue
            
            mask = values >= threshold
            count = mask.sum()
            
            if count >= 10:
                rate = hit_tp100[mask].mean() * 100
                lift = rate / baseline_rate if baseline_rate > 0 else 0
                
                if lift > best_lift:
                    best_lift = lift
                    best_rate = rate
                    best_pctl = pctl
                    best_count = count
                    best_threshold = threshold
        
        if best_pctl:
            results['factors'][factor] = {
                'best_percentile': best_pctl,
                'threshold': round(best_threshold, 4),
                'count': best_count,
                'tp100_rate': round(best_rate, 1),
                'lift': round(best_lift, 2),
            }
    
    # Sort by lift
    results['factors'] = dict(sorted(
        results['factors'].items(), 
        key=lambda x: -x[1]['lift']
    ))
    
    return results


def test_combinations(data: List[Dict], top_factors: List[str], percentile: int = 93) -> Dict:
    """Test combinations of top factors."""
    import numpy as np
    
    hit_tp100 = np.array([d['hit_tp100'] for d in data])
    baseline_rate = hit_tp100.mean() * 100
    
    # Get thresholds for each factor
    thresholds = {}
    for factor in top_factors:
        values = np.array([float(d[factor]) for d in data])
        valid = values > 0
        if valid.sum() > 10:
            thresholds[factor] = np.percentile(values[valid], percentile)
    
    results = []
    
    # Test all 4-factor combinations
    for combo in combinations(top_factors, 4):
        if not all(f in thresholds for f in combo):
            continue
        
        # Compute scores
        scores = np.zeros(len(data))
        for f in combo:
            values = np.array([float(d[f]) for d in data])
            scores += (values >= thresholds[f]).astype(int)
        
        # Test score >= 3
        mask = scores >= 3
        count = mask.sum()
        
        if count >= 10:
            rate = hit_tp100[mask].mean() * 100
            lift = rate / baseline_rate if baseline_rate > 0 else 0
            
            results.append({
                'factors': combo,
                'count': count,
                'tp100_rate': round(rate, 1),
                'lift': round(lift, 2),
            })
    
    # Sort by tp100_rate
    results.sort(key=lambda x: -x['tp100_rate'])
    
    return {
        'percentile': percentile,
        'baseline_rate': round(baseline_rate, 1),
        'combinations': results[:20],  # Top 20
    }


def main():
    """Run comprehensive analysis."""
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=60, help='Lookback days')
    parser.add_argument('--limit', type=int, default=2000, help='Max signals')
    args = parser.parse_args()
    
    print("="*70)
    print("COMPREHENSIVE FACTOR ANALYSIS")
    print("="*70)
    
    # Get data
    data = get_data_with_outcomes(args.days, args.limit)
    
    if not data:
        print("No data available!")
        return
    
    # Analyze individual factors
    print("\n--- INDIVIDUAL FACTOR ANALYSIS ---")
    factor_results = analyze_factors(data)
    
    print(f"\nBaseline TP100 rate: {factor_results['baseline_rate']}%")
    print(f"Total samples: {factor_results['total_samples']}")
    
    print(f"\n{'Factor':<20} {'Pctl':<6} {'Threshold':<12} {'Count':<8} {'TP100%':<10} {'Lift':<8}")
    print("-"*70)
    
    for factor, stats in factor_results['factors'].items():
        print(f"{factor:<20} {stats['best_percentile']:<6} {stats['threshold']:<12.4f} {stats['count']:<8} {stats['tp100_rate']:<10.1f} {stats['lift']:<8.2f}x")
    
    # Test combinations
    print("\n--- 4-FACTOR COMBINATIONS (93rd percentile, score >= 3) ---")
    
    top_factors = list(factor_results['factors'].keys())[:8]  # Top 8 factors
    combo_results = test_combinations(data, top_factors, 93)
    
    print(f"\nTesting combinations of: {top_factors}")
    print(f"Baseline: {combo_results['baseline_rate']}%")
    
    print(f"\n{'Rank':<6} {'Factors':<50} {'Count':<8} {'TP100%':<10} {'Lift':<8}")
    print("-"*80)
    
    for i, combo in enumerate(combo_results['combinations'][:15], 1):
        factors_str = '+'.join(combo['factors'])
        print(f"{i:<6} {factors_str:<50} {combo['count']:<8} {combo['tp100_rate']:<10.1f} {combo['lift']:<8.2f}x")
    
    # Best recommendation
    if combo_results['combinations']:
        best = combo_results['combinations'][0]
        print(f"\n{'='*70}")
        print("RECOMMENDATION")
        print(f"{'='*70}")
        print(f"\nBest 4-factor combination: {'+'.join(best['factors'])}")
        print(f"Expected TP100 rate: {best['tp100_rate']}%")
        print(f"Lift vs baseline: {best['lift']}x")
        print(f"Signal count: {best['count']}")


if __name__ == '__main__':
    main()
