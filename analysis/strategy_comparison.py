#!/usr/bin/env python3
"""
Contract Selection Strategy Comparison

A/B test different contract selection strategies against historical data
to determine which produces the highest TP100 hit rate.

Strategies tested:
- A. Max Volume (current default)
- B. Max Gamma
- C. Best Risk/Reward (gamma * vega / |theta|)
- D. ATM Preference
- E. Model-Ranked (requires trained model)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np
import psycopg2.extras

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StrategyComparison:
    """Compare contract selection strategies using historical data."""
    
    STRATEGIES = ['max_volume', 'max_gamma', 'best_rr', 'atm_preference']
    
    def __init__(self):
        self.conn = None
    
    def _get_connection(self):
        if self.conn is None or self.conn.closed:
            self.conn = db.connect()
        return self.conn
    
    def run_backtest(self, 
                     min_date: Optional[date] = None,
                     max_date: Optional[date] = None,
                     min_magnitude: float = 20000) -> Dict[str, Any]:
        """
        Run backtest comparing all strategies using a simplified approach.
        
        Uses the existing analysis.py approach which has proven to work.
        
        Args:
            min_date: Start date for backtest
            max_date: End date for backtest
            min_magnitude: Minimum magnitude threshold
        
        Returns:
            Dict with comparison results for each strategy
        """
        logger.info("Running strategy comparison backtest (simplified approach)...")
        
        conn = self._get_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Step 1: Get anomaly events with their recommended options
        logger.info("Step 1: Fetching anomaly events...")
        query1 = """
        SELECT 
            a.event_date,
            a.symbol,
            a.direction,
            a.total_magnitude,
            a.is_high_conviction,
            a.high_conviction_score,
            a.recommended_option
        FROM daily_anomaly_snapshot a
        WHERE a.total_magnitude >= %s
          AND COALESCE(a.is_bot_driven, FALSE) = FALSE
          AND COALESCE(a.is_earnings_related, FALSE) = FALSE
          AND a.recommended_option IS NOT NULL
        """
        
        params = [min_magnitude]
        
        if min_date:
            query1 += " AND a.event_date >= %s"
            params.append(min_date)
        if max_date:
            query1 += " AND a.event_date <= %s"
            params.append(max_date)
        
        query1 += " ORDER BY a.event_date"
        
        cur.execute(query1, params)
        anomalies = cur.fetchall()
        logger.info(f"Found {len(anomalies)} anomaly events")
        
        if not anomalies:
            return {}
        
        # Step 2: For each anomaly, get all tradeable contracts and compute outcomes
        logger.info("Step 2: Processing contracts for each event...")
        all_results = []
        
        for i, anomaly in enumerate(anomalies):
            if i % 50 == 0:
                logger.info(f"Processing event {i+1}/{len(anomalies)}...")
            
            event_date = anomaly['event_date']
            symbol = anomaly['symbol']
            direction = anomaly['direction']
            
            # Get all tradeable contracts for this event
            query2 = """
            SELECT 
                o.contract_ticker,
                o.close_price AS entry_price,
                o.volume AS session_volume,
                COALESCE(o.greeks_gamma, 0) AS gamma,
                COALESCE(o.greeks_theta, 0) AS theta,
                COALESCE(o.greeks_vega, 0) AS vega,
                oc.strike_price,
                oc.expiration_date,
                oc.contract_type,
                COALESCE(s.close, s.weighted_average_price, 0) AS underlying_price
            FROM daily_option_snapshot o
            INNER JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
            LEFT JOIN daily_stock_snapshot s ON o.symbol = s.symbol AND o.date = s.date
            WHERE o.symbol = %s 
              AND o.date = %s
              AND o.close_price BETWEEN 0.05 AND 5.00
              AND o.volume > 50
              AND oc.expiration_date > %s
            """
            
            contract_params = [symbol, event_date, event_date]
            
            # Filter by direction
            if direction == 'call_heavy':
                query2 += " AND oc.contract_type = 'call'"
            elif direction == 'put_heavy':
                query2 += " AND oc.contract_type = 'put'"
            
            cur.execute(query2, contract_params)
            contracts = cur.fetchall()
            
            if not contracts:
                continue
            
            # Compute metrics and rank contracts
            contract_data = []
            for c in contracts:
                underlying = float(c['underlying_price']) if c['underlying_price'] else 0
                strike = float(c['strike_price']) if c['strike_price'] else 0
                moneyness = strike / underlying if underlying > 0 else None
                
                theta = float(c['theta']) if c['theta'] else 0
                gamma = float(c['gamma']) if c['gamma'] else 0
                vega = float(c['vega']) if c['vega'] else 0
                
                risk_reward = (gamma * vega / abs(theta)) if abs(theta) > 0.001 else 0
                
                if moneyness is None:
                    continue
                
                contract_data.append({
                    'contract_ticker': c['contract_ticker'],
                    'entry_price': float(c['entry_price']),
                    'session_volume': int(c['session_volume']),
                    'gamma': gamma,
                    'theta': theta,
                    'vega': vega,
                    'moneyness': moneyness,
                    'risk_reward': risk_reward,
                    'expiration_date': c['expiration_date'],
                    'atm_distance': abs(moneyness - 1.0),
                })
            
            if not contract_data:
                continue
            
            # Rank by each strategy
            by_volume = sorted(contract_data, key=lambda x: -x['session_volume'])
            by_gamma = sorted(contract_data, key=lambda x: -x['gamma'])
            by_rr = sorted(contract_data, key=lambda x: -x['risk_reward'])
            by_atm = sorted(contract_data, key=lambda x: x['atm_distance'])
            
            # Get the top pick for each strategy and compute TP100 outcome
            strategy_picks = {
                'max_volume': by_volume[0] if by_volume else None,
                'max_gamma': by_gamma[0] if by_gamma else None,
                'best_rr': by_rr[0] if by_rr else None,
                'atm_preference': by_atm[0] if by_atm else None,
            }
            
            for strategy, pick in strategy_picks.items():
                if pick is None:
                    continue
                
                # Get max future price for this contract
                query3 = """
                SELECT MAX(close_price) AS max_future_price
                FROM daily_option_snapshot
                WHERE contract_ticker = %s
                  AND date > %s
                  AND date <= %s
                """
                cur.execute(query3, [pick['contract_ticker'], event_date, pick['expiration_date']])
                result = cur.fetchone()
                
                max_future = float(result['max_future_price']) if result and result['max_future_price'] else 0
                hit_tp100 = 1 if max_future >= 2.0 * pick['entry_price'] else 0
                
                all_results.append({
                    'event_date': event_date,
                    'symbol': symbol,
                    'strategy': strategy,
                    'contract_ticker': pick['contract_ticker'],
                    'entry_price': pick['entry_price'],
                    'max_future_price': max_future,
                    'hit_tp_100': hit_tp100,
                    'gamma': pick['gamma'],
                    'moneyness': pick['moneyness'],
                    'risk_reward': pick['risk_reward'],
                    'is_high_conviction': anomaly['is_high_conviction'],
                    'high_conviction_score': anomaly['high_conviction_score'],
                })
        
        cur.close()
        
        if not all_results:
            logger.warning("No results generated")
            return {}
        
        df = pd.DataFrame(all_results)
        logger.info(f"Generated {len(df)} strategy-contract pairs")
        
        # Aggregate results by strategy
        results = {}
        
        for strategy in self.STRATEGIES:
            strategy_df = df[df['strategy'] == strategy].copy()
            
            if len(strategy_df) == 0:
                continue
            
            total_trades = len(strategy_df)
            winners = strategy_df['hit_tp_100'].sum()
            losers = total_trades - winners
            tp100_rate = winners / total_trades * 100 if total_trades > 0 else 0
            
            hc_df = strategy_df[strategy_df['is_high_conviction'] == True]
            hc_total = len(hc_df)
            hc_winners = hc_df['hit_tp_100'].sum() if hc_total > 0 else 0
            hc_tp100_rate = hc_winners / hc_total * 100 if hc_total > 0 else 0
            
            results[strategy] = {
                'total_trades': total_trades,
                'winners': int(winners),
                'losers': int(losers),
                'tp100_rate': round(tp100_rate, 2),
                'avg_gamma': round(strategy_df['gamma'].mean(), 6),
                'avg_moneyness': round(strategy_df['moneyness'].mean(), 4),
                'avg_risk_reward': round(strategy_df['risk_reward'].mean(), 4),
                'high_conviction_trades': hc_total,
                'high_conviction_winners': int(hc_winners),
                'high_conviction_tp100_rate': round(hc_tp100_rate, 2),
            }
        
        return results
    
    def print_comparison_report(self, results: Dict[str, Any]) -> None:
        """Print formatted comparison report."""
        if not results:
            print("No results to display")
            return
        
        print("\n" + "="*80)
        print("CONTRACT SELECTION STRATEGY COMPARISON")
        print("="*80)
        
        best_strategy = max(results.items(), key=lambda x: x[1]['tp100_rate'])
        
        print(f"\n{'Strategy':<20} {'Trades':>8} {'Winners':>8} {'TP100%':>8} {'Avg Gamma':>12} {'Avg Moneyness':>14}")
        print("-"*80)
        
        for strategy, stats in sorted(results.items(), key=lambda x: -x[1]['tp100_rate']):
            marker = " *BEST*" if strategy == best_strategy[0] else ""
            print(f"{strategy:<20} {stats['total_trades']:>8} {stats['winners']:>8} {stats['tp100_rate']:>7.1f}% {stats['avg_gamma']:>12.6f} {stats['avg_moneyness']:>14.4f}{marker}")
        
        print("\n" + "-"*80)
        print("HIGH CONVICTION SUBSET (is_high_conviction = TRUE)")
        print("-"*80)
        
        print(f"\n{'Strategy':<20} {'HC Trades':>10} {'HC Winners':>10} {'HC TP100%':>10}")
        print("-"*60)
        
        for strategy, stats in sorted(results.items(), key=lambda x: -x[1]['high_conviction_tp100_rate']):
            print(f"{strategy:<20} {stats['high_conviction_trades']:>10} {stats['high_conviction_winners']:>10} {stats['high_conviction_tp100_rate']:>9.1f}%")
        
        print("\n" + "="*80)
        print(f"RECOMMENDATION: Use '{best_strategy[0]}' strategy ({best_strategy[1]['tp100_rate']:.1f}% TP100 rate)")
        print("="*80)
    
    def get_strategy_lift_analysis(self, results: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate lift of each strategy vs baseline (max_volume).
        
        Returns:
            Dict mapping strategy to lift percentage
        """
        if 'max_volume' not in results:
            return {}
        
        baseline_rate = results['max_volume']['tp100_rate']
        
        lifts = {}
        for strategy, stats in results.items():
            if baseline_rate > 0:
                lift = ((stats['tp100_rate'] - baseline_rate) / baseline_rate) * 100
            else:
                lift = 0
            lifts[strategy] = round(lift, 2)
        
        return lifts


def main():
    """Run strategy comparison."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Compare contract selection strategies')
    parser.add_argument('--days', type=int, default=90, help='Number of days to analyze')
    parser.add_argument('--min-magnitude', type=float, default=20000, help='Minimum magnitude threshold')
    args = parser.parse_args()
    
    comparison = StrategyComparison()
    
    max_date = date.today() - timedelta(days=1)
    min_date = max_date - timedelta(days=args.days)
    
    results = comparison.run_backtest(
        min_date=min_date,
        max_date=max_date,
        min_magnitude=args.min_magnitude
    )
    
    comparison.print_comparison_report(results)
    
    lifts = comparison.get_strategy_lift_analysis(results)
    if lifts:
        print("\nLift vs max_volume baseline:")
        for strategy, lift in sorted(lifts.items(), key=lambda x: -x[1]):
            print(f"  {strategy}: {lift:+.1f}%")


if __name__ == '__main__':
    main()
