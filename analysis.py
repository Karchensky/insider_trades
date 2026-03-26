#!/usr/bin/env python3
"""
TWO-TIER VALIDATION
Correct analysis of event-level scoring and contract selection strategies
for +100% Take Profit strategy.

Fixes from previous version:
- Uses ONE contract per event (recommended_option), not all contracts
- Walk-forward validation (train thresholds on earlier data, test on later)
- Wilson confidence intervals on all hit rates
- Bonferroni correction for multiple comparison
- Separates event-level scoring from contract selection analysis
"""
import sys
import argparse
from math import sqrt
sys.path.append('.')
from database.core.connection import db
import psycopg2.extras
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from itertools import combinations
from datetime import date, timedelta


# =============================================================================
# STATISTICAL HELPERS
# =============================================================================

def wilson_ci(hits, total, z=1.96):
    """Wilson score confidence interval for a proportion."""
    if total == 0:
        return 0.0, 0.0, 0.0
    p = hits / total
    denom = 1 + z**2 / total
    center = (p + z**2 / (2 * total)) / denom
    spread = z * sqrt((p * (1 - p) + z**2 / (4 * total)) / total) / denom
    return max(0, center - spread) * 100, center * 100, min(1, center + spread) * 100


def fmt_ci(hits, total):
    """Format hit rate with Wilson CI."""
    if total == 0:
        return "N/A (n=0)"
    lo, mid, hi = wilson_ci(hits, total)
    return f"{mid:.1f}% [{lo:.1f}-{hi:.1f}%] (n={total})"


# =============================================================================
# DATA LOADING
# =============================================================================

def load_event_level_data(cur):
    """
    Load ONE row per (symbol, event_date) using the recommended_option.
    This is what the production system actually trades.
    """
    cur.execute("""
        SELECT
            a.symbol, a.event_date, a.direction,
            a.recommended_option,
            -- Event-level factors (symbol aggregate scores)
            a.total_score, a.volume_score, a.volume_oi_ratio_score,
            a.otm_score, a.directional_score, a.time_score,
            a.z_score, a.total_magnitude, a.total_volume,
            -- Recommended contract details
            o.close_price AS entry_price,
            o.implied_volatility, o.greeks_delta, o.greeks_gamma,
            o.greeks_theta, o.greeks_vega,
            o.volume AS option_volume, o.open_interest,
            oc.expiration_date
        FROM daily_anomaly_snapshot a
        INNER JOIN daily_option_snapshot o
            ON a.recommended_option = o.contract_ticker
            AND a.event_date = o.date
        INNER JOIN option_contracts oc
            ON a.recommended_option = oc.contract_ticker
        WHERE a.total_magnitude >= 20000
          AND COALESCE(a.is_bot_driven, false) = false
          AND ABS(COALESCE(a.intraday_price_move_pct, 0)) < 5
          AND COALESCE(a.is_earnings_related, false) = false
          AND a.recommended_option IS NOT NULL
          AND o.close_price BETWEEN 0.05 AND 5.00
          AND o.volume > 50
        ORDER BY a.event_date
    """)
    return [dict(row) for row in cur.fetchall()]


def load_contract_selection_data(cur):
    """
    Load ALL tradeable contracts per event for contract selection comparison.
    Each row includes the contract's TP100 outcome.
    """
    cur.execute("""
        WITH events AS (
            SELECT DISTINCT a.symbol, a.event_date, a.direction,
                   a.total_magnitude, a.volume_score, a.z_score,
                   a.volume_oi_ratio_score, a.recommended_option
            FROM daily_anomaly_snapshot a
            WHERE a.total_magnitude >= 20000
              AND COALESCE(a.is_bot_driven, false) = false
              AND ABS(COALESCE(a.intraday_price_move_pct, 0)) < 5
              AND COALESCE(a.is_earnings_related, false) = false
              AND a.recommended_option IS NOT NULL
        ),
        contracts AS (
            SELECT
                e.symbol, e.event_date, e.direction, e.recommended_option,
                o.contract_ticker,
                o.close_price AS entry_price,
                o.volume AS option_volume,
                o.open_interest,
                o.greeks_gamma, o.greeks_vega, o.greeks_theta, o.greeks_delta,
                o.implied_volatility,
                oc.expiration_date,
                oc.strike_price,
                oc.contract_type
            FROM events e
            INNER JOIN daily_option_snapshot o
                ON e.symbol = o.symbol AND e.event_date = o.date
                AND o.volume > 50 AND o.close_price BETWEEN 0.05 AND 5.00
            INNER JOIN option_contracts oc
                ON o.contract_ticker = oc.contract_ticker
        ),
        with_outcomes AS (
            SELECT
                c.*,
                (SELECT MAX(of2.close_price)
                 FROM daily_option_snapshot of2
                 WHERE of2.contract_ticker = c.contract_ticker
                   AND of2.date > c.event_date
                   AND of2.date <= c.expiration_date
                ) AS max_future_price
            FROM contracts c
        )
        SELECT * FROM with_outcomes
        WHERE max_future_price IS NOT NULL
        ORDER BY symbol, event_date, option_volume DESC
    """)
    return [dict(row) for row in cur.fetchall()]


def compute_tp100(data):
    """Add TP100 outcome to each record."""
    for row in data:
        entry = float(row['entry_price'])
        max_price = float(row.get('max_future_price') or row.get('max_price') or 0)
        row['hit_tp100'] = max_price >= entry * 2.0 if entry > 0 else False
    return data


def add_event_outcomes(cur, events):
    """Compute TP100 for each event using recommended_option."""
    results = []
    for i, ev in enumerate(events):
        if i % 200 == 0 and i > 0:
            print(f"  Computing outcomes: {i}/{len(events)}...")
        cur.execute("""
            SELECT MAX(close_price) AS max_price
            FROM daily_option_snapshot
            WHERE contract_ticker = %s
              AND date > %s
              AND date <= %s
        """, (ev['recommended_option'], ev['event_date'], ev['expiration_date']))
        row = cur.fetchone()
        max_price = float(row['max_price']) if row and row['max_price'] else 0
        entry = float(ev['entry_price'])
        ev['max_price'] = max_price
        ev['hit_tp100'] = max_price >= entry * 2.0 if entry > 0 else False
        results.append(ev)
    return results


# =============================================================================
# SECTION 1: EVENT-LEVEL FACTOR ANALYSIS
# =============================================================================

def analyze_event_factors(data, label=""):
    """Analyze event-level factors on the recommended contract's TP100 outcome."""
    if not data:
        print("  No data for analysis")
        return {}

    hit_tp100 = np.array([d['hit_tp100'] for d in data])
    baseline_hits = int(hit_tp100.sum())
    baseline_n = len(data)
    baseline_rate = baseline_hits / baseline_n * 100 if baseline_n > 0 else 0

    print(f"\n  Baseline: {fmt_ci(baseline_hits, baseline_n)}")

    # Event-level factors (these are symbol-level aggregates, not contract-specific)
    event_factors = {
        'volume_score': np.array([float(d['volume_score'] or 0) for d in data]),
        'z_score': np.array([float(d['z_score'] or 0) for d in data]),
        'vol_oi_score': np.array([float(d['volume_oi_ratio_score'] or 0) for d in data]),
        'magnitude': np.array([float(d['total_magnitude'] or 0) for d in data]),
        'total_score': np.array([float(d['total_score'] or 0) for d in data]),
        'directional_score': np.array([float(d['directional_score'] or 0) for d in data]),
        'time_score': np.array([float(d['time_score'] or 0) for d in data]),
        'otm_score': np.array([float(d['otm_score'] or 0) for d in data]),
    }

    # Contract-level factors (from recommended contract — for comparison)
    contract_factors = {
        'gamma': np.array([float(d['greeks_gamma'] or 0) for d in data]),
        'vega': np.array([float(d['greeks_vega'] or 0) for d in data]),
        'theta': np.array([abs(float(d['greeks_theta'] or 0)) for d in data]),
        'delta': np.array([abs(float(d['greeks_delta'] or 0)) for d in data]),
        'iv': np.array([float(d['implied_volatility'] or 0) for d in data]),
    }

    all_factors = {**event_factors, **contract_factors}

    print(f"\n  {'Factor':<20} {'Type':<10} {'Pctl':<6} {'Threshold':<12} {'TP100 Rate':<30} {'Lift':<8}")
    print("  " + "-" * 90)

    results = {}
    for name, arr in all_factors.items():
        factor_type = "EVENT" if name in event_factors else "CONTRACT"
        valid = arr > 0
        if valid.sum() < 50:
            continue

        best = None
        for pctl in [80, 85, 90, 93, 95]:
            thresh = np.percentile(arr[valid], pctl)
            mask = arr >= thresh
            count = int(mask.sum())
            if count < 10:
                continue
            hits = int((hit_tp100 & mask).sum())
            rate = hits / count * 100
            lift = rate / baseline_rate if baseline_rate > 0 else 0
            if best is None or lift > best['lift']:
                best = {'pctl': pctl, 'thresh': thresh, 'count': count,
                        'hits': hits, 'rate': rate, 'lift': lift}

        if best:
            ci_str = fmt_ci(best['hits'], best['count'])
            print(f"  {name:<20} {factor_type:<10} {best['pctl']:<6} {best['thresh']:<12.4f} {ci_str:<30} {best['lift']:.2f}x")
            results[name] = best

    return results


def test_event_combinations(data, factor_names, pctl=93):
    """Test 4-factor combinations at a given percentile, score >= 3."""
    hit_tp100 = np.array([d['hit_tp100'] for d in data])
    baseline_rate = hit_tp100.mean() * 100 if len(data) > 0 else 0

    factors = {}
    thresholds = {}
    for name in factor_names:
        if name == 'magnitude':
            arr = np.array([float(d['total_magnitude'] or 0) for d in data])
        elif name == 'vol_oi_score':
            arr = np.array([float(d['volume_oi_ratio_score'] or 0) for d in data])
        else:
            arr = np.array([float(d.get(name) or 0) for d in data])

        valid = arr > 0
        if valid.sum() < 50:
            continue
        factors[name] = arr
        thresholds[name] = np.percentile(arr[valid], pctl)

    results = []
    for combo in combinations(factors.keys(), 4):
        scores = np.zeros(len(data))
        for f in combo:
            scores += (factors[f] >= thresholds[f]).astype(int)

        mask = scores >= 3
        count = int(mask.sum())
        if count < 10:
            continue
        hits = int((hit_tp100 & mask).sum())
        rate = hits / count * 100
        lift = rate / baseline_rate if baseline_rate > 0 else 0
        lo, _, hi = wilson_ci(hits, count)
        results.append({
            'factors': combo, 'count': count, 'hits': hits,
            'tp_rate': rate, 'lift': lift, 'ci_lo': lo, 'ci_hi': hi
        })

    results.sort(key=lambda x: -x['tp_rate'])
    return results, baseline_rate


# =============================================================================
# SECTION 2: WALK-FORWARD VALIDATION
# =============================================================================

def walk_forward_validation(data, factor_combo, min_train_days=30):
    """
    Walk-forward: train thresholds on earlier data, evaluate on held-out later data.
    Splits by calendar month boundaries.
    """
    from collections import defaultdict

    # Group by month
    monthly = defaultdict(list)
    for d in data:
        key = (d['event_date'].year, d['event_date'].month)
        monthly[key] = monthly.get(key, [])
        monthly[key].append(d)

    months = sorted(monthly.keys())
    if len(months) < 3:
        print("  Not enough months for walk-forward (need >= 3)")
        return None

    fold_results = []
    for test_idx in range(2, len(months)):
        train_months = months[:test_idx]
        test_month = months[test_idx]

        train_data = []
        for m in train_months:
            train_data.extend(monthly[m])
        test_data = monthly[test_month]

        if len(train_data) < 50 or len(test_data) < 10:
            continue

        # Train: compute thresholds at 93rd percentile
        train_thresholds = {}
        for f in factor_combo:
            if f == 'magnitude':
                arr = np.array([float(d['total_magnitude'] or 0) for d in train_data])
            elif f == 'vol_oi_score':
                arr = np.array([float(d['volume_oi_ratio_score'] or 0) for d in train_data])
            else:
                arr = np.array([float(d.get(f) or 0) for d in train_data])
            valid = arr > 0
            if valid.sum() < 20:
                train_thresholds[f] = 999999  # effectively disable
            else:
                train_thresholds[f] = np.percentile(arr[valid], 93)

        # Test: apply thresholds to held-out month
        test_hit = np.array([d['hit_tp100'] for d in test_data])
        test_scores = np.zeros(len(test_data))
        for f in factor_combo:
            if f == 'magnitude':
                arr = np.array([float(d['total_magnitude'] or 0) for d in test_data])
            elif f == 'vol_oi_score':
                arr = np.array([float(d['volume_oi_ratio_score'] or 0) for d in test_data])
            else:
                arr = np.array([float(d.get(f) or 0) for d in test_data])
            test_scores += (arr >= train_thresholds[f]).astype(int)

        mask = test_scores >= 3
        count = int(mask.sum())
        hits = int((test_hit & mask).sum())
        total_test = len(test_data)
        baseline_hits = int(test_hit.sum())

        fold_results.append({
            'test_month': f"{test_month[0]}-{test_month[1]:02d}",
            'train_n': len(train_data),
            'test_n': total_test,
            'baseline_hits': baseline_hits,
            'baseline_n': total_test,
            'signal_hits': hits,
            'signal_n': count,
            'thresholds': dict(train_thresholds),
        })

    return fold_results


# =============================================================================
# SECTION 3: CONTRACT SELECTION STRATEGY COMPARISON
# =============================================================================

def compare_contract_strategies(contract_data):
    """
    For each event, simulate different contract selection strategies
    and compare TP100 outcomes.
    """
    from collections import defaultdict

    # Group contracts by (symbol, event_date)
    events = defaultdict(list)
    for c in contract_data:
        key = (c['symbol'], str(c['event_date']))
        events[key].append(c)

    strategies = {
        'max_volume': lambda cs: max(cs, key=lambda x: x['option_volume'] or 0),
        'max_gamma': lambda cs: max(cs, key=lambda x: float(x['greeks_gamma'] or 0)),
        'max_vega': lambda cs: max(cs, key=lambda x: float(x['greeks_vega'] or 0)),
        'min_otm': lambda cs: min(cs, key=lambda x: abs(1.0 - float(x['strike_price'] or 0) / underlying) if (underlying := next((float(c2.get('strike_price', 0)) for c2 in cs), 0)) > 0 else 999),
        'max_risk_reward': lambda cs: max(cs, key=lambda x: (float(x['greeks_gamma'] or 0) * float(x['greeks_vega'] or 0)) / max(abs(float(x['greeks_theta'] or 0)), 0.001)),
    }

    # Simpler ATM strategy that doesn't need underlying price
    # Use the contract with highest gamma as ATM proxy (gamma peaks at ATM)
    # max_gamma already captures this

    results = {name: {'hits': 0, 'total': 0} for name in strategies}

    for (symbol, event_date), contracts in events.items():
        if len(contracts) < 2:
            continue

        # Filter by direction if available
        direction = contracts[0].get('direction', 'mixed')
        if direction == 'call_heavy':
            eligible = [c for c in contracts if c.get('contract_type') == 'call'] or contracts
        elif direction == 'put_heavy':
            eligible = [c for c in contracts if c.get('contract_type') == 'put'] or contracts
        else:
            eligible = contracts

        if not eligible:
            continue

        for name, selector in strategies.items():
            try:
                selected = selector(eligible)
                entry = float(selected['entry_price'])
                max_p = float(selected.get('max_future_price') or 0)
                hit = max_p >= entry * 2.0 if entry > 0 else False
                results[name]['hits'] += int(hit)
                results[name]['total'] += 1
            except (ValueError, TypeError, ZeroDivisionError):
                pass

    return results


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Two-tier validation analysis')
    parser.add_argument('--skip-contracts', action='store_true',
                        help='Skip contract selection analysis (faster)')
    args = parser.parse_args()

    conn = db.connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 80)
    print("TWO-TIER VALIDATION: Event Scoring + Contract Selection")
    print("=" * 80)
    print("\nMethodology fixes:")
    print("  - ONE contract per event (recommended_option), not all contracts")
    print("  - Walk-forward validation (train on earlier months, test on later)")
    print("  - Wilson confidence intervals on all hit rates")
    print("  - Separate event-level factors from contract-level factors")

    # =========================================================================
    # SECTION 1: Event-level data
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 1: LOADING EVENT-LEVEL DATA (one row per symbol/date)")
    print("=" * 80)

    events = load_event_level_data(cur)
    print(f"\nTotal events: {len(events)}")
    if not events:
        print("No data available!")
        conn.close()
        return

    print("Computing TP100 outcomes for recommended contracts...")
    events = add_event_outcomes(cur, events)

    dates = sorted(set(d['event_date'] for d in events))
    print(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} trading days)")

    hit_count = sum(d['hit_tp100'] for d in events)
    print(f"Baseline TP100: {fmt_ci(hit_count, len(events))}")

    # =========================================================================
    # SECTION 2: Individual factor analysis (in-sample, for discovery)
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 2: INDIVIDUAL FACTOR PERFORMANCE (in-sample, for discovery)")
    print("=" * 80)
    print("  Note: in-sample results are for hypothesis generation only.")
    print("  Walk-forward results (Section 4) are what matters.")

    factor_results = analyze_event_factors(events, "all")

    # =========================================================================
    # SECTION 3: 4-factor combination search (in-sample)
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 3: 4-FACTOR COMBINATIONS (in-sample, 93rd pctl, score>=3)")
    print("=" * 80)

    # Only test event-level factors for event scoring
    event_factor_names = ['volume_score', 'z_score', 'vol_oi_score', 'magnitude',
                          'total_score', 'directional_score', 'time_score', 'otm_score']

    combos, baseline = test_event_combinations(events, event_factor_names, pctl=93)

    n_combos = len(combos)
    bonferroni_note = f"(Bonferroni: {n_combos} comparisons, effective alpha = {0.05/max(n_combos,1):.4f})"
    print(f"\n  Tested {n_combos} combinations {bonferroni_note}")
    print(f"  Baseline: {baseline:.1f}%")

    print(f"\n  {'Rank':<6} {'Factors':<55} {'n':<6} {'TP100%':<8} {'95% CI':<18} {'Lift':<6}")
    print("  " + "-" * 100)
    for i, c in enumerate(combos[:15], 1):
        fs = '+'.join(c['factors'])
        ci = f"[{c['ci_lo']:.1f}-{c['ci_hi']:.1f}%]"
        print(f"  {i:<6} {fs:<55} {c['count']:<6} {c['tp_rate']:<8.1f} {ci:<18} {c['lift']:.2f}x")

    # =========================================================================
    # SECTION 4: Walk-forward validation of top combos
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 4: WALK-FORWARD VALIDATION (out-of-sample)")
    print("=" * 80)
    print("  Train thresholds on earlier months, test on held-out month.")

    # Test top 5 combos + the proposed event scoring combo
    proposed_event_combo = ('volume_score', 'z_score', 'vol_oi_score', 'magnitude')
    combos_to_test = []
    seen = set()
    # Always include proposed combo
    combos_to_test.append(proposed_event_combo)
    seen.add(frozenset(proposed_event_combo))
    # Add top in-sample combos
    for c in combos[:10]:
        key = frozenset(c['factors'])
        if key not in seen:
            combos_to_test.append(c['factors'])
            seen.add(key)
        if len(combos_to_test) >= 6:
            break

    for combo in combos_to_test:
        combo_name = '+'.join(combo)
        print(f"\n  --- {combo_name} ---")

        folds = walk_forward_validation(events, combo)
        if not folds:
            print("  Insufficient data for walk-forward")
            continue

        total_signal_hits = 0
        total_signal_n = 0
        total_baseline_hits = 0
        total_baseline_n = 0

        print(f"  {'Month':<12} {'Train n':<10} {'Test n':<10} {'Baseline':<20} {'Signal':<30}")
        print("  " + "-" * 85)

        for fold in folds:
            bl_str = fmt_ci(fold['baseline_hits'], fold['baseline_n'])
            if fold['signal_n'] > 0:
                sig_str = fmt_ci(fold['signal_hits'], fold['signal_n'])
            else:
                sig_str = "no signals"

            print(f"  {fold['test_month']:<12} {fold['train_n']:<10} {fold['test_n']:<10} {bl_str:<20} {sig_str:<30}")

            total_signal_hits += fold['signal_hits']
            total_signal_n += fold['signal_n']
            total_baseline_hits += fold['baseline_hits']
            total_baseline_n += fold['baseline_n']

        if total_signal_n > 0:
            agg_str = fmt_ci(total_signal_hits, total_signal_n)
            bl_agg = fmt_ci(total_baseline_hits, total_baseline_n)
            lift = (total_signal_hits / total_signal_n) / (total_baseline_hits / total_baseline_n) if total_baseline_hits > 0 else 0
            print(f"\n  OUT-OF-SAMPLE AGGREGATE:")
            print(f"    Baseline: {bl_agg}")
            print(f"    Signal:   {agg_str}  (lift: {lift:.2f}x)")
        else:
            print(f"\n  No signals produced out-of-sample")

    # =========================================================================
    # SECTION 5: Contract selection strategy comparison
    # =========================================================================
    if not args.skip_contracts:
        print("\n" + "=" * 80)
        print("SECTION 5: CONTRACT SELECTION STRATEGY COMPARISON")
        print("=" * 80)
        print("  For each event, which contract selection strategy yields best TP100?")
        print("  (This query may take a few minutes...)")

        try:
            contract_data = load_contract_selection_data(cur)
            print(f"\n  Loaded {len(contract_data)} contract-event rows")

            strategy_results = compare_contract_strategies(contract_data)

            print(f"\n  {'Strategy':<20} {'TP100 Rate':<35} {'Lift vs max_volume'}")
            print("  " + "-" * 70)

            mv_rate = strategy_results['max_volume']['hits'] / max(strategy_results['max_volume']['total'], 1)
            for name, res in sorted(strategy_results.items(), key=lambda x: -(x[1]['hits'] / max(x[1]['total'], 1))):
                ci_str = fmt_ci(res['hits'], res['total'])
                rate = res['hits'] / max(res['total'], 1)
                lift_vs_mv = rate / mv_rate if mv_rate > 0 else 0
                marker = " <-- current" if name == 'max_volume' else ""
                print(f"  {name:<20} {ci_str:<35} {lift_vs_mv:.2f}x{marker}")
        except Exception as e:
            print(f"\n  Contract selection query failed (may timeout): {e}")
            print("  Run with --skip-contracts to skip this section.")
    else:
        print("\n  (Contract selection analysis skipped with --skip-contracts)")

    # =========================================================================
    # SECTION 6: SUMMARY & RECOMMENDATION
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 6: SUMMARY")
    print("=" * 80)

    print(f"""
TWO-TIER ARCHITECTURE RECOMMENDATION:

TIER 1 - EVENT SCORING (symbol-level, reduces false positives):
  Factors: volume_score, z_score, vol_oi_ratio_score, magnitude
  Gate: >= 3 of 4 above 93rd percentile thresholds
  Filters: NOT bot-driven, NOT earnings-related
  Purpose: "Is this unusual enough to trade?"

TIER 2 - CONTRACT SELECTION (contract-level, picks best option):
  Strategy: Compare max_gamma vs max_volume on your data
  Pool: Tradeable contracts ($0.05-$5.00, vol > 50, direction-aligned)
  Purpose: "Which contract gives best TP100 odds?"

IMPORTANT: The in-sample numbers (Section 3) are for hypothesis generation.
Only the walk-forward results (Section 4) should inform production decisions.
    """)

    # =========================================================================
    # VISUALIZATION
    # =========================================================================
    print("Creating visualization...")

    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle('Two-Tier Validation: Event Scoring + Contract Selection\n'
                 '(Using recommended_option only, walk-forward validated)',
                 fontsize=13, fontweight='bold')

    # 1. Individual factor lift (in-sample)
    ax1 = axes[0, 0]
    if factor_results:
        event_factors_set = {'volume_score', 'z_score', 'vol_oi_score', 'magnitude',
                             'total_score', 'directional_score', 'time_score', 'otm_score'}
        sorted_factors = sorted(factor_results.items(), key=lambda x: -x[1]['lift'])[:12]
        names = [f[0] for f in sorted_factors]
        lifts = [f[1]['lift'] for f in sorted_factors]
        colors = ['#2196F3' if n in event_factors_set else '#FF9800' for n in names]
        ax1.barh(range(len(names)), lifts, color=colors)
        ax1.set_yticks(range(len(names)))
        ax1.set_yticklabels(names)
        ax1.axvline(x=1, color='red', linestyle='--', label='Baseline (1.0x)')
        ax1.set_xlabel('Lift vs baseline')
        ax1.set_title('Individual Factor Lift (in-sample)\nBlue=Event-level, Orange=Contract-level')
        ax1.invert_yaxis()
        ax1.legend()

    # 2. Top combinations (in-sample)
    ax2 = axes[0, 1]
    if combos:
        top_n = min(10, len(combos))
        combo_labels = ['+'.join(c['factors']) for c in combos[:top_n]]
        combo_rates = [c['tp_rate'] for c in combos[:top_n]]
        combo_lo = [c['ci_lo'] for c in combos[:top_n]]
        combo_hi = [c['ci_hi'] for c in combos[:top_n]]
        y_pos = range(top_n)

        proposed_set = frozenset(proposed_event_combo)
        colors = ['#4CAF50' if frozenset(c['factors']) == proposed_set else '#2196F3'
                  for c in combos[:top_n]]

        ax2.barh(y_pos, combo_rates, color=colors, alpha=0.8)
        # Error bars from CI
        for i, (lo, hi, rate) in enumerate(zip(combo_lo, combo_hi, combo_rates)):
            ax2.plot([lo, hi], [i, i], 'k-', linewidth=1.5)
        ax2.set_yticks(y_pos)
        ax2.set_yticklabels([l[:45] + '...' if len(l) > 45 else l for l in combo_labels], fontsize=8)
        ax2.axvline(x=baseline, color='red', linestyle='--', label=f'Baseline ({baseline:.0f}%)')
        for i, c in enumerate(combos[:top_n]):
            ax2.text(c['tp_rate'] + 0.5, i, f'n={c["count"]}', va='center', fontsize=7)
        ax2.set_xlabel('+100% TP Rate (%)')
        ax2.set_title('Top 4-Factor Combos (in-sample, 93rd pctl)\nGreen=Proposed event combo')
        ax2.invert_yaxis()
        ax2.legend(fontsize=8)

    # 3. Walk-forward results for proposed combo
    ax3 = axes[1, 0]
    proposed_folds = walk_forward_validation(events, proposed_event_combo)
    if proposed_folds:
        months_labels = [f['test_month'] for f in proposed_folds]
        bl_rates = [f['baseline_hits']/max(f['baseline_n'],1)*100 for f in proposed_folds]
        sig_rates = [f['signal_hits']/max(f['signal_n'],1)*100 if f['signal_n'] > 0 else 0 for f in proposed_folds]
        sig_ns = [f['signal_n'] for f in proposed_folds]

        x = np.arange(len(months_labels))
        width = 0.35
        ax3.bar(x - width/2, bl_rates, width, label='Baseline', color='#BDBDBD')
        bars = ax3.bar(x + width/2, sig_rates, width, label='Signal (score>=3)', color='#4CAF50')
        for bar, n in zip(bars, sig_ns):
            if n > 0:
                ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                         f'n={n}', ha='center', fontsize=8)
        ax3.set_xticks(x)
        ax3.set_xticklabels(months_labels, rotation=45)
        ax3.set_ylabel('TP100 Rate (%)')
        ax3.set_title('Walk-Forward: Event Score (vol+z+vol_oi+mag)\nOut-of-sample monthly performance')
        ax3.legend()
        ax3.grid(axis='y', alpha=0.3)

    # 4. Summary text
    ax4 = axes[1, 1]
    ax4.axis('off')

    # Aggregate walk-forward stats
    if proposed_folds:
        total_sig_hits = sum(f['signal_hits'] for f in proposed_folds)
        total_sig_n = sum(f['signal_n'] for f in proposed_folds)
        total_bl_hits = sum(f['baseline_hits'] for f in proposed_folds)
        total_bl_n = sum(f['baseline_n'] for f in proposed_folds)
        oos_rate_str = fmt_ci(total_sig_hits, total_sig_n) if total_sig_n > 0 else "N/A"
        bl_rate_str = fmt_ci(total_bl_hits, total_bl_n)
        oos_lift = (total_sig_hits/total_sig_n) / (total_bl_hits/total_bl_n) if total_bl_hits > 0 and total_sig_n > 0 else 0
    else:
        oos_rate_str = "N/A"
        bl_rate_str = "N/A"
        oos_lift = 0

    summary_text = f"""
TWO-TIER VALIDATION RESULTS

DATA:
  Events: {len(events)} (one per symbol/date)
  Date range: {dates[0]} to {dates[-1]}
  Baseline TP100: {fmt_ci(hit_count, len(events))}

PROPOSED EVENT SCORING:
  volume_score + z_score + vol_oi_score + magnitude
  Threshold: 93rd percentile, score >= 3

WALK-FORWARD (out-of-sample):
  Signal TP100: {oos_rate_str}
  Baseline:     {bl_rate_str}
  Lift:         {oos_lift:.2f}x

METHODOLOGY:
  - One contract per event (recommended_option)
  - Walk-forward: train on earlier months, test later
  - Wilson confidence intervals
  - {n_combos} combinations tested

NOTE: Only walk-forward results are trustworthy.
In-sample results are for hypothesis generation only.
"""

    ax4.text(0.02, 0.98, summary_text, transform=ax4.transAxes, fontsize=9.5,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

    plt.tight_layout()
    plt.savefig('final_validation.png', dpi=150, bbox_inches='tight')
    print("Saved: final_validation.png")

    conn.close()
    print("\n" + "=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    main()
