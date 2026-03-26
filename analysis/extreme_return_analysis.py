#!/usr/bin/env python3
"""
Extreme Return & Insider Detection Analysis

Goal: Find the cheaters. Not option mechanics, not high-frequency moderate gains.
We want VERY FEW triggers that have a real chance at 500-1000%+ returns.

Key insight from rapid_return_analysis.py:
- At TP100, scoring factors barely differentiate (1.1-1.9x lift)
- At TP500+, the same signatures show 7-27x lift
- This means the insider-type filters DON'T improve moderate returns
  but MASSIVELY concentrate the extreme movers

Framework:
- Optimize for TP500+ / TP1000+, NOT TP100
- Target <5 events/week (not 5/day)
- Entry price is NOT a filter (that's mechanics)
- Evaluate expected value per trade, not just hit rate

EV math for binary options trades:
  TP500:  breakeven at >17% hit rate (risk $1 to make $5)
  TP1000: breakeven at >9% hit rate  (risk $1 to make $10)
  TP2000: breakeven at >5% hit rate  (risk $1 to make $20)
"""

import sys
import argparse
from math import sqrt
from collections import defaultdict

sys.path.append('.')
from database.core.connection import db
import psycopg2.extras
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def wilson_ci(hits, total, z=1.96):
    if total == 0:
        return 0.0, 0.0, 0.0
    p = hits / total
    denom = 1 + z**2 / total
    center = (p + z**2 / (2 * total)) / denom
    spread = z * sqrt((p * (1 - p) + z**2 / (4 * total)) / total) / denom
    return max(0, center - spread) * 100, center * 100, min(1, center + spread) * 100


def fmt_ci(hits, total):
    if total == 0:
        return "N/A (n=0)"
    lo, mid, hi = wilson_ci(hits, total)
    return f"{mid:.1f}% [{lo:.1f}-{hi:.1f}%] (n={total})"


def expected_value(hit_rate, tp_pct, avg_loss_pct=100):
    """EV per dollar risked. hit_rate as fraction, tp_pct/avg_loss_pct as percentage."""
    return hit_rate * (tp_pct / 100) - (1 - hit_rate) * (avg_loss_pct / 100)


def load_events(cur):
    """Load events with all factor scores."""
    cur.execute("""
        SELECT
            a.symbol, a.event_date, a.direction,
            a.recommended_option,
            a.total_score, a.volume_score, a.volume_oi_ratio_score,
            a.otm_score, a.directional_score, a.time_score,
            a.z_score, a.total_magnitude, a.total_volume,
            o.close_price AS entry_price,
            o.volume AS option_volume, o.open_interest,
            o.implied_volatility, o.greeks_delta, o.greeks_gamma,
            oc.expiration_date, oc.contract_type
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


def load_price_series(cur, events):
    """Load post-event price series using HIGH prices for intraday capture."""
    print(f"  Loading price series for {len(events)} events...")
    for i, ev in enumerate(events):
        if i % 500 == 0 and i > 0:
            print(f"    {i}/{len(events)}...")

        cur.execute("""
            SELECT date, close_price, high_price
            FROM daily_option_snapshot
            WHERE contract_ticker = %s
              AND date > %s
              AND date <= %s
            ORDER BY date
        """, (ev['recommended_option'], ev['event_date'], ev['expiration_date']))

        prices = []
        for row in cur.fetchall():
            prices.append({
                'date': row['date'],
                'close': float(row['close_price'] or 0),
                'high': float(row['high_price'] or 0),
            })

        entry = float(ev['entry_price'])
        ev['days_to_expiry'] = (ev['expiration_date'] - ev['event_date']).days

        if entry > 0 and prices:
            for window in [1, 2, 3, 5, 10]:
                window_highs = [p['high'] for p in prices[:window] if p['high'] > 0]
                if window_highs:
                    ev[f'max_return_{window}d'] = (max(window_highs) / entry - 1) * 100
                else:
                    ev[f'max_return_{window}d'] = -100.0

            all_highs = [p['high'] for p in prices if p['high'] > 0]
            if all_highs:
                ev['max_return_expiry'] = (max(all_highs) / entry - 1) * 100
            else:
                ev['max_return_expiry'] = -100.0
        else:
            for window in [1, 2, 3, 5, 10]:
                ev[f'max_return_{window}d'] = -100.0
            ev['max_return_expiry'] = -100.0

    return events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-events', type=int, default=0)
    args = parser.parse_args()

    conn = db.connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 90)
    print("EXTREME RETURN & INSIDER DETECTION ANALYSIS")
    print("=" * 90)
    print("\nObjective: Find the cheaters. Minimize triggers, maximize extreme returns.")
    print("Metric: TP500+ / TP1000+ hit rate and expected value per trade.")
    print("NOT optimizing for TP100 (that's option mechanics).")

    # Load
    events = load_events(cur)
    if args.max_events > 0:
        events = events[:args.max_events]
    print(f"\nTotal events: {len(events)}")

    events = load_price_series(cur, events)

    dates = sorted(set(d['event_date'] for d in events))
    weeks = len(dates) / 5.0
    print(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} trading days, ~{weeks:.1f} weeks)")

    # =========================================================================
    # SECTION 1: EXTREME RETURN BASELINE
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 1: HOW RARE ARE EXTREME RETURNS?")
    print("=" * 90)

    extreme_targets = [200, 300, 500, 750, 1000, 2000, 5000]
    windows = [1, 2, 3, 5, 'expiry']

    print(f"\n  {'Window':<12}", end="")
    for tp in extreme_targets:
        print(f"{'TP'+str(tp)+'%':>10}", end="")
    print()
    print("  " + "-" * (12 + 10 * len(extreme_targets)))

    baseline_rates = {}
    for window in windows:
        wlabel = f"{window}d" if window != 'expiry' else 'to expiry'
        print(f"  {wlabel:<12}", end="")
        for tp in extreme_targets:
            if window == 'expiry':
                hits = sum(1 for ev in events if ev.get('max_return_expiry', -100) >= tp)
            else:
                hits = sum(1 for ev in events if ev.get(f'max_return_{window}d', -100) >= tp)
            rate = hits / len(events) * 100
            baseline_rates[(tp, window)] = rate
            print(f"{rate:>9.1f}%", end="")
        print()

    print(f"\n  Event counts for key targets:")
    for tp in [500, 1000, 2000]:
        for w in [3, 5, 'expiry']:
            wlabel = f"{w}d" if w != 'expiry' else 'expiry'
            if w == 'expiry':
                hits = sum(1 for ev in events if ev.get('max_return_expiry', -100) >= tp)
            else:
                hits = sum(1 for ev in events if ev.get(f'max_return_{w}d', -100) >= tp)
            print(f"    TP{tp} @ {wlabel}: {hits} events ({hits/len(events)*100:.2f}%), "
                  f"~{hits/weeks:.1f}/week, ~{hits/len(dates):.1f}/day")

    # EV breakeven
    print(f"\n  Breakeven hit rates (assuming 100% loss on misses):")
    for tp in extreme_targets:
        be = 100.0 / (tp + 100) * 100
        print(f"    TP{tp}: >{be:.1f}% hit rate needed")

    # =========================================================================
    # SECTION 2: REVERSE ENGINEER EXTREME WINNERS
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 2: WHAT DO EXTREME WINNERS LOOK LIKE?")
    print("=" * 90)
    print("  These are the events we want to catch. What's their profile?")

    factor_arrays = {
        'z_score': np.array([float(d.get('z_score') or 0) for d in events]),
        'vol_oi_score': np.array([float(d.get('volume_oi_ratio_score') or 0) for d in events]),
        'volume_score': np.array([float(d.get('volume_score') or 0) for d in events]),
        'otm_score': np.array([float(d.get('otm_score') or 0) for d in events]),
        'directional_score': np.array([float(d.get('directional_score') or 0) for d in events]),
        'time_score': np.array([float(d.get('time_score') or 0) for d in events]),
        'total_score': np.array([float(d.get('total_score') or 0) for d in events]),
        'magnitude': np.array([float(d.get('total_magnitude') or 0) for d in events]),
    }

    dte_arr = np.array([ev['days_to_expiry'] for ev in events])
    entry_arr = np.array([float(ev['entry_price']) for ev in events])

    # Compute percentile thresholds
    thresholds = {}
    for name, arr in factor_arrays.items():
        valid = arr[arr > 0]
        if len(valid) > 0:
            thresholds[name] = {
                p: np.percentile(valid, p)
                for p in [80, 85, 90, 93, 95, 97, 99, 99.5]
            }

    for win_label, win_filter in [
        ("TP500 within 3d", lambda ev: ev.get('max_return_3d', -100) >= 500),
        ("TP500 within 5d", lambda ev: ev.get('max_return_5d', -100) >= 500),
        ("TP1000 within 5d", lambda ev: ev.get('max_return_5d', -100) >= 1000),
        ("TP1000 to expiry", lambda ev: ev.get('max_return_expiry', -100) >= 1000),
        ("TP2000 to expiry", lambda ev: ev.get('max_return_expiry', -100) >= 2000),
    ]:
        winners = [ev for ev in events if win_filter(ev)]
        print(f"\n  --- {win_label}: {len(winners)} events ({len(winners)/len(events)*100:.2f}%) ---")

        if len(winners) < 3:
            print("    Too few for reliable analysis")
            if winners:
                for w in winners:
                    print(f"    {w['symbol']} {w['event_date']} {w['direction']} "
                          f"z={float(w['z_score'] or 0):.2f} voloi={float(w['volume_oi_ratio_score'] or 0):.2f} "
                          f"dte={w['days_to_expiry']} entry=${float(w['entry_price']):.2f} "
                          f"ret={w.get('max_return_expiry', 0):.0f}%")
            continue

        print(f"    ~{len(winners)/weeks:.1f}/week, ~{len(winners)/len(dates):.1f}/day")

        # Profile comparison
        print(f"    {'Factor':<20} {'Winner Med':>12} {'All Med':>12} {'Ratio':>8} {'Win P25-P75':>24}")
        print("    " + "-" * 80)

        for name in ['z_score', 'vol_oi_score', 'volume_score', 'otm_score',
                      'directional_score', 'time_score', 'total_score', 'magnitude']:
            field = 'volume_oi_ratio_score' if name == 'vol_oi_score' else \
                    'total_magnitude' if name == 'magnitude' else name
            w_vals = np.array([float(ev.get(field) or 0) for ev in winners])
            a_vals = factor_arrays[name]
            a_med = np.median(a_vals[a_vals > 0]) if np.any(a_vals > 0) else 0
            w_med = np.median(w_vals)
            ratio = w_med / a_med if a_med > 0 else 0

            if name == 'magnitude':
                print(f"    {name:<20} ${w_med:>10,.0f} ${a_med:>10,.0f} {ratio:>7.2f}x "
                      f"${np.percentile(w_vals, 25):>10,.0f}-${np.percentile(w_vals, 75):>10,.0f}")
            else:
                print(f"    {name:<20} {w_med:>12.2f} {a_med:>12.2f} {ratio:>7.2f}x "
                      f"{np.percentile(w_vals, 25):>12.2f}-{np.percentile(w_vals, 75):>12.2f}")

        w_dte = np.array([ev['days_to_expiry'] for ev in winners])
        w_entry = np.array([float(ev['entry_price']) for ev in winners])
        print(f"    {'days_to_expiry':<20} {np.median(w_dte):>12.0f} {np.median(dte_arr):>12.0f} "
              f"{np.median(w_dte)/np.median(dte_arr) if np.median(dte_arr) > 0 else 0:>7.2f}x "
              f"{np.percentile(w_dte, 25):>12.0f}-{np.percentile(w_dte, 75):>12.0f}")
        print(f"    {'entry_price':<20} ${np.median(w_entry):>11.2f} ${np.median(entry_arr):>11.2f} "
              f"{np.median(w_entry)/np.median(entry_arr) if np.median(entry_arr) > 0 else 0:>7.2f}x "
              f"${np.percentile(w_entry, 25):>11.2f}-${np.percentile(w_entry, 75):>11.2f}")

        # Percentile positions
        print(f"\n    Winner median percentile in overall population:")
        for name in ['z_score', 'vol_oi_score', 'volume_score', 'otm_score',
                      'directional_score', 'time_score', 'total_score']:
            field = 'volume_oi_ratio_score' if name == 'vol_oi_score' else name
            w_vals = [float(ev.get(field) or 0) for ev in winners]
            w_med = np.median(w_vals)
            pctl = np.searchsorted(np.sort(factor_arrays[name]), w_med) / len(events) * 100
            print(f"      {name:<20} median={w_med:.2f} -> P{pctl:.0f}")

        # Direction breakdown
        calls = sum(1 for ev in winners if ev['direction'] == 'call_heavy')
        puts = sum(1 for ev in winners if ev['direction'] == 'put_heavy')
        print(f"\n    Direction: {calls} bullish ({calls/len(winners)*100:.0f}%), "
              f"{puts} bearish ({puts/len(winners)*100:.0f}%)")

        # List some examples if small enough
        if len(winners) <= 30:
            print(f"\n    Individual events:")
            for w in sorted(winners, key=lambda x: -x.get('max_return_expiry', 0)):
                ret_3d = w.get('max_return_3d', -100)
                ret_5d = w.get('max_return_5d', -100)
                ret_exp = w.get('max_return_expiry', -100)
                print(f"      {w['symbol']:<6} {w['event_date']} {w['direction']:<11} "
                      f"z={float(w['z_score'] or 0):>6.2f} voloi={float(w['volume_oi_ratio_score'] or 0):>5.2f} "
                      f"otm={float(w['otm_score'] or 0):>5.2f} dir={float(w['directional_score'] or 0):>5.2f} "
                      f"dte={w['days_to_expiry']:>3} entry=${float(w['entry_price']):>5.2f} "
                      f"3d={ret_3d:>+7.0f}% 5d={ret_5d:>+7.0f}% exp={ret_exp:>+7.0f}%")

    # =========================================================================
    # SECTION 3: INDIVIDUAL FACTOR LIFT AT EXTREME TARGETS
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 3: WHICH INDIVIDUAL FACTORS CONCENTRATE EXTREME RETURNS?")
    print("=" * 90)
    print("  Testing each factor at various percentile cutoffs.")
    print("  Metric: lift at TP500@5d and TP1000@expiry\n")

    bl_tp500_5d = sum(1 for ev in events if ev.get('max_return_5d', -100) >= 500) / len(events) * 100
    bl_tp1000_exp = sum(1 for ev in events if ev.get('max_return_expiry', -100) >= 1000) / len(events) * 100

    print(f"  Baselines: TP500@5d = {bl_tp500_5d:.2f}%, TP1000@expiry = {bl_tp1000_exp:.2f}%")

    factor_tests = [
        ('z_score', 'z_score', [90, 95, 97, 99, 99.5]),
        ('vol_oi_score', 'volume_oi_ratio_score', [80, 90, 95, 97]),
        ('volume_score', 'volume_score', [80, 90, 95, 97]),
        ('otm_score', 'otm_score', [80, 90, 95]),
        ('directional_score', 'directional_score', [80, 90, 95]),
        ('time_score', 'time_score', [80, 90, 95]),
        ('total_score', 'total_score', [80, 90, 95, 97]),
    ]

    # DTE as a special factor (lower = more extreme)
    dte_tests = [30, 21, 14, 7, 5, 3]

    print(f"\n  {'Factor':<20} {'Pctl':>6} {'Thresh':>10} {'n':>6} "
          f"{'TP500@5d':>10} {'Lift':>8} {'TP1000exp':>10} {'Lift':>8}")
    print("  " + "-" * 85)

    for name, field, pctls in factor_tests:
        if name not in thresholds:
            continue
        for pctl in pctls:
            thresh = thresholds[name][pctl]
            filtered = [ev for ev in events if float(ev.get(field) or 0) >= thresh]
            n = len(filtered)
            if n < 5:
                continue
            tp500 = sum(1 for ev in filtered if ev.get('max_return_5d', -100) >= 500) / n * 100
            tp1000 = sum(1 for ev in filtered if ev.get('max_return_expiry', -100) >= 1000) / n * 100
            lift500 = tp500 / bl_tp500_5d if bl_tp500_5d > 0 else 0
            lift1000 = tp1000 / bl_tp1000_exp if bl_tp1000_exp > 0 else 0
            marker = " ***" if lift500 > 2 or lift1000 > 2 else ""
            if name == 'magnitude':
                print(f"  {name:<20} P{pctl:<5} ${thresh:>8,.0f} {n:>6} "
                      f"{tp500:>9.2f}% {lift500:>7.2f}x {tp1000:>9.2f}% {lift1000:>7.2f}x{marker}")
            else:
                print(f"  {name:<20} P{pctl:<5} {thresh:>10.2f} {n:>6} "
                      f"{tp500:>9.2f}% {lift500:>7.2f}x {tp1000:>9.2f}% {lift1000:>7.2f}x{marker}")

    # DTE tests
    print()
    for dte in dte_tests:
        filtered = [ev for ev in events if ev['days_to_expiry'] <= dte]
        n = len(filtered)
        if n < 5:
            continue
        tp500 = sum(1 for ev in filtered if ev.get('max_return_5d', -100) >= 500) / n * 100
        tp1000 = sum(1 for ev in filtered if ev.get('max_return_expiry', -100) >= 1000) / n * 100
        lift500 = tp500 / bl_tp500_5d if bl_tp500_5d > 0 else 0
        lift1000 = tp1000 / bl_tp1000_exp if bl_tp1000_exp > 0 else 0
        marker = " ***" if lift500 > 2 or lift1000 > 2 else ""
        print(f"  {'DTE':<20} <={dte:<5} {'':>10} {n:>6} "
              f"{tp500:>9.2f}% {lift500:>7.2f}x {tp1000:>9.2f}% {lift1000:>7.2f}x{marker}")

    # =========================================================================
    # SECTION 4: INSIDER DETECTION SIGNATURES (optimized for extreme returns)
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 4: INSIDER DETECTION SIGNATURES")
    print("=" * 90)
    print("  Ultra-narrow filters targeting <5 events/week.")
    print("  Optimized for TP500+/TP1000+ (catching cheaters, not mechanics).\n")

    def make_filter(criteria):
        def filt(ev):
            for check in criteria.values():
                if not check(ev):
                    return False
            return True
        return filt

    signatures = {}

    # ---- Anomaly-driven (the volume is screaming) ----

    signatures['Extreme Anomaly: z99+volOI95'] = make_filter({
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][95],
    })

    signatures['Ultra Anomaly: z99.5+volOI95'] = make_filter({
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99.5],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][95],
    })

    signatures['Fresh Flood: volOI97+z97'] = make_filter({
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][97],
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][97],
    })

    # ---- Time-urgency (they know WHEN) ----

    signatures['Ticking Bomb: DTE<=7+z99+volOI90'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 7,
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
    })

    signatures['Last Minute: DTE<=5+z97+volOI90'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 5,
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][97],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
    })

    signatures['Day-Before: DTE<=3+z95+volOI90'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 3,
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][95],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
    })

    # ---- OTM + urgency (max leverage play) ----

    signatures['Informed Bet: DTE<=7+z99+volOI90+otm90'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 7,
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
    })

    signatures['Insider Special: DTE<=14+z99+volOI95+otm90'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 14,
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][95],
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
    })

    # ---- Pure volume extremity (simplest signal) ----

    signatures['Volume Screamer: z99.5+DTE<=14'] = make_filter({
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99.5],
        'dte': lambda ev: ev['days_to_expiry'] <= 14,
    })

    signatures['Volume Screamer Short: z99.5+DTE<=7'] = make_filter({
        'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99.5],
        'dte': lambda ev: ev['days_to_expiry'] <= 7,
    })

    # ---- Composite extremity score ----
    # Rank every event by percentile across multiple factors, sum percentile ranks

    print("  Computing composite extremity scores...")
    for ev in events:
        z_pctl = np.searchsorted(np.sort(factor_arrays['z_score']),
                                  float(ev['z_score'] or 0)) / len(events)
        voi_pctl = np.searchsorted(np.sort(factor_arrays['vol_oi_score']),
                                    float(ev['volume_oi_ratio_score'] or 0)) / len(events)
        time_pctl = 1.0 - (ev['days_to_expiry'] / max(dte_arr))  # lower DTE = higher score
        otm_pctl = np.searchsorted(np.sort(factor_arrays['otm_score']),
                                    float(ev['otm_score'] or 0)) / len(events)

        ev['insider_composite'] = z_pctl + voi_pctl + time_pctl + otm_pctl

    composite_arr = np.array([ev['insider_composite'] for ev in events])
    comp_thresholds = {p: np.percentile(composite_arr, p) for p in [95, 97, 99, 99.5]}

    signatures['Composite P99: top 1% insider score'] = lambda ev: ev['insider_composite'] >= comp_thresholds[99]
    signatures['Composite P99.5: top 0.5% insider score'] = lambda ev: ev['insider_composite'] >= comp_thresholds[99.5]
    signatures['Composite P99+DTE<=7'] = lambda ev: (
        ev['insider_composite'] >= comp_thresholds[99] and ev['days_to_expiry'] <= 7
    )

    # ---- Run all signatures ----
    tp_targets = [200, 300, 500, 750, 1000, 2000]
    eval_windows = [1, 3, 5, 'expiry']

    sig_summary = []

    for sig_name, sig_filter in signatures.items():
        if callable(sig_filter) and not isinstance(sig_filter, type(lambda: None)):
            # It's a make_filter result
            filtered = [ev for ev in events if sig_filter(ev)]
        else:
            filtered = [ev for ev in events if sig_filter(ev)]
        n = len(filtered)

        if n < 3:
            print(f"\n  === {sig_name} === n={n} (too few, skipping)")
            sig_summary.append((sig_name, n, 0, {}, {}))
            continue

        epw = n / weeks
        epd = n / len(dates)

        print(f"\n  === {sig_name} ===")
        print(f"  n={n} ({n/len(events)*100:.2f}%), ~{epw:.1f}/week, ~{epd:.1f}/day")

        # Hit rate matrix
        header = f"    {'Window':<12}" + "".join(f"{'TP'+str(tp)+'%':>10}" for tp in tp_targets)
        print(header)
        print("    " + "-" * (12 + 10 * len(tp_targets)))

        hit_rates = {}
        for window in eval_windows:
            wlabel = f"{window}d" if window != 'expiry' else 'to expiry'
            row = f"    {wlabel:<12}"
            for tp in tp_targets:
                if window == 'expiry':
                    hits = sum(1 for ev in filtered if ev.get('max_return_expiry', -100) >= tp)
                else:
                    hits = sum(1 for ev in filtered if ev.get(f'max_return_{window}d', -100) >= tp)
                rate = hits / n * 100
                hit_rates[(tp, window)] = rate
                row += f"{rate:>9.1f}%"
            print(row)

        # Lift vs baseline
        print(f"    Lift vs baseline:")
        lifts = {}
        for window in eval_windows:
            wlabel = f"{window}d" if window != 'expiry' else 'to expiry'
            row = f"    {wlabel:<12}"
            for tp in tp_targets:
                bl = baseline_rates.get((tp, window), 0)
                sig = hit_rates.get((tp, window), 0)
                lift = sig / bl if bl > 0 else 0
                lifts[(tp, window)] = lift
                row += f"{lift:>9.2f}x"
            print(row)

        # Expected value at key targets
        print(f"    Expected value per $1 risked (assuming 100% loss on miss):")
        for tp in [500, 1000, 2000]:
            for w in [3, 5, 'expiry']:
                wlabel = f"{w}d" if w != 'expiry' else 'exp'
                rate = hit_rates.get((tp, w), 0) / 100
                ev_val = expected_value(rate, tp)
                status = "POSITIVE" if ev_val > 0 else "negative"
                ci = fmt_ci(int(rate * n), n)
                if ev_val > 0:
                    print(f"    >>> TP{tp}@{wlabel}: EV = ${ev_val:+.2f} ({status}) | {ci}")
                elif rate > 0:
                    print(f"        TP{tp}@{wlabel}: EV = ${ev_val:+.2f} ({status}) | {ci}")

        sig_summary.append((sig_name, n, epw, hit_rates, lifts))
        print()

    # =========================================================================
    # SECTION 5: FINAL RANKING — EV-OPTIMAL SIGNATURES
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 5: FINAL RANKING — WHICH SIGNATURES ARE +EV?")
    print("=" * 90)

    print(f"\n  Ranked by best expected value across any TP/window combination")
    print(f"  Only showing signatures with n >= 5\n")

    ranking_data = []
    for sig_name, n, epw, hit_rates, lifts in sig_summary:
        if n < 5 or not hit_rates:
            continue

        best_ev = -999
        best_combo = ""
        for tp in [500, 1000, 2000]:
            for w in [3, 5, 'expiry']:
                rate = hit_rates.get((tp, w), 0) / 100
                ev_val = expected_value(rate, tp)
                if ev_val > best_ev:
                    best_ev = ev_val
                    wlabel = f"{w}d" if w != 'expiry' else 'exp'
                    best_combo = f"TP{tp}@{wlabel}"

        tp500_5d = hit_rates.get((500, 5), 0)
        tp1000_exp = hit_rates.get((1000, 'expiry'), 0)
        ranking_data.append((sig_name, n, epw, best_ev, best_combo, tp500_5d, tp1000_exp))

    ranking_data.sort(key=lambda x: -x[3])

    print(f"  {'Rank':<5} {'Signature':<45} {'n':>5} {'/wk':>5} {'Best EV':>10} {'@':>12} "
          f"{'TP500@5d':>10} {'TP1000exp':>10}")
    print("  " + "-" * 110)

    for i, (name, n, epw, ev, combo, tp500, tp1000) in enumerate(ranking_data, 1):
        short = name[:42]
        ev_marker = " <<<" if ev > 0 else ""
        print(f"  {i:<5} {short:<45} {n:>5} {epw:>4.1f} ${ev:>+8.2f} {combo:>12} "
              f"{tp500:>9.1f}% {tp1000:>9.1f}%{ev_marker}")

    # Count positive EV signatures
    pos_ev = [r for r in ranking_data if r[3] > 0]
    print(f"\n  {len(pos_ev)} signature(s) with positive expected value")

    if pos_ev:
        print(f"\n  +EV SIGNATURES:")
        for name, n, epw, ev, combo, tp500, tp1000 in pos_ev:
            print(f"    {name}")
            print(f"      n={n}, ~{epw:.1f}/week, Best: {combo} -> EV=${ev:+.2f}/dollar")
            print()

    # =========================================================================
    # VISUALIZATION
    # =========================================================================
    print("Creating visualization...")

    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle('Extreme Return & Insider Detection Analysis\n'
                 'Optimizing for TP500+/TP1000+ — Finding the cheaters',
                 fontsize=14, fontweight='bold')

    # 1. Extreme return baseline heatmap
    ax1 = axes[0, 0]
    matrix = np.zeros((len(windows), len(extreme_targets)))
    for i, w in enumerate(windows):
        for j, tp in enumerate(extreme_targets):
            matrix[i, j] = baseline_rates.get((tp, w), 0)

    im = ax1.imshow(matrix, cmap='YlOrRd', aspect='auto')
    ax1.set_xticks(range(len(extreme_targets)))
    ax1.set_xticklabels([f'TP{tp}%' for tp in extreme_targets], fontsize=8)
    ax1.set_yticks(range(len(windows)))
    ax1.set_yticklabels([f'{w}d' if w != 'expiry' else 'Expiry' for w in windows])
    for i in range(len(windows)):
        for j in range(len(extreme_targets)):
            color = 'white' if matrix[i, j] > 5 or matrix[i, j] < 0.5 else 'black'
            ax1.text(j, i, f'{matrix[i, j]:.1f}%', ha='center', va='center',
                     fontsize=9, fontweight='bold', color=color)
    ax1.set_title(f'Baseline Extreme Return Rates (n={len(events)})', fontsize=11)
    plt.colorbar(im, ax=ax1, shrink=0.8)

    # 2. Signature EV ranking
    ax2 = axes[0, 1]
    if ranking_data:
        top_n = min(12, len(ranking_data))
        names = [r[0][:30] for r in ranking_data[:top_n]]
        evs = [r[3] for r in ranking_data[:top_n]]
        counts = [r[1] for r in ranking_data[:top_n]]

        colors = ['#4CAF50' if e > 0 else '#F44336' for e in evs]
        y_pos = np.arange(top_n)
        bars = ax2.barh(y_pos, evs, color=colors, alpha=0.8)
        ax2.axvline(0, color='black', linewidth=1)

        for i, (n, ev) in enumerate(zip(counts, evs)):
            ax2.text(max(ev, 0) + 0.02, i, f'n={n}', va='center', fontsize=8)

        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(names, fontsize=8)
        ax2.set_xlabel('Expected Value ($ per $1 risked)')
        ax2.set_title('Signature Ranking by Best Expected Value', fontsize=11)
        ax2.invert_yaxis()

    # 3. Signature lift at TP500@5d vs TP1000@expiry
    ax3 = axes[1, 0]
    if ranking_data:
        top_sigs = [r for r in ranking_data if r[1] >= 5][:10]
        names = [r[0][:25] for r in top_sigs]
        tp500_rates = [r[5] for r in top_sigs]
        tp1000_rates = [r[6] for r in top_sigs]

        y_pos = np.arange(len(top_sigs))
        ax3.barh(y_pos - 0.15, tp500_rates, 0.3, label='TP500 @ 5d', color='#FF9800', alpha=0.9)
        ax3.barh(y_pos + 0.15, tp1000_rates, 0.3, label='TP1000 @ expiry', color='#9C27B0', alpha=0.9)

        # Breakeven lines
        ax3.axvline(100 / 6, color='#FF9800', linestyle='--', alpha=0.5, label='TP500 breakeven (17%)')
        ax3.axvline(100 / 11, color='#9C27B0', linestyle='--', alpha=0.5, label='TP1000 breakeven (9%)')

        ax3.set_yticks(y_pos)
        ax3.set_yticklabels(names, fontsize=8)
        ax3.set_xlabel('Hit Rate (%)')
        ax3.set_title('Extreme Return Rates by Signature', fontsize=11)
        ax3.invert_yaxis()
        ax3.legend(fontsize=7, loc='lower right')

    # 4. Summary
    ax4 = axes[1, 1]
    ax4.axis('off')

    summary = f"""INSIDER DETECTION RESULTS

DATA: {len(events)} events, {len(dates)} trading days
Range: {dates[0]} to {dates[-1]}

BASELINE EXTREME RETURNS:
  TP500  @ 5d:      {bl_tp500_5d:.2f}%
  TP1000 @ expiry:  {bl_tp1000_exp:.2f}%

EV BREAKEVEN (100% loss on miss):
  TP500:  >17% hit rate
  TP1000: >9% hit rate
  TP2000: >5% hit rate
"""

    if pos_ev:
        summary += f"\n+EV SIGNATURES FOUND: {len(pos_ev)}\n"
        for name, n, epw, ev, combo, _, _ in pos_ev[:3]:
            summary += f"  {name[:35]}\n"
            summary += f"    n={n}, {epw:.1f}/wk, {combo} EV=${ev:+.2f}\n"
    else:
        summary += "\nNO +EV SIGNATURES FOUND\n"
        summary += "with current data and thresholds.\n"

    summary += f"""
INTERPRETATION:
If +EV signatures exist, implement them
as ultra-selective production filters.
If not, the signal-to-noise ratio in
public options flow may be insufficient
for this specific use case.
"""

    ax4.text(0.02, 0.98, summary, transform=ax4.transAxes, fontsize=9.5,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

    plt.tight_layout()
    plt.savefig('extreme_return_analysis.png', dpi=150, bbox_inches='tight')
    print("Saved: extreme_return_analysis.png")

    conn.close()
    print("\n" + "=" * 90)
    print("ANALYSIS COMPLETE")
    print("=" * 90)


if __name__ == '__main__':
    main()
