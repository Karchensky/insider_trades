#!/usr/bin/env python3
"""
Enriched Signal Analysis

Tests whether enrichment features improve signal quality.
Focuses on features computable from existing DB data:
1. Symbol novelty (first-time vs. frequent triggers)
2. Put vs. call direction (bearish had better directional accuracy)
3. Flow concentration (one contract vs. spread across many)
4. Stock price level (small-cap proxy)
5. Combined "insider profile" signatures

Then samples a subset of events to test Polygon news + EDGAR enrichment.
"""

import sys
import os
from math import sqrt
from collections import defaultdict
from datetime import timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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


def load_events(cur):
    """Load events with option outcomes and metadata for enrichment analysis."""
    cur.execute("""
        SELECT
            a.symbol, a.event_date, a.direction,
            a.recommended_option,
            a.total_score, a.volume_score, a.volume_oi_ratio_score,
            a.otm_score, a.directional_score, a.time_score,
            a.z_score, a.total_magnitude, a.total_volume,
            o.close_price AS option_entry_price,
            o.high_price AS option_entry_high,
            o.volume AS option_volume, o.open_interest,
            oc.expiration_date, oc.strike_price,
            -- Stock price for small-cap proxy
            s.close AS stock_price,
            s.trading_volume AS stock_volume
        FROM daily_anomaly_snapshot a
        INNER JOIN daily_option_snapshot o
            ON a.recommended_option = o.contract_ticker
            AND a.event_date = o.date
        INNER JOIN option_contracts oc
            ON a.recommended_option = oc.contract_ticker
        LEFT JOIN daily_stock_snapshot s
            ON a.symbol = s.symbol AND a.event_date = s.date
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


def compute_novelty(events):
    """Compute trigger novelty for each event from the event list itself."""
    # Group by symbol, sorted by date
    symbol_dates = defaultdict(list)
    for ev in events:
        symbol_dates[ev['symbol']].append(ev['event_date'])

    for ev in events:
        symbol = ev['symbol']
        event_date = ev['event_date']
        all_dates = symbol_dates[symbol]

        # Count prior triggers
        prior_30d = [d for d in all_dates
                     if event_date - timedelta(days=30) <= d < event_date]
        prior_7d = [d for d in all_dates
                    if event_date - timedelta(days=7) <= d < event_date]
        prior_all = [d for d in all_dates if d < event_date]

        ev['trigger_count_30d'] = len(prior_30d)
        ev['trigger_count_7d'] = len(prior_7d)
        ev['is_first_trigger'] = len(prior_all) == 0
        ev['total_prior_triggers'] = len(prior_all)

        # Novelty score
        count_30d = len(prior_30d)
        if len(prior_all) == 0:
            ev['novelty_score'] = 1.0
        elif count_30d == 0:
            ev['novelty_score'] = 0.9
        elif count_30d <= 2:
            ev['novelty_score'] = 0.7
        elif count_30d <= 5:
            ev['novelty_score'] = 0.5
        elif count_30d <= 10:
            ev['novelty_score'] = 0.3
        else:
            ev['novelty_score'] = max(0.1, 1.0 - count_30d / 30.0)


def compute_concentration(cur, events):
    """Compute flow concentration proxy using recommended option volume vs total."""
    for ev in events:
        rec_vol = int(ev.get('option_volume') or 0)
        total_vol = int(ev.get('total_volume') or 0)
        if total_vol > 0 and rec_vol > 0:
            ev['rec_vol_share'] = rec_vol / total_vol
        else:
            ev['rec_vol_share'] = None
        # Use vol:OI as concentration proxy (high vol:OI = fresh, concentrated)
        ev['contract_count'] = 1  # Not available without extra query


def load_option_outcomes(cur, events):
    """Load option outcomes for each event."""
    print(f"  Loading option outcomes for {len(events)} events...")
    for i, ev in enumerate(events):
        if i % 2000 == 0 and i > 0:
            print(f"    {i}/{len(events)}...")

        cur.execute("""
            SELECT date, close_price, high_price
            FROM daily_option_snapshot
            WHERE contract_ticker = %s
              AND date > %s
              AND date <= %s
            ORDER BY date
        """, (ev['recommended_option'], ev['event_date'], ev['expiration_date']))

        entry = float(ev['option_entry_price'])
        ev['days_to_expiry'] = (ev['expiration_date'] - ev['event_date']).days

        max_return_3d = 0
        max_return_exp = 0
        max_return_high_3d = 0
        max_return_high_exp = 0

        for j, row in enumerate(cur.fetchall()):
            close = float(row['close_price']) if row['close_price'] else 0
            high = float(row['high_price']) if row['high_price'] else close

            if entry > 0:
                ret_close = (close / entry - 1) * 100
                ret_high = (high / entry - 1) * 100
            else:
                ret_close = 0
                ret_high = 0

            max_return_exp = max(max_return_exp, ret_close)
            max_return_high_exp = max(max_return_high_exp, ret_high)

            if j < 3:
                max_return_3d = max(max_return_3d, ret_close)
                max_return_high_3d = max(max_return_high_3d, ret_high)

        ev['max_return_3d'] = max_return_3d
        ev['max_return_exp'] = max_return_exp
        ev['max_return_high_3d'] = max_return_high_3d
        ev['max_return_high_exp'] = max_return_high_exp

        # TP flags
        for target in [100, 200, 500, 1000]:
            ev[f'tp{target}_3d'] = max_return_high_3d >= target
            ev[f'tp{target}_exp'] = max_return_high_exp >= target


def analyze_feature(events, feature_name, filter_fn, label=""):
    """Analyze TP rates for a filtered subset."""
    subset = [ev for ev in events if filter_fn(ev)]
    if not subset:
        return None

    results = {}
    for target in [100, 200, 500, 1000]:
        for window in ['3d', 'exp']:
            key = f'tp{target}_{window}'
            hits = sum(1 for ev in subset if ev.get(key, False))
            results[key] = {'hits': hits, 'total': len(subset),
                            'rate': hits / len(subset) * 100 if subset else 0}

    return {
        'label': label,
        'n': len(subset),
        'results': results,
    }


def print_section(title):
    print(f"\n{'='*90}")
    print(f"{title}")
    print(f"{'='*90}")


def main():
    print("=" * 90)
    print("ENRICHED SIGNAL ANALYSIS")
    print("=" * 90)
    print("\nTesting whether enrichment features improve signal quality.")
    print("Focus: low frequency / high conviction insider trading detection.\n")

    conn = db.connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Load events
    events = load_events(cur)
    print(f"Loaded {len(events)} events")

    if not events:
        print("No events found!")
        return

    # Compute enrichment features
    print("Computing novelty scores...")
    compute_novelty(events)

    print("Computing flow concentration...")
    compute_concentration(cur, events)

    # Load option outcomes
    load_option_outcomes(cur, events)

    date_range = f"{events[0]['event_date']} to {events[-1]['event_date']}"
    n_days = (events[-1]['event_date'] - events[0]['event_date']).days
    print(f"\nDate range: {date_range} ({n_days} days)")

    # === BASELINE ===
    print_section("SECTION 1: BASELINE")
    for target in [100, 200, 500, 1000]:
        for window in ['3d', 'exp']:
            hits = sum(1 for ev in events if ev.get(f'tp{target}_{window}', False))
            print(f"  TP{target}@{window}: {fmt_ci(hits, len(events))}")

    # === SECTION 2: SYMBOL NOVELTY ===
    print_section("SECTION 2: SYMBOL NOVELTY")
    print("  Does first-time/rare trigger predict better outcomes?")
    print()

    novelty_bins = [
        ("First trigger ever", lambda ev: ev.get('is_first_trigger', False)),
        ("Novelty >= 0.9 (rare)", lambda ev: ev.get('novelty_score', 0) >= 0.9),
        ("Novelty >= 0.7", lambda ev: ev.get('novelty_score', 0) >= 0.7),
        ("Novelty 0.3-0.7 (moderate)", lambda ev: 0.3 <= ev.get('novelty_score', 0) < 0.7),
        ("Novelty < 0.3 (frequent)", lambda ev: ev.get('novelty_score', 0) < 0.3),
        ("0 triggers in 30d", lambda ev: ev.get('trigger_count_30d', 0) == 0),
        ("1-2 triggers in 30d", lambda ev: 1 <= ev.get('trigger_count_30d', 0) <= 2),
        ("3-5 triggers in 30d", lambda ev: 3 <= ev.get('trigger_count_30d', 0) <= 5),
        ("10+ triggers in 30d", lambda ev: ev.get('trigger_count_30d', 0) >= 10),
        ("20+ triggers in 30d", lambda ev: ev.get('trigger_count_30d', 0) >= 20),
    ]

    baseline_tp100_exp = sum(1 for ev in events if ev.get('tp100_exp', False)) / len(events) * 100
    baseline_tp500_exp = sum(1 for ev in events if ev.get('tp500_exp', False)) / len(events) * 100

    print(f"  {'Filter':<35} {'n':>6}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@exp':>10}  {'TP1000@exp':>10}  {'Lift100':>8}")
    print(f"  {'-'*110}")

    for label, filter_fn in novelty_bins:
        r = analyze_feature(events, 'novelty', filter_fn, label)
        if r and r['n'] >= 10:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            lift = tp100_exp / baseline_tp100_exp if baseline_tp100_exp > 0 else 0
            print(f"  {label:<35} {r['n']:>6}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%  {lift:>7.2f}x")

    # === SECTION 3: PUT VS CALL ===
    print_section("SECTION 3: PUT-HEAVY VS CALL-HEAVY")
    print("  Put-heavy direction had 55.1% accuracy vs 43.6% for calls.")
    print("  Do put-heavy anomalies produce better option returns?")
    print()

    direction_bins = [
        ("Call-heavy (bullish)", lambda ev: ev.get('direction') == 'call_heavy'),
        ("Put-heavy (bearish)", lambda ev: ev.get('direction') == 'put_heavy'),
    ]

    print(f"  {'Direction':<35} {'n':>6}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@exp':>10}  {'TP1000@exp':>10}  {'Lift100':>8}")
    print(f"  {'-'*110}")

    for label, filter_fn in direction_bins:
        r = analyze_feature(events, 'direction', filter_fn, label)
        if r and r['n'] >= 10:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            lift = tp100_exp / baseline_tp100_exp if baseline_tp100_exp > 0 else 0
            print(f"  {label:<35} {r['n']:>6}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%  {lift:>7.2f}x")

    # === SECTION 4: STOCK PRICE (SMALL CAP PROXY) ===
    print_section("SECTION 4: STOCK PRICE LEVEL (SMALL-CAP PROXY)")
    print("  Insider trading more common on small/mid caps?")
    print()

    price_bins = [
        ("Stock < $10", lambda ev: ev.get('stock_price') and float(ev['stock_price']) < 10),
        ("Stock $10-25", lambda ev: ev.get('stock_price') and 10 <= float(ev['stock_price']) < 25),
        ("Stock $25-50", lambda ev: ev.get('stock_price') and 25 <= float(ev['stock_price']) < 50),
        ("Stock $50-100", lambda ev: ev.get('stock_price') and 50 <= float(ev['stock_price']) < 100),
        ("Stock $100-500", lambda ev: ev.get('stock_price') and 100 <= float(ev['stock_price']) < 500),
        ("Stock > $500", lambda ev: ev.get('stock_price') and float(ev['stock_price']) >= 500),
    ]

    print(f"  {'Price Bin':<35} {'n':>6}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@exp':>10}  {'TP1000@exp':>10}  {'Lift100':>8}")
    print(f"  {'-'*110}")

    for label, filter_fn in price_bins:
        r = analyze_feature(events, 'price', filter_fn, label)
        if r and r['n'] >= 10:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            lift = tp100_exp / baseline_tp100_exp if baseline_tp100_exp > 0 else 0
            print(f"  {label:<35} {r['n']:>6}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%  {lift:>7.2f}x")

    # === SECTION 5: FLOW CONCENTRATION ===
    print_section("SECTION 5: FLOW CONCENTRATION")
    print("  Is flow concentrated in one contract (sniper) or spread across many?")
    print()

    conc_bins = [
        ("1 contract only", lambda ev: ev.get('contract_count', 1) == 1),
        ("2-3 contracts", lambda ev: 2 <= ev.get('contract_count', 1) <= 3),
        ("4-5 contracts", lambda ev: 4 <= ev.get('contract_count', 1) <= 5),
        ("6+ contracts", lambda ev: ev.get('contract_count', 1) >= 6),
        ("Rec option >80% of flow", lambda ev: ev.get('rec_vol_share') and ev['rec_vol_share'] > 0.8),
        ("Rec option >50% of flow", lambda ev: ev.get('rec_vol_share') and ev['rec_vol_share'] > 0.5),
        ("Rec option <20% of flow", lambda ev: ev.get('rec_vol_share') and ev['rec_vol_share'] < 0.2),
    ]

    print(f"  {'Concentration':<35} {'n':>6}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@exp':>10}  {'TP1000@exp':>10}  {'Lift100':>8}")
    print(f"  {'-'*110}")

    for label, filter_fn in conc_bins:
        r = analyze_feature(events, 'concentration', filter_fn, label)
        if r and r['n'] >= 10:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            lift = tp100_exp / baseline_tp100_exp if baseline_tp100_exp > 0 else 0
            print(f"  {label:<35} {r['n']:>6}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%  {lift:>7.2f}x")

    # === SECTION 6: DTE ANALYSIS ===
    print_section("SECTION 6: DAYS TO EXPIRY")
    print("  Short-dated options = more conviction (time pressure on insider)")
    print()

    dte_bins = [
        ("DTE <= 3", lambda ev: ev.get('days_to_expiry', 999) <= 3),
        ("DTE 4-7", lambda ev: 4 <= ev.get('days_to_expiry', 999) <= 7),
        ("DTE 8-14", lambda ev: 8 <= ev.get('days_to_expiry', 999) <= 14),
        ("DTE 15-30", lambda ev: 15 <= ev.get('days_to_expiry', 999) <= 30),
        ("DTE > 30", lambda ev: ev.get('days_to_expiry', 999) > 30),
    ]

    print(f"  {'DTE Bin':<35} {'n':>6}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@exp':>10}  {'TP1000@exp':>10}  {'Lift100':>8}")
    print(f"  {'-'*110}")

    for label, filter_fn in dte_bins:
        r = analyze_feature(events, 'dte', filter_fn, label)
        if r and r['n'] >= 10:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            lift = tp100_exp / baseline_tp100_exp if baseline_tp100_exp > 0 else 0
            print(f"  {label:<35} {r['n']:>6}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%  {lift:>7.2f}x")

    # === SECTION 7: ENTRY PRICE ===
    print_section("SECTION 7: OPTION ENTRY PRICE")
    print("  Cheap options have more leverage but more theta risk")
    print()

    price_entry_bins = [
        ("Entry $0.05-0.20", lambda ev: 0.05 <= float(ev['option_entry_price']) < 0.20),
        ("Entry $0.20-0.50", lambda ev: 0.20 <= float(ev['option_entry_price']) < 0.50),
        ("Entry $0.50-1.00", lambda ev: 0.50 <= float(ev['option_entry_price']) < 1.00),
        ("Entry $1.00-2.00", lambda ev: 1.00 <= float(ev['option_entry_price']) < 2.00),
        ("Entry $2.00-5.00", lambda ev: 2.00 <= float(ev['option_entry_price']) <= 5.00),
    ]

    print(f"  {'Entry Price':<35} {'n':>6}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@exp':>10}  {'TP1000@exp':>10}  {'Lift100':>8}")
    print(f"  {'-'*110}")

    for label, filter_fn in price_entry_bins:
        r = analyze_feature(events, 'entry', filter_fn, label)
        if r and r['n'] >= 10:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            lift = tp100_exp / baseline_tp100_exp if baseline_tp100_exp > 0 else 0
            print(f"  {label:<35} {r['n']:>6}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%  {lift:>7.2f}x")

    # === SECTION 8: COMBINED INSIDER SIGNATURES ===
    print_section("SECTION 8: COMBINED INSIDER SIGNATURES")
    print("  Testing combinations of enrichment features + existing factors")
    print("  Goal: find the lowest-frequency, highest-conviction filters")
    print()

    # Build combined signatures
    signatures = [
        # Novelty + extreme anomaly
        ("Novel + z99 + volOI90",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 99)
                    and ev.get('volume_oi_ratio_score') and float(ev['volume_oi_ratio_score']) >= np.percentile([float(e['volume_oi_ratio_score']) for e in events if e.get('volume_oi_ratio_score')], 90)),

        # Novel + OTM + moderate anomaly (OTM was best single factor)
        ("Novel + otm90 + z95",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('otm_score') and float(ev['otm_score']) >= np.percentile([float(e['otm_score']) for e in events if e.get('otm_score')], 90)
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 95)),

        # Put-heavy + novel + extreme
        ("Put + novel + z97",
         lambda ev: ev.get('direction') == 'put_heavy'
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 97)),

        # Small stock + novel + extreme anomaly
        ("SmallCap + novel + z95",
         lambda ev: ev.get('stock_price') and float(ev['stock_price']) < 50
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 95)),

        # Short DTE + novel + extreme
        ("DTE<=7 + novel + z97",
         lambda ev: ev.get('days_to_expiry', 999) <= 7
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 97)),

        # Cheap entry + short DTE + novel
        ("Entry<$0.50 + DTE<=7 + novel",
         lambda ev: float(ev['option_entry_price']) < 0.50
                    and ev.get('days_to_expiry', 999) <= 7
                    and ev.get('novelty_score', 0) >= 0.7),

        # The "classic insider" profile: novel + OTM + short DTE + extreme z
        ("INSIDER: novel+otm90+DTE<=7+z97",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('otm_score') and float(ev['otm_score']) >= np.percentile([float(e['otm_score']) for e in events if e.get('otm_score')], 90)
                    and ev.get('days_to_expiry', 999) <= 7
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 97)),

        # First-time trigger + any extreme factor
        ("FIRST TRIGGER + z95",
         lambda ev: ev.get('is_first_trigger', False)
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 95)),

        # First-time trigger + OTM (best stock-move predictor)
        ("FIRST TRIGGER + otm90",
         lambda ev: ev.get('is_first_trigger', False)
                    and ev.get('otm_score') and float(ev['otm_score']) >= np.percentile([float(e['otm_score']) for e in events if e.get('otm_score')], 90)),

        # Put-heavy + small stock + novel (institutional insider selling)
        ("Put + stock<$50 + novel",
         lambda ev: ev.get('direction') == 'put_heavy'
                    and ev.get('stock_price') and float(ev['stock_price']) < 50
                    and ev.get('novelty_score', 0) >= 0.7),

        # Concentrated flow + novel + extreme
        ("Concentrated + novel + z97",
         lambda ev: ev.get('contract_count', 1) <= 2
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 97)),

        # Ultra-tight: everything
        ("ULTRA: novel+otm95+DTE<=5+z99+volOI90",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('otm_score') and float(ev['otm_score']) >= np.percentile([float(e['otm_score']) for e in events if e.get('otm_score')], 95)
                    and ev.get('days_to_expiry', 999) <= 5
                    and ev.get('z_score') and float(ev['z_score']) >= np.percentile([float(e['z_score']) for e in events if e.get('z_score')], 99)
                    and ev.get('volume_oi_ratio_score') and float(ev['volume_oi_ratio_score']) >= np.percentile([float(e['volume_oi_ratio_score']) for e in events if e.get('volume_oi_ratio_score')], 90)),
    ]

    # Precompute percentiles
    z_vals = sorted([float(e['z_score']) for e in events if e.get('z_score')])
    otm_vals = sorted([float(e['otm_score']) for e in events if e.get('otm_score')])
    voi_vals = sorted([float(e['volume_oi_ratio_score']) for e in events if e.get('volume_oi_ratio_score')])

    z_p95 = np.percentile(z_vals, 95) if z_vals else 0
    z_p97 = np.percentile(z_vals, 97) if z_vals else 0
    z_p99 = np.percentile(z_vals, 99) if z_vals else 0
    otm_p90 = np.percentile(otm_vals, 90) if otm_vals else 0
    otm_p95 = np.percentile(otm_vals, 95) if otm_vals else 0
    voi_p90 = np.percentile(voi_vals, 90) if voi_vals else 0

    # Simpler signatures using precomputed thresholds
    simple_signatures = [
        ("Novel + z99 + volOI90",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= z_p99
                    and ev.get('volume_oi_ratio_score') and float(ev['volume_oi_ratio_score']) >= voi_p90),

        ("Novel + otm90 + z95",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('otm_score') and float(ev['otm_score']) >= otm_p90
                    and ev.get('z_score') and float(ev['z_score']) >= z_p95),

        ("Put + novel + z97",
         lambda ev: ev.get('direction') == 'put_heavy'
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= z_p97),

        ("Stock<$50 + novel + z95",
         lambda ev: ev.get('stock_price') and float(ev['stock_price']) < 50
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= z_p95),

        ("DTE<=7 + novel + z97",
         lambda ev: ev.get('days_to_expiry', 999) <= 7
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= z_p97),

        ("Entry<$0.50 + DTE<=7 + novel",
         lambda ev: float(ev['option_entry_price']) < 0.50
                    and ev.get('days_to_expiry', 999) <= 7
                    and ev.get('novelty_score', 0) >= 0.7),

        ("INSIDER: novel+otm90+DTE<=7+z97",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('otm_score') and float(ev['otm_score']) >= otm_p90
                    and ev.get('days_to_expiry', 999) <= 7
                    and ev.get('z_score') and float(ev['z_score']) >= z_p97),

        ("FIRST + z95",
         lambda ev: ev.get('is_first_trigger', False)
                    and ev.get('z_score') and float(ev['z_score']) >= z_p95),

        ("FIRST + otm90",
         lambda ev: ev.get('is_first_trigger', False)
                    and ev.get('otm_score') and float(ev['otm_score']) >= otm_p90),

        ("Put + stock<$50 + novel",
         lambda ev: ev.get('direction') == 'put_heavy'
                    and ev.get('stock_price') and float(ev['stock_price']) < 50
                    and ev.get('novelty_score', 0) >= 0.7),

        ("Conc<=2 + novel + z97",
         lambda ev: ev.get('contract_count', 1) <= 2
                    and ev.get('novelty_score', 0) >= 0.7
                    and ev.get('z_score') and float(ev['z_score']) >= z_p97),

        ("ULTRA: novel+otm95+DTE<=5+z99+voi90",
         lambda ev: ev.get('novelty_score', 0) >= 0.7
                    and ev.get('otm_score') and float(ev['otm_score']) >= otm_p95
                    and ev.get('days_to_expiry', 999) <= 5
                    and ev.get('z_score') and float(ev['z_score']) >= z_p99
                    and ev.get('volume_oi_ratio_score') and float(ev['volume_oi_ratio_score']) >= voi_p90),
    ]

    weeks = n_days / 7.0

    print(f"  {'Signature':<40} {'n':>5} {'/wk':>5}  {'TP100@3d':>10}  {'TP100@exp':>10}  "
          f"{'TP500@3d':>10}  {'TP500@exp':>10}  {'TP1000@exp':>10}")
    print(f"  {'-'*130}")

    for label, filter_fn in simple_signatures:
        r = analyze_feature(events, 'sig', filter_fn, label)
        if r and r['n'] >= 5:
            tp100_3d = r['results']['tp100_3d']['rate']
            tp100_exp = r['results']['tp100_exp']['rate']
            tp500_3d = r['results']['tp500_3d']['rate']
            tp500_exp = r['results']['tp500_exp']['rate']
            tp1000_exp = r['results']['tp1000_exp']['rate']
            per_week = r['n'] / weeks if weeks > 0 else 0
            print(f"  {label:<40} {r['n']:>5} {per_week:>4.1f}  {tp100_3d:>9.1f}%  {tp100_exp:>9.1f}%  "
                  f"{tp500_3d:>9.1f}%  {tp500_exp:>9.1f}%  {tp1000_exp:>9.1f}%")

    # === SECTION 9: REVERSE ENGINEER BIG WINNERS ===
    print_section("SECTION 9: REVERSE-ENGINEERING BIG WINNERS")
    print("  What do TP500+ and TP1000+ winners look like?")
    print()

    tp500_winners = [ev for ev in events if ev.get('tp500_exp', False)]
    tp1000_winners = [ev for ev in events if ev.get('tp1000_exp', False)]

    for label, winners in [("TP500+ winners", tp500_winners), ("TP1000+ winners", tp1000_winners)]:
        if not winners:
            print(f"  {label}: 0 events")
            continue

        print(f"  {label} (n={len(winners)}):")

        # Novelty distribution
        novel_high = sum(1 for w in winners if w.get('novelty_score', 0) >= 0.7)
        novel_first = sum(1 for w in winners if w.get('is_first_trigger', False))
        print(f"    Novelty >= 0.7: {novel_high}/{len(winners)} ({novel_high/len(winners)*100:.0f}%)")
        print(f"    First trigger:  {novel_first}/{len(winners)} ({novel_first/len(winners)*100:.0f}%)")

        # Direction
        calls = sum(1 for w in winners if w.get('direction') == 'call_heavy')
        puts = sum(1 for w in winners if w.get('direction') == 'put_heavy')
        print(f"    Call-heavy: {calls} ({calls/len(winners)*100:.0f}%), Put-heavy: {puts} ({puts/len(winners)*100:.0f}%)")

        # Stock price
        prices = [float(w['stock_price']) for w in winners if w.get('stock_price')]
        if prices:
            print(f"    Stock price: median ${np.median(prices):.0f}, "
                  f"mean ${np.mean(prices):.0f}, "
                  f"<$50: {sum(1 for p in prices if p < 50)}/{len(prices)}")

        # DTE
        dtes = [w.get('days_to_expiry', 0) for w in winners]
        print(f"    DTE: median {np.median(dtes):.0f}, "
              f"<=7: {sum(1 for d in dtes if d <= 7)}/{len(dtes)}, "
              f"<=14: {sum(1 for d in dtes if d <= 14)}/{len(dtes)}")

        # Entry price
        entries = [float(w['option_entry_price']) for w in winners]
        print(f"    Entry: median ${np.median(entries):.2f}, "
              f"<$0.50: {sum(1 for e in entries if e < 0.50)}/{len(entries)}")

        # Factor percentiles
        for factor in ['z_score', 'otm_score', 'volume_oi_ratio_score', 'volume_score']:
            all_vals = sorted([float(e[factor]) for e in events if e.get(factor)])
            winner_vals = [float(w[factor]) for w in winners if w.get(factor)]
            if winner_vals and all_vals:
                pctls = [np.searchsorted(all_vals, v) / len(all_vals) * 100
                         for v in winner_vals]
                print(f"    {factor}: median P{np.median(pctls):.0f}, "
                      f"mean P{np.mean(pctls):.0f}, "
                      f"range P{min(pctls):.0f}-P{max(pctls):.0f}")

        # Contract count
        counts = [w.get('contract_count', 1) for w in winners]
        print(f"    Contract count: median {np.median(counts):.0f}, "
              f"single: {sum(1 for c in counts if c == 1)}/{len(counts)}")

        print()

    # === SECTION 10: EV ANALYSIS FOR TOP SIGNATURES ===
    print_section("SECTION 10: EXPECTED VALUE ANALYSIS")
    print("  For each signature, compute EV assuming $1 entry per trade")
    print("  TP target = exit at that return; loss = -$1 (full loss at expiry)")
    print()

    print(f"  {'Signature':<40} {'n':>5}  {'TP500 EV':>10}  {'TP1000 EV':>10}  {'TP500 rate':>10}  {'Breakeven':>10}")
    print(f"  {'-'*100}")

    for label, filter_fn in simple_signatures:
        subset = [ev for ev in events if filter_fn(ev)]
        if len(subset) < 5:
            continue

        for target, multiplier in [(500, 5.0), (1000, 10.0)]:
            hits = sum(1 for ev in subset if ev.get(f'tp{target}_exp', False))
            rate = hits / len(subset) if subset else 0
            ev_val = rate * multiplier - (1 - rate)  # win * payout - lose * $1
            breakeven = 1 / (1 + multiplier)
            if target == 500:
                tp500_rate = f"{rate*100:.1f}%"
                tp500_ev = f"${ev_val:.2f}"
            else:
                tp1000_rate = f"{rate*100:.1f}%"
                tp1000_ev = f"${ev_val:.2f}"
                breakeven_str = f"{breakeven*100:.1f}%"

        print(f"  {label:<40} {len(subset):>5}  {tp500_ev:>10}  {tp1000_ev:>10}  {tp500_rate:>10}  {breakeven_str:>10}")

    # === VERDICT ===
    print_section("VERDICT")

    # Find best EV signature
    best_ev = -999
    best_label = ""
    best_n = 0
    best_rate = 0
    for label, filter_fn in simple_signatures:
        subset = [ev for ev in events if filter_fn(ev)]
        if len(subset) < 5:
            continue
        hits = sum(1 for ev in subset if ev.get('tp500_exp', False))
        rate = hits / len(subset) if subset else 0
        ev_val = rate * 5.0 - (1 - rate)
        if ev_val > best_ev:
            best_ev = ev_val
            best_label = label
            best_n = len(subset)
            best_rate = rate

    print(f"\n  Best TP500 EV signature: {best_label}")
    print(f"    n={best_n}, rate={best_rate*100:.1f}%, EV=${best_ev:.2f}/dollar")
    print(f"    Breakeven for TP500: 16.7%")

    if best_ev > 0:
        print(f"\n  *** POSITIVE EV FOUND ***")
        print(f"  This signature has a positive expected value at TP500!")
        print(f"  Recommended: integrate novelty scoring into alert pipeline.")
    elif best_rate > 0.10:
        print(f"\n  Close to breakeven. Enrichment (news, EDGAR) may push this positive.")
    else:
        print(f"\n  No positive EV found. Signal needs more differentiation.")

    # Create visualization
    print("\nCreating visualization...")
    create_visualization(events, simple_signatures, n_days)
    print("Saved: enriched_signal_analysis.png")

    print(f"\n{'='*90}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*90}")


def create_visualization(events, signatures, n_days):
    """Create summary visualization."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Enriched Signal Analysis\nDo novelty & enrichment features improve insider detection?',
                 fontsize=14, fontweight='bold')

    # 1. Novelty vs TP rates
    ax = axes[0, 0]
    novelty_bins = [
        ('First\ntrigger', lambda ev: ev.get('is_first_trigger', False)),
        ('0 in\n30d', lambda ev: ev.get('trigger_count_30d', 0) == 0 and not ev.get('is_first_trigger', False)),
        ('1-2 in\n30d', lambda ev: 1 <= ev.get('trigger_count_30d', 0) <= 2),
        ('3-5 in\n30d', lambda ev: 3 <= ev.get('trigger_count_30d', 0) <= 5),
        ('10+ in\n30d', lambda ev: ev.get('trigger_count_30d', 0) >= 10),
    ]

    labels, tp100_rates, tp500_rates, counts = [], [], [], []
    for lbl, fn in novelty_bins:
        subset = [ev for ev in events if fn(ev)]
        if subset:
            labels.append(lbl)
            counts.append(len(subset))
            tp100_rates.append(sum(1 for ev in subset if ev.get('tp100_exp', False)) / len(subset) * 100)
            tp500_rates.append(sum(1 for ev in subset if ev.get('tp500_exp', False)) / len(subset) * 100)

    x = np.arange(len(labels))
    w = 0.35
    ax.bar(x - w/2, tp100_rates, w, label='TP100@exp', color='#2196F3')
    ax.bar(x + w/2, tp500_rates, w, label='TP500@exp', color='#FF9800')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel('Hit Rate (%)')
    ax.set_title('Symbol Novelty vs Option Returns')
    ax.legend(fontsize=8)
    for i, c in enumerate(counts):
        ax.annotate(f'n={c}', (x[i], max(tp100_rates[i], tp500_rates[i]) + 0.5),
                   ha='center', fontsize=7)

    # 2. Stock price bins
    ax = axes[0, 1]
    price_bins = [
        ('<$10', lambda ev: ev.get('stock_price') and float(ev['stock_price']) < 10),
        ('$10-25', lambda ev: ev.get('stock_price') and 10 <= float(ev['stock_price']) < 25),
        ('$25-50', lambda ev: ev.get('stock_price') and 25 <= float(ev['stock_price']) < 50),
        ('$50-100', lambda ev: ev.get('stock_price') and 50 <= float(ev['stock_price']) < 100),
        ('$100-500', lambda ev: ev.get('stock_price') and 100 <= float(ev['stock_price']) < 500),
        ('>$500', lambda ev: ev.get('stock_price') and float(ev['stock_price']) >= 500),
    ]

    labels, tp100_rates, tp500_rates, counts = [], [], [], []
    for lbl, fn in price_bins:
        subset = [ev for ev in events if fn(ev)]
        if subset:
            labels.append(lbl)
            counts.append(len(subset))
            tp100_rates.append(sum(1 for ev in subset if ev.get('tp100_exp', False)) / len(subset) * 100)
            tp500_rates.append(sum(1 for ev in subset if ev.get('tp500_exp', False)) / len(subset) * 100)

    x = np.arange(len(labels))
    ax.bar(x - w/2, tp100_rates, w, label='TP100@exp', color='#2196F3')
    ax.bar(x + w/2, tp500_rates, w, label='TP500@exp', color='#FF9800')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel('Hit Rate (%)')
    ax.set_title('Stock Price Level vs Option Returns')
    ax.legend(fontsize=8)
    for i, c in enumerate(counts):
        ax.annotate(f'n={c}', (x[i], max(tp100_rates[i], tp500_rates[i]) + 0.5),
                   ha='center', fontsize=7)

    # 3. Signature comparison
    ax = axes[1, 0]
    sig_labels, sig_tp500, sig_n = [], [], []
    weeks = n_days / 7.0
    for label, filter_fn in signatures:
        subset = [ev for ev in events if filter_fn(ev)]
        if len(subset) >= 5:
            rate = sum(1 for ev in subset if ev.get('tp500_exp', False)) / len(subset) * 100
            sig_labels.append(f"{label}\nn={len(subset)}")
            sig_tp500.append(rate)
            sig_n.append(len(subset))

    if sig_labels:
        colors = ['#4CAF50' if r > 16.7 else '#FF9800' if r > 10 else '#f44336'
                  for r in sig_tp500]
        y = np.arange(len(sig_labels))
        ax.barh(y, sig_tp500, color=colors)
        ax.axvline(x=16.7, color='red', linestyle='--', alpha=0.7, label='TP500 breakeven (16.7%)')
        ax.set_yticks(y)
        ax.set_yticklabels(sig_labels, fontsize=6)
        ax.set_xlabel('TP500@exp Hit Rate (%)')
        ax.set_title('Insider Signatures: TP500 Rates')
        ax.legend(fontsize=8)

    # 4. Summary text
    ax = axes[1, 1]
    ax.axis('off')

    summary_lines = [
        f"ENRICHED SIGNAL ANALYSIS RESULTS",
        f"",
        f"Data: {len(events)} events, {n_days} days",
        f"",
        f"BASELINE RATES:",
        f"  TP100@exp: {sum(1 for e in events if e.get('tp100_exp'))/len(events)*100:.1f}%",
        f"  TP500@exp: {sum(1 for e in events if e.get('tp500_exp'))/len(events)*100:.1f}%",
        f"  TP1000@exp: {sum(1 for e in events if e.get('tp1000_exp'))/len(events)*100:.1f}%",
        f"",
        f"KEY FINDINGS:",
    ]

    # Add findings
    first_trigger = [ev for ev in events if ev.get('is_first_trigger', False)]
    if first_trigger:
        ft_tp100 = sum(1 for e in first_trigger if e.get('tp100_exp', False)) / len(first_trigger) * 100
        ft_tp500 = sum(1 for e in first_trigger if e.get('tp500_exp', False)) / len(first_trigger) * 100
        summary_lines.append(f"  First-trigger: {ft_tp100:.1f}% TP100, {ft_tp500:.1f}% TP500")
        summary_lines.append(f"    (n={len(first_trigger)})")

    put_events = [ev for ev in events if ev.get('direction') == 'put_heavy']
    if put_events:
        pt_tp100 = sum(1 for e in put_events if e.get('tp100_exp', False)) / len(put_events) * 100
        summary_lines.append(f"  Put-heavy: {pt_tp100:.1f}% TP100 (n={len(put_events)})")

    ax.text(0.05, 0.95, '\n'.join(summary_lines),
            transform=ax.transAxes, fontsize=9, verticalalignment='top',
            fontfamily='monospace',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8))

    plt.tight_layout()
    plt.savefig('enriched_signal_analysis.png', dpi=150, bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    main()
