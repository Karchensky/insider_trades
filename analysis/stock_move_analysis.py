#!/usr/bin/env python3
"""
Stock Move Prediction Analysis

THE KEY QUESTION: Can our anomaly signals predict underlying STOCK moves?

Previous analyses measured whether options doubled (TP100+). But option
returns are driven by mechanics (DTE, entry price, theta). That's noise.

If insider trading is happening, the STOCK moves. The option is just a vehicle.
So the real test is: do anomaly signals predict significant stock price moves
in the expected direction?

This analysis:
1. Joins anomaly events with daily_stock_snapshot to get post-event stock returns
2. Defines "catalyst" as stock moving >X% in predicted direction within N days
3. Tests which factors predict catalysts (stock-level, not option-level)
4. For events WHERE a catalyst occurred, THEN evaluates option returns
5. Compares: catalyst-triggered option returns vs. all option returns

If factors predict stock moves: we have a real insider detection signal.
If they don't: the signal isn't in these features, period.
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


def load_events_with_stock_moves(cur):
    """
    Load anomaly events joined with subsequent stock price data.
    For each event, get stock closes for the following 10 trading days.
    """
    # First load events
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
            oc.expiration_date,
            -- Stock price on event day
            s.close AS stock_close_event,
            s.high AS stock_high_event,
            s.low AS stock_low_event,
            s.open AS stock_open_event
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
          AND s.close IS NOT NULL
        ORDER BY a.event_date
    """)
    events = [dict(row) for row in cur.fetchall()]
    return events


def load_stock_moves(cur, events):
    """For each event, load stock prices for the following trading days."""
    print(f"  Loading stock price series for {len(events)} events...")

    # Batch by symbol to reduce queries
    symbol_events = defaultdict(list)
    for ev in events:
        symbol_events[ev['symbol']].append(ev)

    processed = 0
    for symbol, sym_events in symbol_events.items():
        # Get all dates we need for this symbol
        min_date = min(ev['event_date'] for ev in sym_events)
        max_date = max(ev['event_date'] for ev in sym_events)

        cur.execute("""
            SELECT date, open, high, low, close
            FROM daily_stock_snapshot
            WHERE symbol = %s AND date >= %s
            ORDER BY date
        """, (symbol, min_date))

        # Build date->price lookup
        price_data = {}
        for row in cur.fetchall():
            price_data[row['date']] = {
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
            }

        sorted_dates = sorted(price_data.keys())

        for ev in sym_events:
            event_date = ev['event_date']
            stock_close = float(ev['stock_close_event'])

            if stock_close <= 0:
                continue

            # Find trading days after event
            future_dates = [d for d in sorted_dates if d > event_date]

            # Compute directional stock returns
            is_bullish = ev['direction'] == 'call_heavy'

            for window in [1, 2, 3, 5, 10]:
                window_dates = future_dates[:window]
                if not window_dates:
                    ev[f'stock_return_{window}d'] = 0
                    ev[f'stock_max_move_{window}d'] = 0
                    ev[f'stock_dir_return_{window}d'] = 0
                    continue

                # Max close in window
                max_close = max(price_data[d]['close'] for d in window_dates)
                min_close = min(price_data[d]['close'] for d in window_dates)
                # Also check highs/lows for intraday
                max_high = max(price_data[d]['high'] for d in window_dates)
                min_low = min(price_data[d]['low'] for d in window_dates)

                # Raw return (close-to-close)
                last_close = price_data[window_dates[-1]]['close']
                ev[f'stock_return_{window}d'] = (last_close / stock_close - 1) * 100

                # Max directional move (using highs for calls, lows for puts)
                if is_bullish:
                    ev[f'stock_max_move_{window}d'] = (max_high / stock_close - 1) * 100
                else:
                    ev[f'stock_max_move_{window}d'] = (1 - min_low / stock_close) * 100

                # Directional return (positive = moved in predicted direction)
                if is_bullish:
                    ev[f'stock_dir_return_{window}d'] = (max_close / stock_close - 1) * 100
                else:
                    ev[f'stock_dir_return_{window}d'] = (1 - min_close / stock_close) * 100

        processed += len(sym_events)
        if processed % 2000 == 0:
            print(f"    {processed}/{len(events)}...")

    # Also load option price series for events with catalysts
    print(f"  Loading option price series...")
    for i, ev in enumerate(events):
        if i % 1000 == 0 and i > 0:
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

        prices = []
        for row in cur.fetchall():
            prices.append({
                'close': float(row['close_price'] or 0),
                'high': float(row['high_price'] or 0),
            })

        if entry > 0 and prices:
            for window in [1, 2, 3, 5, 10]:
                highs = [p['high'] for p in prices[:window] if p['high'] > 0]
                ev[f'option_return_{window}d'] = (max(highs) / entry - 1) * 100 if highs else -100
            all_highs = [p['high'] for p in prices if p['high'] > 0]
            ev['option_return_expiry'] = (max(all_highs) / entry - 1) * 100 if all_highs else -100
        else:
            for window in [1, 2, 3, 5, 10]:
                ev[f'option_return_{window}d'] = -100
            ev['option_return_expiry'] = -100

    return events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-events', type=int, default=0)
    args = parser.parse_args()

    conn = db.connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 90)
    print("STOCK MOVE PREDICTION ANALYSIS")
    print("=" * 90)
    print("\nTHE KEY QUESTION: Do anomaly signals predict STOCK moves?")
    print("If yes -> real insider detection. If no -> features are noise.")

    events = load_events_with_stock_moves(cur)
    if args.max_events > 0:
        events = events[:args.max_events]
    print(f"\nTotal events with stock data: {len(events)}")

    events = load_stock_moves(cur, events)

    dates = sorted(set(d['event_date'] for d in events))
    weeks = len(dates) / 5.0
    print(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} days, ~{weeks:.1f} weeks)")

    # Compute factor arrays
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

    thresholds = {}
    for name, arr in factor_arrays.items():
        valid = arr[arr > 0]
        if len(valid) > 0:
            thresholds[name] = {p: np.percentile(valid, p) for p in [80, 90, 95, 97, 99, 99.5]}

    # =========================================================================
    # SECTION 1: BASELINE STOCK MOVE RATES
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 1: HOW OFTEN DO STOCKS MOVE AFTER ANOMALIES?")
    print("=" * 90)

    move_thresholds = [2, 3, 5, 7, 10, 15, 20]
    windows = [1, 2, 3, 5]

    print(f"\n  Directional stock moves (max move in predicted direction, using highs/lows)")
    print(f"\n  {'Window':<10}", end="")
    for pct in move_thresholds:
        print(f"{'>' + str(pct) + '%':>8}", end="")
    print()
    print("  " + "-" * (10 + 8 * len(move_thresholds)))

    catalyst_rates = {}
    for w in windows:
        print(f"  {w}d{'':<7}", end="")
        for pct in move_thresholds:
            field = f'stock_max_move_{w}d'
            hits = sum(1 for ev in events if ev.get(field, 0) >= pct)
            rate = hits / len(events) * 100
            catalyst_rates[(pct, w)] = {'hits': hits, 'rate': rate}
            print(f"{rate:>7.1f}%", end="")
        print()

    print(f"\n  For context: how often do stocks move >5% in 3 days?")
    c = catalyst_rates[(5, 3)]
    print(f"  {c['hits']} events ({c['rate']:.1f}%), ~{c['hits']/weeks:.0f}/week, ~{c['hits']/len(dates):.0f}/day")

    # =========================================================================
    # SECTION 2: DIRECTION ACCURACY
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 2: DIRECTIONAL ACCURACY")
    print("=" * 90)
    print("  Does the predicted direction (call_heavy/put_heavy) actually predict stock direction?")

    for w in [1, 2, 3, 5]:
        field = f'stock_return_{w}d'
        bullish_events = [ev for ev in events if ev['direction'] == 'call_heavy' and field in ev]
        bearish_events = [ev for ev in events if ev['direction'] == 'put_heavy' and field in ev]

        if bullish_events:
            bull_correct = sum(1 for ev in bullish_events if ev[field] > 0)
            bull_accuracy = bull_correct / len(bullish_events) * 100
        else:
            bull_accuracy = 0

        if bearish_events:
            bear_correct = sum(1 for ev in bearish_events if ev[field] < 0)
            bear_accuracy = bear_correct / len(bearish_events) * 100
        else:
            bear_accuracy = 0

        total = len(bullish_events) + len(bearish_events)
        total_correct = (sum(1 for ev in bullish_events if ev.get(field, 0) > 0) +
                         sum(1 for ev in bearish_events if ev.get(field, 0) < 0))
        overall_accuracy = total_correct / total * 100 if total > 0 else 0

        print(f"  {w}d: Bullish accuracy={bull_accuracy:.1f}% (n={len(bullish_events)}), "
              f"Bearish accuracy={bear_accuracy:.1f}% (n={len(bearish_events)}), "
              f"Overall={overall_accuracy:.1f}%")

    # =========================================================================
    # SECTION 3: WHICH FACTORS PREDICT STOCK CATALYSTS?
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 3: WHICH FACTORS PREDICT >5% STOCK MOVES (3-day)?")
    print("=" * 90)
    print("  THIS IS THE KEY SECTION. If nothing predicts stock moves, game over.")

    catalyst_field = 'stock_max_move_3d'
    catalyst_threshold = 5.0
    baseline_catalyst = sum(1 for ev in events if ev.get(catalyst_field, 0) >= catalyst_threshold)
    baseline_rate = baseline_catalyst / len(events) * 100

    print(f"\n  Baseline: {fmt_ci(baseline_catalyst, len(events))} of events see >5% stock move in 3d")

    # Test each factor
    print(f"\n  {'Factor':<20} {'Pctl':>6} {'Thresh':>10} {'n':>6} "
          f"{'Catalyst%':>10} {'Lift':>8} {'CI':>22}")
    print("  " + "-" * 90)

    for name in ['z_score', 'vol_oi_score', 'volume_score', 'otm_score',
                  'directional_score', 'time_score', 'total_score', 'magnitude']:
        if name not in thresholds:
            continue

        field = 'volume_oi_ratio_score' if name == 'vol_oi_score' else \
                'total_magnitude' if name == 'magnitude' else name

        for pctl in [90, 95, 97, 99, 99.5]:
            if pctl not in thresholds[name]:
                continue
            thresh = thresholds[name][pctl]
            filtered = [ev for ev in events if float(ev.get(field) or 0) >= thresh]
            n = len(filtered)
            if n < 10:
                continue

            hits = sum(1 for ev in filtered if ev.get(catalyst_field, 0) >= catalyst_threshold)
            rate = hits / n * 100
            lift = rate / baseline_rate if baseline_rate > 0 else 0
            ci = fmt_ci(hits, n)
            marker = " ***" if lift > 1.5 else " **" if lift > 1.2 else ""
            if name == 'magnitude':
                print(f"  {name:<20} P{pctl:<5} ${thresh:>8,.0f} {n:>6} "
                      f"{rate:>9.1f}% {lift:>7.2f}x {ci}{marker}")
            else:
                print(f"  {name:<20} P{pctl:<5} {thresh:>10.2f} {n:>6} "
                      f"{rate:>9.1f}% {lift:>7.2f}x {ci}{marker}")

    # Also test DTE
    print()
    for dte in [30, 14, 7, 5, 3]:
        filtered = [ev for ev in events
                    if (ev['expiration_date'] - ev['event_date']).days <= dte]
        n = len(filtered)
        if n < 10:
            continue
        hits = sum(1 for ev in filtered if ev.get(catalyst_field, 0) >= catalyst_threshold)
        rate = hits / n * 100
        lift = rate / baseline_rate if baseline_rate > 0 else 0
        marker = " ***" if lift > 1.5 else " **" if lift > 1.2 else ""
        print(f"  {'DTE':<20} <={dte:<5} {'':>10} {n:>6} "
              f"{rate:>9.1f}% {lift:>7.2f}x {fmt_ci(hits, n)}{marker}")

    # =========================================================================
    # SECTION 4: REPEAT FOR MULTIPLE CATALYST DEFINITIONS
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 4: FACTOR LIFT ACROSS MULTIPLE CATALYST DEFINITIONS")
    print("=" * 90)

    # Test the most promising factors at their best percentile for each catalyst def
    test_combos = [
        ('z_score', 'z_score', 99),
        ('z_score', 'z_score', 99.5),
        ('vol_oi_score', 'volume_oi_ratio_score', 95),
        ('otm_score', 'otm_score', 95),
        ('time_score', 'time_score', 90),
        ('total_score', 'total_score', 97),
        ('magnitude', 'total_magnitude', 95),
    ]

    catalysts = [
        ('>3% 1d', 'stock_max_move_1d', 3),
        ('>5% 1d', 'stock_max_move_1d', 5),
        ('>5% 3d', 'stock_max_move_3d', 5),
        ('>10% 3d', 'stock_max_move_3d', 10),
        ('>5% 5d', 'stock_max_move_5d', 5),
        ('>10% 5d', 'stock_max_move_5d', 10),
    ]

    header = f"  {'Factor@Pctl':<25}"
    for cat_label, _, _ in catalysts:
        header += f"{cat_label:>10}"
    print(header)
    print("  " + "-" * (25 + 10 * len(catalysts)))

    # Baseline row
    row = f"  {'BASELINE':<25}"
    for _, cat_field, cat_thresh in catalysts:
        bl_hits = sum(1 for ev in events if ev.get(cat_field, 0) >= cat_thresh)
        bl_rate = bl_hits / len(events) * 100
        row += f"{bl_rate:>9.1f}%"
    print(row)
    print()

    for name, field, pctl in test_combos:
        thresh = thresholds[name][pctl]
        filtered = [ev for ev in events if float(ev.get(field) or 0) >= thresh]
        n = len(filtered)
        if n < 10:
            continue

        row = f"  {name}@P{pctl} (n={n})"
        row = f"  {row:<25}"
        for _, cat_field, cat_thresh in catalysts:
            bl_hits = sum(1 for ev in events if ev.get(cat_field, 0) >= cat_thresh)
            bl_rate = bl_hits / len(events) * 100
            hits = sum(1 for ev in filtered if ev.get(cat_field, 0) >= cat_thresh)
            rate = hits / n * 100
            lift = rate / bl_rate if bl_rate > 0 else 0
            row += f"  {lift:>7.2f}x"
        print(row)

    # =========================================================================
    # SECTION 5: SIGNATURE COMBINATIONS FOR STOCK CATALYSTS
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 5: INSIDER SIGNATURES -> STOCK CATALYSTS")
    print("=" * 90)
    print("  Do our insider signatures predict >5% stock moves?")

    def make_filter(criteria):
        def filt(ev):
            for check in criteria.values():
                if not check(ev):
                    return False
            return True
        return filt

    signatures = {
        'z99+volOI95': make_filter({
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
            'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][95],
        }),
        'z99.5+DTE<=7': make_filter({
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99.5],
            'dte': lambda ev: (ev['expiration_date'] - ev['event_date']).days <= 7,
        }),
        'z99+volOI90+DTE<=7': make_filter({
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
            'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
            'dte': lambda ev: (ev['expiration_date'] - ev['event_date']).days <= 7,
        }),
        'z99+volOI90+otm90': make_filter({
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
            'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
            'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
        }),
        'z99+volOI90+otm90+DTE<=14': make_filter({
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
            'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
            'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
            'dte': lambda ev: (ev['expiration_date'] - ev['event_date']).days <= 14,
        }),
        'z97+volOI90+DTE<=5': make_filter({
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][97],
            'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
            'dte': lambda ev: (ev['expiration_date'] - ev['event_date']).days <= 5,
        }),
        'totalScore@P97+DTE<=7': make_filter({
            'ts': lambda ev: float(ev['total_score'] or 0) >= thresholds['total_score'][97],
            'dte': lambda ev: (ev['expiration_date'] - ev['event_date']).days <= 7,
        }),
        'otm95+z95+volOI90': make_filter({
            'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][95],
            'z': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][95],
            'voi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
        }),
    }

    print(f"\n  Baseline >5% stock move rates:")
    for w in [1, 2, 3, 5]:
        bl = sum(1 for ev in events if ev.get(f'stock_max_move_{w}d', 0) >= 5)
        print(f"    {w}d: {bl/len(events)*100:.1f}% ({bl} events)")

    bl_3d = sum(1 for ev in events if ev.get('stock_max_move_3d', 0) >= 5) / len(events) * 100

    print(f"\n  {'Signature':<40} {'n':>5} {'/wk':>5} "
          f"{'Cat@1d':>8} {'Cat@3d':>8} {'Cat@5d':>8} "
          f"{'Lift@3d':>8} {'DirAcc3d':>8}")
    print("  " + "-" * 95)

    sig_results = []
    for sig_name, sig_filter in signatures.items():
        filtered = [ev for ev in events if sig_filter(ev)]
        n = len(filtered)
        if n < 5:
            print(f"  {sig_name:<40} n={n} (too few)")
            continue

        epw = n / weeks

        cat_1d = sum(1 for ev in filtered if ev.get('stock_max_move_1d', 0) >= 5)
        cat_3d = sum(1 for ev in filtered if ev.get('stock_max_move_3d', 0) >= 5)
        cat_5d = sum(1 for ev in filtered if ev.get('stock_max_move_5d', 0) >= 5)

        rate_1d = cat_1d / n * 100
        rate_3d = cat_3d / n * 100
        rate_5d = cat_5d / n * 100
        lift_3d = rate_3d / bl_3d if bl_3d > 0 else 0

        # Directional accuracy
        correct_3d = sum(1 for ev in filtered
                         if (ev['direction'] == 'call_heavy' and ev.get('stock_return_3d', 0) > 0) or
                            (ev['direction'] == 'put_heavy' and ev.get('stock_return_3d', 0) < 0))
        dir_acc = correct_3d / n * 100

        print(f"  {sig_name:<40} {n:>5} {epw:>4.1f} "
              f"{rate_1d:>7.1f}% {rate_3d:>7.1f}% {rate_5d:>7.1f}% "
              f"{lift_3d:>7.2f}x {dir_acc:>7.1f}%")

        sig_results.append((sig_name, n, epw, rate_1d, rate_3d, rate_5d, lift_3d, dir_acc))

    # =========================================================================
    # SECTION 6: CONDITIONAL OPTION RETURNS (catalyst-triggered)
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 6: OPTION RETURNS CONDITIONAL ON STOCK CATALYST")
    print("=" * 90)
    print("  If a >5% stock move occurs within 3d, how do the OPTIONS perform?")
    print("  This separates 'did we detect informed flow?' from 'did the option print?'\n")

    catalyst_events = [ev for ev in events if ev.get('stock_max_move_3d', 0) >= 5]
    non_catalyst = [ev for ev in events if ev.get('stock_max_move_3d', 0) < 5]

    print(f"  Catalyst events (>5% stock move in 3d): {len(catalyst_events)} "
          f"({len(catalyst_events)/len(events)*100:.1f}%)")
    print(f"  Non-catalyst events: {len(non_catalyst)} ({len(non_catalyst)/len(events)*100:.1f}%)")

    tp_targets = [100, 200, 500, 1000]

    print(f"\n  {'Group':<25} {'n':>6}", end="")
    for tp in tp_targets:
        print(f"  {'TP'+str(tp)+'@3d':>10}", end="")
    for tp in tp_targets:
        print(f"  {'TP'+str(tp)+'@exp':>10}", end="")
    print()
    print("  " + "-" * (33 + 12 * len(tp_targets) * 2))

    for label, group in [("All events", events), ("Catalyst (>5% stock)", catalyst_events),
                          ("Non-catalyst", non_catalyst)]:
        n = len(group)
        if n == 0:
            continue
        row = f"  {label:<25} {n:>6}"
        for tp in tp_targets:
            hits = sum(1 for ev in group if ev.get('option_return_3d', -100) >= tp)
            row += f"  {hits/n*100:>9.1f}%"
        for tp in tp_targets:
            hits = sum(1 for ev in group if ev.get('option_return_expiry', -100) >= tp)
            row += f"  {hits/n*100:>9.1f}%"
        print(row)

    # Now: for each signature, show option returns on catalyst-filtered events
    print(f"\n  Signature option returns (only events where stock moved >5% in 3d):")
    print(f"  {'Signature':<35} {'Cat n':>6}", end="")
    for tp in [100, 200, 500, 1000]:
        print(f"  {'TP'+str(tp)+'@3d':>10}", end="")
    print(f"  {'TP500@exp':>10}")
    print("  " + "-" * (45 + 12 * 5))

    for sig_name, sig_filter in signatures.items():
        filtered = [ev for ev in events if sig_filter(ev)]
        cat_filtered = [ev for ev in filtered if ev.get('stock_max_move_3d', 0) >= 5]
        n = len(cat_filtered)
        if n < 3:
            continue

        row = f"  {sig_name:<35} {n:>6}"
        for tp in [100, 200, 500, 1000]:
            hits = sum(1 for ev in cat_filtered if ev.get('option_return_3d', -100) >= tp)
            row += f"  {hits/n*100:>9.1f}%"
        tp500_exp = sum(1 for ev in cat_filtered if ev.get('option_return_expiry', -100) >= 500)
        row += f"  {tp500_exp/n*100:>9.1f}%"
        print(row)

    # =========================================================================
    # SECTION 7: THE VERDICT
    # =========================================================================
    print("\n" + "=" * 90)
    print("SECTION 7: THE VERDICT")
    print("=" * 90)

    # Find best signature for catalyst prediction
    best_sig = max(sig_results, key=lambda x: x[6]) if sig_results else None  # by lift@3d
    best_dir = max(sig_results, key=lambda x: x[7]) if sig_results else None  # by dir accuracy

    if best_sig:
        print(f"\n  Best catalyst predictor: {best_sig[0]}")
        print(f"    n={best_sig[1]}, ~{best_sig[2]:.1f}/week")
        print(f"    >5% stock move in 3d: {best_sig[4]:.1f}% (lift {best_sig[6]:.2f}x)")
        print(f"    Direction accuracy: {best_sig[7]:.1f}%")

    if best_dir:
        print(f"\n  Best directional predictor: {best_dir[0]}")
        print(f"    n={best_dir[1]}, ~{best_dir[2]:.1f}/week")
        print(f"    Direction accuracy: {best_dir[7]:.1f}%")

    print(f"""
  INTERPRETATION:
  - If lift > 1.5x for stock catalysts: factors DO detect informed flow
    -> Enrichment (news, EDGAR) will improve signal further
  - If lift ~1.0x: factors don't predict stock moves at all
    -> Signal isn't in these features, enrichment won't help
  - If direction accuracy > 55%: anomaly direction prediction has value
  - If direction accuracy ~50%: direction signal is random
    """)

    # =========================================================================
    # VISUALIZATION
    # =========================================================================
    print("Creating visualization...")

    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle('Stock Move Prediction Analysis\n'
                 'Do anomaly signals predict underlying STOCK moves?',
                 fontsize=14, fontweight='bold')

    # 1. Catalyst rates by factor
    ax1 = axes[0, 0]
    factor_lifts = []
    factor_names = []
    for name in ['z_score', 'vol_oi_score', 'volume_score', 'otm_score',
                  'directional_score', 'time_score', 'total_score']:
        if name not in thresholds or 99 not in thresholds[name]:
            best_pctl = max(p for p in thresholds.get(name, {}).keys() if p <= 99)
        else:
            best_pctl = 99

        if name not in thresholds or best_pctl not in thresholds[name]:
            continue

        field = 'volume_oi_ratio_score' if name == 'vol_oi_score' else \
                'total_magnitude' if name == 'magnitude' else name
        thresh = thresholds[name][best_pctl]
        filtered = [ev for ev in events if float(ev.get(field) or 0) >= thresh]
        if len(filtered) < 10:
            continue

        hits = sum(1 for ev in filtered if ev.get('stock_max_move_3d', 0) >= 5)
        rate = hits / len(filtered) * 100
        lift = rate / bl_3d if bl_3d > 0 else 0
        factor_lifts.append(lift)
        factor_names.append(f"{name}@P{best_pctl}")

    if factor_names:
        colors = ['#4CAF50' if l > 1.2 else '#F44336' if l < 0.8 else '#FFC107' for l in factor_lifts]
        y_pos = np.arange(len(factor_names))
        ax1.barh(y_pos, factor_lifts, color=colors)
        ax1.axvline(1.0, color='black', linewidth=1, linestyle='--')
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(factor_names, fontsize=9)
        ax1.set_xlabel('Lift vs baseline')
        ax1.set_title('Factor Lift for >5% Stock Move (3d)', fontsize=11)
        ax1.invert_yaxis()

    # 2. Signature catalyst prediction
    ax2 = axes[0, 1]
    if sig_results:
        sig_names = [r[0][:30] for r in sig_results]
        sig_lifts = [r[6] for r in sig_results]
        sig_dir_acc = [r[7] for r in sig_results]
        sig_n = [r[1] for r in sig_results]

        y_pos = np.arange(len(sig_results))
        bars = ax2.barh(y_pos, sig_lifts, color='#FF9800', alpha=0.8)
        ax2.axvline(1.0, color='black', linewidth=1, linestyle='--', label='Baseline (1.0x)')
        ax2.axvline(1.5, color='red', linewidth=1, linestyle=':', alpha=0.5, label='Meaningful (1.5x)')

        for i, (n, l) in enumerate(zip(sig_n, sig_lifts)):
            ax2.text(l + 0.02, i, f'n={n}', va='center', fontsize=8)

        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(sig_names, fontsize=8)
        ax2.set_xlabel('Lift for >5% stock move (3d)')
        ax2.set_title('Signature Catalyst Prediction', fontsize=11)
        ax2.invert_yaxis()
        ax2.legend(fontsize=8)

    # 3. Conditional option returns
    ax3 = axes[1, 0]
    cat_tps = [100, 200, 500, 1000]
    cat_rates = []
    noncat_rates = []
    for tp in cat_tps:
        if catalyst_events:
            cat_rates.append(sum(1 for ev in catalyst_events
                                 if ev.get('option_return_3d', -100) >= tp) / len(catalyst_events) * 100)
        else:
            cat_rates.append(0)
        if non_catalyst:
            noncat_rates.append(sum(1 for ev in non_catalyst
                                    if ev.get('option_return_3d', -100) >= tp) / len(non_catalyst) * 100)
        else:
            noncat_rates.append(0)

    x = np.arange(len(cat_tps))
    width = 0.35
    ax3.bar(x - width/2, cat_rates, width, label=f'Catalyst (>5% stock, n={len(catalyst_events)})',
            color='#4CAF50')
    ax3.bar(x + width/2, noncat_rates, width, label=f'Non-catalyst (n={len(non_catalyst)})',
            color='#BDBDBD')
    ax3.set_xticks(x)
    ax3.set_xticklabels([f'TP{tp}%' for tp in cat_tps])
    ax3.set_ylabel('Hit Rate (%)')
    ax3.set_title('Option Returns: Catalyst vs Non-Catalyst Events (3d)', fontsize=11)
    ax3.legend(fontsize=9)
    ax3.grid(axis='y', alpha=0.3)

    # 4. Summary
    ax4 = axes[1, 1]
    ax4.axis('off')

    summary = f"""STOCK MOVE PREDICTION RESULTS

DATA: {len(events)} events, {len(dates)} trading days

BASELINE CATALYST RATES (>5% stock move):
  1d: {catalyst_rates[(5,1)]['rate']:.1f}%
  3d: {catalyst_rates[(5,3)]['rate']:.1f}%
  5d: {catalyst_rates[(5,5)]['rate']:.1f}%
"""
    if best_sig:
        summary += f"""
BEST CATALYST PREDICTOR:
  {best_sig[0]}
  n={best_sig[1]}, {best_sig[2]:.1f}/wk
  Catalyst rate: {best_sig[4]:.1f}% (lift {best_sig[6]:.2f}x)
  Direction accuracy: {best_sig[7]:.1f}%
"""

    summary += f"""
CONDITIONAL OPTION RETURNS (3d):
  With catalyst:  TP100={cat_rates[0]:.1f}% TP500={cat_rates[2]:.1f}%
  Without:        TP100={noncat_rates[0]:.1f}% TP500={noncat_rates[2]:.1f}%

VERDICT:
  Lift > 1.5x = real signal, enrich further
  Lift ~ 1.0x = noise, features can't detect
  Dir accuracy > 55% = direction has value
"""

    ax4.text(0.02, 0.98, summary, transform=ax4.transAxes, fontsize=9.5,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

    plt.tight_layout()
    plt.savefig('stock_move_analysis.png', dpi=150, bbox_inches='tight')
    print("Saved: stock_move_analysis.png")

    conn.close()
    print("\n" + "=" * 90)
    print("ANALYSIS COMPLETE")
    print("=" * 90)


if __name__ == '__main__':
    main()
