#!/usr/bin/env python3
"""
Rapid Return & Insider Signature Analysis

Instead of "did this option double before expiration?", asks:
- How FAST do returns materialize? (1d, 2d, 3d, 5d windows)
- At what TP targets? (TP30, TP50, TP100, TP200, TP500)
- Can we identify "insider signature" events with extreme, rapid returns?

Insider trading hypothesis: real insider trades show:
- Short-dated OTM options (maximum leverage, they know WHEN)
- Strong directional bias (all calls or all puts, not hedging)
- Anomalous volume (high z-score vs baseline)
- Fresh positioning (high vol:OI ratio — new money, not rolling)
- Fast payoff (catalyst within 1-3 days, not a slow grind)

Uses high_price (intraday highs) to capture midday spikes, not just close.
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


# =============================================================================
# STATISTICAL HELPERS
# =============================================================================

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


# =============================================================================
# DATA LOADING
# =============================================================================

def load_events(cur):
    """Load one row per (symbol, event_date) with all factor scores."""
    cur.execute("""
        SELECT
            a.symbol, a.event_date, a.direction,
            a.recommended_option,
            a.total_score, a.volume_score, a.volume_oi_ratio_score,
            a.otm_score, a.directional_score, a.time_score,
            a.z_score, a.total_magnitude, a.total_volume,
            o.close_price AS entry_price,
            o.high_price AS entry_high,
            o.volume AS option_volume, o.open_interest,
            o.implied_volatility, o.greeks_delta, o.greeks_gamma,
            o.greeks_theta, o.greeks_vega,
            oc.expiration_date,
            oc.strike_price, oc.contract_type
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
    """Load post-event price series (close + high) for each event's recommended contract."""
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
        ev['price_series'] = prices

        entry = float(ev['entry_price'])
        ev['days_to_expiry'] = (ev['expiration_date'] - ev['event_date']).days

        if entry > 0 and prices:
            # Time-windowed max returns using HIGH price (captures intraday spikes)
            for window in [1, 2, 3, 5, 10]:
                window_highs = [p['high'] for p in prices[:window] if p['high'] > 0]
                window_closes = [p['close'] for p in prices[:window] if p['close'] > 0]
                if window_highs:
                    max_h = max(window_highs)
                    ev[f'max_return_{window}d'] = (max_h / entry - 1) * 100
                else:
                    ev[f'max_return_{window}d'] = -100.0
                # Also track close-based returns for comparison
                if window_closes:
                    max_c = max(window_closes)
                    ev[f'max_close_return_{window}d'] = (max_c / entry - 1) * 100
                else:
                    ev[f'max_close_return_{window}d'] = -100.0

            # To expiry (using highs)
            all_highs = [p['high'] for p in prices if p['high'] > 0]
            if all_highs:
                ev['max_return_expiry'] = (max(all_highs) / entry - 1) * 100
            else:
                ev['max_return_expiry'] = -100.0
        else:
            for window in [1, 2, 3, 5, 10]:
                ev[f'max_return_{window}d'] = -100.0
                ev[f'max_close_return_{window}d'] = -100.0
            ev['max_return_expiry'] = -100.0

    return events


# =============================================================================
# ANALYSIS FUNCTIONS
# =============================================================================

def compute_hit_matrix(events, tp_targets, windows):
    """Compute hit rates for each TP target x time window."""
    results = {}
    n = len(events)
    for window in windows:
        for tp in tp_targets:
            key = f'TP{tp}_{window}'
            if window == 'expiry':
                hits = sum(1 for ev in events if ev.get('max_return_expiry', -100) >= tp)
            else:
                hits = sum(1 for ev in events if ev.get(f'max_return_{window}d', -100) >= tp)
            results[key] = {'hits': hits, 'total': n}
    return results


def print_matrix(results, tp_targets, windows, n, label=""):
    """Print a clean TP x Window hit rate matrix."""
    if label:
        print(f"\n  {label} (n={n})")

    header = f"  {'Window':<12}" + "".join(f"{'TP'+str(tp)+'%':>10}" for tp in tp_targets)
    print(header)
    print("  " + "-" * (12 + 10 * len(tp_targets)))

    for window in windows:
        wlabel = f"{window}d" if window != 'expiry' else 'to expiry'
        row = f"  {wlabel:<12}"
        for tp in tp_targets:
            key = f'TP{tp}_{window}'
            h = results[key]
            rate = h['hits'] / h['total'] * 100 if h['total'] > 0 else 0
            row += f"{rate:>9.1f}%"
        print(row)


def print_lift_matrix(sig_results, bl_results, tp_targets, windows, sig_n, label=""):
    """Print lift vs baseline matrix."""
    if label:
        print(f"\n  {label} (n={sig_n})")

    header = f"  {'Window':<12}" + "".join(f"{'TP'+str(tp)+'%':>10}" for tp in tp_targets)
    print(header)
    print("  " + "-" * (12 + 10 * len(tp_targets)))

    for window in windows:
        wlabel = f"{window}d" if window != 'expiry' else 'to expiry'
        row = f"  {wlabel:<12}"
        for tp in tp_targets:
            key = f'TP{tp}_{window}'
            s = sig_results[key]
            b = bl_results[key]
            s_rate = s['hits'] / s['total'] if s['total'] > 0 else 0
            b_rate = b['hits'] / b['total'] if b['total'] > 0 else 0
            lift = s_rate / b_rate if b_rate > 0 else 0
            row += f"{lift:>9.2f}x"
        print(row)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Rapid return & insider signature analysis')
    parser.add_argument('--max-events', type=int, default=0,
                        help='Limit events for testing (0=all)')
    args = parser.parse_args()

    conn = db.connect()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 80)
    print("RAPID RETURN & INSIDER SIGNATURE ANALYSIS")
    print("=" * 80)
    print("\nKey differences from previous analysis:")
    print("  - Uses HIGH PRICE (intraday peaks), not just close")
    print("  - Time-windowed returns: 1d, 2d, 3d, 5d, 10d, to-expiry")
    print("  - Multiple TP targets: 30%, 50%, 100%, 200%, 500%")
    print("  - Insider signature filters targeting extreme outliers")
    print("  - Reverse-engineers what winners actually look like")

    # Load data
    print("\n" + "=" * 80)
    print("LOADING DATA")
    print("=" * 80)

    events = load_events(cur)
    if args.max_events > 0:
        events = events[:args.max_events]
    print(f"\n  Total events: {len(events)}")

    events = load_price_series(cur, events)

    dates = sorted(set(d['event_date'] for d in events))
    print(f"  Date range: {dates[0]} to {dates[-1]} ({len(dates)} trading days)")
    print(f"  Events/day: ~{len(events)/len(dates):.0f}")

    tp_targets = [30, 50, 100, 200, 500]
    windows = [1, 2, 3, 5, 10, 'expiry']

    # =========================================================================
    # SECTION 1: BASELINE MATRIX
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 1: BASELINE — TP TARGET x TIME WINDOW (using intraday highs)")
    print("=" * 80)

    baseline = compute_hit_matrix(events, tp_targets, windows)
    print_matrix(baseline, tp_targets, windows, len(events), "ALL EVENTS")

    # Close-only comparison for TP100
    print(f"\n  Close-only vs High-based TP100 comparison:")
    for w in [1, 2, 3, 5]:
        high_hits = sum(1 for ev in events if ev.get(f'max_return_{w}d', -100) >= 100)
        close_hits = sum(1 for ev in events if ev.get(f'max_close_return_{w}d', -100) >= 100)
        print(f"    {w}d: high-based={high_hits/len(events)*100:.1f}%, "
              f"close-based={close_hits/len(events)*100:.1f}% "
              f"(+{(high_hits-close_hits)/len(events)*100:.1f}% from intraday peaks)")

    # =========================================================================
    # SECTION 2: ENTRY PRICE SEGMENTATION
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 2: ENTRY PRICE SEGMENTATION")
    print("=" * 80)
    print("  Cheap options double more easily. Does price band matter?")

    price_bands = [
        ("$0.05-$0.25", 0.05, 0.25),
        ("$0.25-$0.50", 0.25, 0.50),
        ("$0.50-$1.00", 0.50, 1.00),
        ("$1.00-$2.00", 1.00, 2.00),
        ("$2.00-$5.00", 2.00, 5.00),
    ]

    key_targets = [(100, 3), (100, 5), (50, 3), (200, 3)]  # (TP, window)

    header = f"  {'Price Band':<16} {'n':>6}"
    for tp, w in key_targets:
        header += f"  {'TP'+str(tp)+'@'+str(w)+'d':>10}"
    print(header)
    print("  " + "-" * (24 + 12 * len(key_targets)))

    for label, lo, hi in price_bands:
        band_events = [ev for ev in events if lo <= float(ev['entry_price']) < hi]
        if len(band_events) < 10:
            continue
        row = f"  {label:<16} {len(band_events):>6}"
        for tp, w in key_targets:
            hits = sum(1 for ev in band_events if ev.get(f'max_return_{w}d', -100) >= tp)
            rate = hits / len(band_events) * 100
            row += f"  {rate:>9.1f}%"
        print(row)

    # =========================================================================
    # SECTION 3: FACTOR PERCENTILE DISTRIBUTIONS
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 3: FACTOR DISTRIBUTIONS & THRESHOLDS")
    print("=" * 80)

    factor_map = {
        'z_score': ('z_score', None),
        'vol_oi_score': ('volume_oi_ratio_score', None),
        'volume_score': ('volume_score', None),
        'otm_score': ('otm_score', None),
        'directional_score': ('directional_score', None),
        'time_score': ('time_score', None),
        'total_score': ('total_score', None),
        'magnitude': ('total_magnitude', None),
    }

    factor_arrays = {}
    for name, (field, _) in factor_map.items():
        factor_arrays[name] = np.array([float(d.get(field) or 0) for d in events])

    dte_arr = np.array([ev['days_to_expiry'] for ev in events])
    entry_arr = np.array([float(ev['entry_price']) for ev in events])

    thresholds = {}
    print(f"\n  {'Factor':<20} {'P50':>8} {'P80':>8} {'P90':>8} {'P95':>8} {'P99':>8} {'P99.5':>8}")
    print("  " + "-" * 70)
    for name, arr in factor_arrays.items():
        valid = arr[arr > 0]
        if len(valid) > 0:
            pcts = np.percentile(valid, [50, 80, 90, 95, 99, 99.5])
            thresholds[name] = {p: np.percentile(valid, p) for p in [80, 85, 90, 93, 95, 97, 99, 99.5]}
            if name == 'magnitude':
                print(f"  {name:<20} {pcts[0]:>8,.0f} {pcts[1]:>8,.0f} {pcts[2]:>8,.0f} "
                      f"{pcts[3]:>8,.0f} {pcts[4]:>8,.0f} {pcts[5]:>8,.0f}")
            else:
                print(f"  {name:<20} {pcts[0]:>8.2f} {pcts[1]:>8.2f} {pcts[2]:>8.2f} "
                      f"{pcts[3]:>8.2f} {pcts[4]:>8.2f} {pcts[5]:>8.2f}")

    print(f"\n  Days to expiry: P10={np.percentile(dte_arr, 10):.0f}, "
          f"P25={np.percentile(dte_arr, 25):.0f}, P50={np.median(dte_arr):.0f}, "
          f"P75={np.percentile(dte_arr, 75):.0f}")
    print(f"  Entry price:    P10=${np.percentile(entry_arr, 10):.2f}, "
          f"P25=${np.percentile(entry_arr, 25):.2f}, P50=${np.median(entry_arr):.2f}, "
          f"P75=${np.percentile(entry_arr, 75):.2f}")

    # =========================================================================
    # SECTION 4: REVERSE ENGINEERING — WHAT DO WINNERS LOOK LIKE?
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 4: REVERSE ENGINEERING — WHAT DO RAPID WINNERS LOOK LIKE?")
    print("=" * 80)

    # Winners at various speeds
    for win_label, win_filter in [
        ("TP100 within 1d", lambda ev: ev.get('max_return_1d', -100) >= 100),
        ("TP100 within 2d", lambda ev: ev.get('max_return_2d', -100) >= 100),
        ("TP100 within 3d", lambda ev: ev.get('max_return_3d', -100) >= 100),
        ("TP200 within 3d", lambda ev: ev.get('max_return_3d', -100) >= 200),
        ("TP500 within 5d", lambda ev: ev.get('max_return_5d', -100) >= 500),
    ]:
        winners = [ev for ev in events if win_filter(ev)]
        print(f"\n  --- {win_label}: {len(winners)} events ({len(winners)/len(events)*100:.2f}%) ---")

        if len(winners) < 5:
            print("    Too few winners for analysis")
            continue

        print(f"    ~{len(winners)/len(dates):.1f}/day")

        print(f"    {'Factor':<20} {'Winner Med':>12} {'All Med':>12} {'Ratio':>8} {'Winner P25':>12} {'Winner P75':>12}")
        print("    " + "-" * 80)

        for name in ['z_score', 'vol_oi_score', 'volume_score', 'otm_score',
                      'directional_score', 'time_score', 'magnitude']:
            field = 'volume_oi_ratio_score' if name == 'vol_oi_score' else \
                    'total_magnitude' if name == 'magnitude' else name
            w_vals = np.array([float(ev.get(field) or 0) for ev in winners])
            a_vals = factor_arrays[name]

            w_med = np.median(w_vals)
            a_med = np.median(a_vals[a_vals > 0]) if np.any(a_vals > 0) else 0
            ratio = w_med / a_med if a_med > 0 else 0
            w_p25 = np.percentile(w_vals, 25)
            w_p75 = np.percentile(w_vals, 75)

            if name == 'magnitude':
                print(f"    {name:<20} ${w_med:>10,.0f} ${a_med:>10,.0f} {ratio:>7.2f}x ${w_p25:>10,.0f} ${w_p75:>10,.0f}")
            else:
                print(f"    {name:<20} {w_med:>12.2f} {a_med:>12.2f} {ratio:>7.2f}x {w_p25:>12.2f} {w_p75:>12.2f}")

        # DTE and entry price
        w_dte = np.array([ev['days_to_expiry'] for ev in winners])
        w_entry = np.array([float(ev['entry_price']) for ev in winners])
        print(f"    {'days_to_expiry':<20} {np.median(w_dte):>12.0f} {np.median(dte_arr):>12.0f} "
              f"{np.median(w_dte)/np.median(dte_arr) if np.median(dte_arr) > 0 else 0:>7.2f}x "
              f"{np.percentile(w_dte, 25):>12.0f} {np.percentile(w_dte, 75):>12.0f}")
        print(f"    {'entry_price':<20} ${np.median(w_entry):>11.2f} ${np.median(entry_arr):>11.2f} "
              f"{np.median(w_entry)/np.median(entry_arr) if np.median(entry_arr) > 0 else 0:>7.2f}x "
              f"${np.percentile(w_entry, 25):>11.2f} ${np.percentile(w_entry, 75):>11.2f}")

        # What percentile are winners at?
        print(f"\n    Where winners sit in the overall distribution:")
        for name in ['z_score', 'vol_oi_score', 'volume_score', 'otm_score',
                      'directional_score', 'time_score']:
            field = 'volume_oi_ratio_score' if name == 'vol_oi_score' else name
            w_vals = [float(ev.get(field) or 0) for ev in winners]
            all_vals = factor_arrays[name]
            # What percentile is the winner median in the overall distribution?
            w_med = np.median(w_vals)
            pctl = np.searchsorted(np.sort(all_vals), w_med) / len(all_vals) * 100
            print(f"    {name:<20} winner median={w_med:.2f} sits at P{pctl:.0f} of all events")

    # =========================================================================
    # SECTION 5: INSIDER SIGNATURE FILTERS
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 5: INSIDER SIGNATURE COMBINATIONS")
    print("=" * 80)
    print("  Testing specific combinations of insider trading indicators.")
    print("  Goal: few events/day with high rapid-return rates.")

    def make_filter(criteria):
        """Build a filter function from a dict of criteria."""
        def filt(ev):
            for key, check in criteria.items():
                if not check(ev):
                    return False
            return True
        return filt

    # Build signatures from the insider trading literature
    signatures = {}

    # 1. Classic: extreme anomaly + fresh money + OTM + short-dated
    signatures['Classic Insider\n  z99 + volOI95 + otm90 + DTE<=14'] = make_filter({
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][95],
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
        'dte': lambda ev: ev['days_to_expiry'] <= 14,
    })

    # 2. Short fuse: very near-term + anomaly + directional + OTM
    signatures['Short Fuse\n  DTE<=7 + z95 + dir90 + otm90'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 7,
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][95],
        'dir': lambda ev: float(ev['directional_score'] or 0) >= thresholds['directional_score'][90],
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
    })

    # 3. Fresh OTM sweep: very high vol:OI + OTM + anomalous
    signatures['Fresh OTM Sweep\n  volOI97 + otm90 + z95'] = make_filter({
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][97],
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][95],
    })

    # 4. Cheap + fresh + extreme: penny options with massive anomaly
    signatures['Cheap Conviction\n  entry<=$0.50 + z99 + volOI95'] = make_filter({
        'price': lambda ev: float(ev['entry_price']) <= 0.50,
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][95],
    })

    # 5. Maximum conviction: ALL flags lit
    signatures['Max Conviction\n  z99 + volOI97 + otm95 + dir95 + DTE<=14'] = make_filter({
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][97],
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][95],
        'dir': lambda ev: float(ev['directional_score'] or 0) >= thresholds['directional_score'][95],
        'dte': lambda ev: ev['days_to_expiry'] <= 14,
    })

    # 6. Needle in haystack: ultra-extreme everything
    signatures['Ultra Needle\n  z99.5 + volOI97 + DTE<=7 + otm95'] = make_filter({
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][99.5],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][97],
        'dte': lambda ev: ev['days_to_expiry'] <= 7,
        'otm': lambda ev: float(ev['otm_score'] or 0) >= thresholds['otm_score'][95],
    })

    # 7. Data-driven from Section 4: use what winners actually look like
    # (thresholds will be filled after we see winner profiles)
    signatures['Winner Profile\n  z95 + volOI90 + DTE<=14 + entry<=$1'] = make_filter({
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][95],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
        'dte': lambda ev: ev['days_to_expiry'] <= 14,
        'price': lambda ev: float(ev['entry_price']) <= 1.00,
    })

    # 8. Relaxed version for more volume
    signatures['Elevated\n  z95 + volOI90 + 2of(otm90,dir90,DTE<=14)'] = make_filter({
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][95],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
        'combo': lambda ev: sum([
            float(ev['otm_score'] or 0) >= thresholds['otm_score'][90],
            float(ev['directional_score'] or 0) >= thresholds['directional_score'][90],
            ev['days_to_expiry'] <= 14,
        ]) >= 2,
    })

    # 9. Time-pressure sweep: very short-dated + extreme volume
    signatures['Expiry Sweep\n  DTE<=5 + z97 + volOI90 + entry<=$2'] = make_filter({
        'dte': lambda ev: ev['days_to_expiry'] <= 5,
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][97],
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
        'price': lambda ev: float(ev['entry_price']) <= 2.00,
    })

    # 10. Directional conviction: extreme bias + anomaly + short-dated
    signatures['Directional Blast\n  dir95 + z97 + DTE<=14 + volOI90'] = make_filter({
        'dir': lambda ev: float(ev['directional_score'] or 0) >= thresholds['directional_score'][95],
        'z_score': lambda ev: float(ev['z_score'] or 0) >= thresholds['z_score'][97],
        'dte': lambda ev: ev['days_to_expiry'] <= 14,
        'vol_oi': lambda ev: float(ev['volume_oi_ratio_score'] or 0) >= thresholds['vol_oi_score'][90],
    })

    # Run all signatures
    sig_summary = []
    for sig_name, sig_filter in signatures.items():
        filtered = [ev for ev in events if sig_filter(ev)]
        n = len(filtered)
        clean_name = sig_name.replace('\n', ' ').strip()

        if n < 3:
            print(f"\n  === {clean_name} === n={n} (too few, skipping)")
            sig_summary.append((clean_name, n, 0, 0, 0, 0))
            continue

        epd = n / len(dates)
        print(f"\n  === {clean_name} ===")
        print(f"  n={n} ({n/len(events)*100:.2f}% of events), ~{epd:.1f}/day")

        sig_results = compute_hit_matrix(filtered, tp_targets, windows)
        print_matrix(sig_results, tp_targets, windows, n, "Hit rates")
        print_lift_matrix(sig_results, baseline, tp_targets, windows, n, "Lift vs baseline")

        # Key metric extraction for summary
        tp100_3d = sig_results['TP100_3']['hits'] / n * 100 if n > 0 else 0
        tp100_1d = sig_results['TP100_1']['hits'] / n * 100 if n > 0 else 0
        tp50_3d = sig_results['TP50_3']['hits'] / n * 100 if n > 0 else 0

        bl_tp100_3d = baseline['TP100_3']['hits'] / baseline['TP100_3']['total'] * 100
        lift_tp100_3d = tp100_3d / bl_tp100_3d if bl_tp100_3d > 0 else 0

        print(f"\n  KEY: TP100@3d = {fmt_ci(sig_results['TP100_3']['hits'], n)}, lift={lift_tp100_3d:.2f}x")

        sig_summary.append((clean_name, n, epd, tp100_3d, tp50_3d, lift_tp100_3d))

    # =========================================================================
    # SECTION 6: SUMMARY RANKING
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 6: SIGNATURE RANKING")
    print("=" * 80)

    bl_tp100_3d = baseline['TP100_3']['hits'] / baseline['TP100_3']['total'] * 100
    bl_tp50_3d = baseline['TP50_3']['hits'] / baseline['TP50_3']['total'] * 100

    print(f"\n  Baseline: TP100@3d = {bl_tp100_3d:.1f}%, TP50@3d = {bl_tp50_3d:.1f}%")
    print(f"\n  {'Rank':<5} {'Signature':<50} {'n':>5} {'/day':>6} {'TP100@3d':>10} {'TP50@3d':>10} {'Lift':>8}")
    print("  " + "-" * 100)

    ranked = sorted(sig_summary, key=lambda x: -x[3])
    for i, (name, n, epd, tp100, tp50, lift) in enumerate(ranked, 1):
        short = name.split('  ')[0].strip()
        print(f"  {i:<5} {short:<50} {n:>5} {epd:>5.1f} {tp100:>9.1f}% {tp50:>9.1f}% {lift:>7.2f}x")

    # =========================================================================
    # SECTION 7: WHAT WOULD IT TAKE?
    # =========================================================================
    print("\n" + "=" * 80)
    print("SECTION 7: BREAKEVEN ANALYSIS — WHAT HIT RATE DO WE NEED?")
    print("=" * 80)
    print("""
  For binary TP strategies (gain X% or lose 100%):
    TP30:  need >77% hit rate for +EV  (risk $1 to make $0.30)
    TP50:  need >67% hit rate for +EV  (risk $1 to make $0.50)
    TP100: need >50% hit rate for +EV  (risk $1 to make $1.00)
    TP200: need >33% hit rate for +EV  (risk $1 to make $2.00)
    TP500: need >17% hit rate for +EV  (risk $1 to make $5.00)

  BUT: options don't always go to zero on loss.
  With a stop-loss or partial recovery, thresholds drop.
  Example: if avg loss is 50% (not 100%):
    TP100: need >33% hit rate for +EV
    TP200: need >20% hit rate for +EV
""")

    # Compute actual average loss for events that don't hit various TPs
    for tp, w in [(100, 3), (100, 5), (50, 3)]:
        non_winners = [ev for ev in events
                       if ev.get(f'max_return_{w}d', -100) < tp]
        if non_winners:
            # What's the max return these "losers" got?
            loser_returns = [ev.get(f'max_return_{w}d', -100) for ev in non_winners]
            print(f"  Non-TP{tp}@{w}d outcomes (n={len(non_winners)}):")
            print(f"    Median max return: {np.median(loser_returns):+.1f}%")
            print(f"    P25 max return:    {np.percentile(loser_returns, 25):+.1f}%")
            print(f"    P75 max return:    {np.percentile(loser_returns, 75):+.1f}%")
            print(f"    % that had >0% return: {sum(1 for r in loser_returns if r > 0)/len(loser_returns)*100:.1f}%")
            print()

    # =========================================================================
    # VISUALIZATION
    # =========================================================================
    print("Creating visualization...")

    fig, axes = plt.subplots(2, 2, figsize=(20, 16))
    fig.suptitle('Rapid Return & Insider Signature Analysis\n'
                 'Using intraday highs | Time-windowed returns',
                 fontsize=14, fontweight='bold')

    # 1. Baseline heatmap
    ax1 = axes[0, 0]
    matrix = np.zeros((len(windows), len(tp_targets)))
    for i, w in enumerate(windows):
        for j, tp in enumerate(tp_targets):
            key = f'TP{tp}_{w}'
            h = baseline[key]
            matrix[i, j] = h['hits'] / h['total'] * 100 if h['total'] > 0 else 0

    im = ax1.imshow(matrix, cmap='RdYlGn', aspect='auto', vmin=0, vmax=max(40, matrix.max()))
    ax1.set_xticks(range(len(tp_targets)))
    ax1.set_xticklabels([f'TP{tp}%' for tp in tp_targets])
    ax1.set_yticks(range(len(windows)))
    ax1.set_yticklabels([f'{w}d' if w != 'expiry' else 'Expiry' for w in windows])
    for i in range(len(windows)):
        for j in range(len(tp_targets)):
            color = 'white' if matrix[i, j] > 25 or matrix[i, j] < 3 else 'black'
            ax1.text(j, i, f'{matrix[i, j]:.1f}%', ha='center', va='center',
                     fontsize=10, fontweight='bold', color=color)
    ax1.set_title(f'Baseline Hit Rates (n={len(events)})\nAll events, intraday highs', fontsize=11)
    plt.colorbar(im, ax=ax1, shrink=0.8)

    # 2. Signature comparison at TP100@3d
    ax2 = axes[0, 1]
    valid_sigs = [(name, n, epd, tp100, tp50, lift) for name, n, epd, tp100, tp50, lift in ranked if n >= 3]
    if valid_sigs:
        names_short = [s[0].split('  ')[0].strip()[:25] for s in valid_sigs]
        tp100_rates = [s[3] for s in valid_sigs]
        tp50_rates = [s[4] for s in valid_sigs]
        counts = [s[1] for s in valid_sigs]

        y_pos = np.arange(len(valid_sigs))
        bars1 = ax2.barh(y_pos - 0.15, tp100_rates, 0.3, label='TP100 @ 3d', color='#4CAF50', alpha=0.9)
        bars2 = ax2.barh(y_pos + 0.15, tp50_rates, 0.3, label='TP50 @ 3d', color='#2196F3', alpha=0.9)

        for i, n in enumerate(counts):
            max_rate = max(tp100_rates[i], tp50_rates[i])
            ax2.text(max_rate + 0.3, i, f'n={n}', va='center', fontsize=8)

        ax2.axvline(bl_tp100_3d, color='green', linestyle='--', alpha=0.6,
                     label=f'BL TP100={bl_tp100_3d:.1f}%')
        ax2.axvline(bl_tp50_3d, color='blue', linestyle='--', alpha=0.6,
                     label=f'BL TP50={bl_tp50_3d:.1f}%')
        ax2.axvline(50, color='red', linestyle='-', alpha=0.3, label='TP100 breakeven (50%)')

        ax2.set_yticks(y_pos)
        ax2.set_yticklabels(names_short, fontsize=8)
        ax2.set_xlabel('Hit Rate (%)')
        ax2.set_title('Signature Performance (3-day window)', fontsize=11)
        ax2.invert_yaxis()
        ax2.legend(fontsize=7, loc='lower right')

    # 3. Entry price effect on TP100
    ax3 = axes[1, 0]
    band_data = []
    for label, lo, hi in price_bands:
        band = [ev for ev in events if lo <= float(ev['entry_price']) < hi]
        if len(band) >= 10:
            for w in [1, 2, 3, 5]:
                hits = sum(1 for ev in band if ev.get(f'max_return_{w}d', -100) >= 100)
                band_data.append((label, w, hits / len(band) * 100, len(band)))

    if band_data:
        import pandas as pd
        bd = pd.DataFrame(band_data, columns=['band', 'window', 'rate', 'n'])
        for band_label in [b[0] for b in price_bands]:
            sub = bd[bd['band'] == band_label]
            if not sub.empty:
                ax3.plot(sub['window'], sub['rate'], 'o-', label=f"{band_label} (n={sub['n'].iloc[0]})")
        ax3.set_xlabel('Days after signal')
        ax3.set_ylabel('TP100 Hit Rate (%)')
        ax3.set_title('TP100 by Entry Price Band & Time Window', fontsize=11)
        ax3.legend(fontsize=8)
        ax3.grid(alpha=0.3)
        ax3.set_xticks([1, 2, 3, 5])

    # 4. Summary text
    ax4 = axes[1, 1]
    ax4.axis('off')

    # Find best signature
    best = ranked[0] if ranked else None
    summary_text = f"""RAPID RETURN ANALYSIS RESULTS

DATA: {len(events)} events, {len(dates)} trading days
Date range: {dates[0]} to {dates[-1]}

BASELINE (all events):
  TP100 to expiry:  {baseline['TP100_expiry']['hits']/len(events)*100:.1f}%
  TP100 @ 3 days:   {bl_tp100_3d:.1f}%
  TP50  @ 3 days:   {bl_tp50_3d:.1f}%
  TP100 @ 1 day:    {baseline['TP100_1']['hits']/len(events)*100:.1f}%
"""

    if best and best[1] > 0:
        summary_text += f"""
BEST SIGNATURE: {best[0].split('  ')[0].strip()}
  n={best[1]}, ~{best[2]:.1f}/day
  TP100@3d: {best[3]:.1f}% (lift {best[5]:.2f}x)
  TP50@3d:  {best[4]:.1f}%
"""

    summary_text += f"""
BREAKEVEN THRESHOLDS:
  TP100: need >50% hit rate
  TP200: need >33% hit rate
  TP500: need >17% hit rate

KEY QUESTION: Can any signature
reach the breakeven threshold?
"""

    ax4.text(0.02, 0.98, summary_text, transform=ax4.transAxes, fontsize=9.5,
             verticalalignment='top', fontfamily='monospace',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

    plt.tight_layout()
    plt.savefig('rapid_return_analysis.png', dpi=150, bbox_inches='tight')
    print("Saved: rapid_return_analysis.png")

    conn.close()
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    main()
