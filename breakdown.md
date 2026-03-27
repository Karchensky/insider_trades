## Insider Trades System — Technical Breakdown

Detailed reference for the data model, scheduling, timing, and scoring logic. For quick start and configuration, see `README.md`. For AI/contributor context, see `CLAUDE.md`.

---

## 1. Core Objective

Detect anomalous options flow that may indicate insider trading. Produce low-frequency, high-conviction email alerts with enrichment context for human review.

**Evaluation rule** (for backtesting): buy recommended contract at signal-day close; exit first day close >= 2x entry (TP100); else hold to expiration (typically full loss).

---

## 2. Data Model & Tables

### Intraday (short-lived)

**`temp_stock`**
- Source: Polygon `/v2/snapshot/locale/us/markets/stocks/tickers` via `FullMarketSnapshotScraper`.
- Columns: `as_of_timestamp`, `symbol`, `day_open`, `day_high`, `day_low`, `day_close`, `day_volume`, `day_vwap`.
- Primary key: `(as_of_timestamp, symbol)`.
- Truncated after daily job.

**`temp_option`**
- Source: Polygon `/v3/snapshot` (`type=options`, `ticker.any_of`) via `UnifiedOptionsSnapshotScraper`.
- Columns: contract info (`symbol`, `contract_ticker`, `contract_type`, `strike_price`, `expiration_date`), session OHLCV, Greeks (`greeks_delta/gamma/theta/vega`, sanitized), IV, OI, underlying price.
- Primary key: `(as_of_timestamp, symbol, contract_ticker)`.
- Truncated after daily job.

### Daily (historical, ~90 business day retention)

**`daily_stock_snapshot`**
- Source: Polygon `/v2/aggs/grouped/locale/us/market/stocks/{date}` via `PolygonDailyScraper`.
- One row per `(date, symbol)` with OHLCV + VWAP.

**`daily_option_snapshot`**
- Source: Polygon S3 flat files (options day aggregates) via `PolygonOptionFlatFileLoader`.
- One row per `(date, contract_ticker)` with daily OHLCV + volume.
- Enriched post-load with: Greeks/IV from `temp_option`, open interest from fresh temp snapshot.

**`option_contracts`**
- Source: Polygon `/v3/reference/options/contracts` via `PolygonOptionContractsScraper`.
- Contract metadata: symbol, type, strike, expiration, exercise style, shares per contract.
- Updated incrementally (only symbols with new contracts in `daily_option_snapshot`).

**`earnings_calendar`**
- Source: yfinance via `EarningsCalendarScraper`.
- Columns: `symbol`, `earnings_date`, `time_of_day`.
- Used for earnings proximity filtering (4-day window).

### Anomaly / signal table

**`daily_anomaly_snapshot`**
- Written by `InsiderAnomalyDetector._store_anomalies_bulk`.
- One row per `(event_date, symbol, as_of_timestamp)` (multiple intraday snapshots per day).
- Contains:
  - **Event scoring (Tier 1)**: `high_conviction_score` (0-4), `is_high_conviction`, `volume_score`, `z_score`, `volume_oi_ratio_score`, `total_magnitude`.
  - **Legacy composite**: `total_score` (0-10), `otm_score`, `directional_score`, `time_score`.
  - **Volume & size**: `call_volume`, `put_volume`, `total_volume`, `call_magnitude`, `put_magnitude`, `direction`.
  - **Open interest**: `open_interest`, `call/put_volume_oi_ratio`, `call/put_volume_oi_z_score`.
  - **Greeks tracking**: `greeks_theta/gamma/vega_value`, `greeks_*_percentile`, `greeks_*_met` flags.
  - **Contract recommendation**: `recommended_option`, `contract_candidates`, `selected_strategy`.
  - **Filters**: `is_bot_driven`, `is_earnings_related`, `is_actionable`, `intraday_price_move_pct`, `earnings_proximity_days`.

---

## 3. Scheduling & Data Flow

### 3.1 Intraday Process (every ~15 min during market hours)

Entry point: `intraday_schedule.py`

1. **Stock snapshot**: `FullMarketSnapshotScraper` -> `temp_stock` (11K+ stocks, 1 API call).
2. **Options snapshot**: Batch all contract tickers from latest `daily_option_snapshot`. Fetch concurrently via `UnifiedOptionsSnapshotScraper.fetch_by_tickers()`. Retry missing. Load to `temp_option`.
3. **Anomaly detection**: `InsiderAnomalyDetector.run_detection()`:
   - Join `temp_option` + `temp_stock` + `option_contracts`.
   - Compute 90-day baselines from `daily_option_snapshot`.
   - Score symbol-level metrics (volume anomaly, z-score, vol:OI, magnitude).
   - Select recommended contract (max_volume among tradeable).
   - Write to `daily_anomaly_snapshot`.
4. **Signal persistence**: Track consecutive snapshots where a symbol meets high-conviction threshold. Persistent signals (2+ snapshots) are more reliable.
5. **Enrichment + email**: For high-conviction alerts, enrich with novelty/news/EDGAR context, then send HTML email.

**Timing note**: Intraday temp data captures live Greeks/IV. Earnings and bot flags are fully populated by the daily job, not intraday.

### 3.2 Daily Process (once, morning after market close)

Entry point: `daily_schedule.py`

1. **Determine trading days**: Last N business days (default 3).
2. **Load daily stock OHLC**: `PolygonDailyScraper` -> `daily_stock_snapshot`.
3. **Load daily options OHLC**: S3 flat files -> `daily_option_snapshot`.
4. **Sync contract metadata**: `PolygonOptionContractsScraper.scrape_incremental_smart()` -> `option_contracts`.
5. **Fix symbol mismatches**: Align `daily_option_snapshot.symbol` to `option_contracts`.
6. **Backfill Greeks/IV**: From previous day's `temp_option` -> `daily_option_snapshot`.
7. **Refresh open interest**: Fresh `temp_option` (current day OI = prior close) -> previous day's `daily_option_snapshot`.
8. **Apply filters**: `intraday_price_move_pct`, `is_bot_driven` (>=5%), `is_earnings_related` (within 4 days), `is_actionable`.
9. **Truncate temp tables**: `temp_stock`, `temp_option`.
10. **Retention cleanup**: Delete rows older than ~90 business days from all snapshot tables.

**Key dependency**: The daily process MUST run after the last intraday snapshot of the previous day. It uses `temp_option` data (with Greeks) to enrich `daily_option_snapshot`, then truncates temp tables.

---

## 4. Scoring Architecture

### Tier 1 — Event Scoring (gates alerts)

`high_conviction_score` (0-4) counts how many factors exceed thresholds:

| Factor | Threshold | Source |
|--------|-----------|--------|
| `volume_score` | >= 2.0 | Z-score of daily call+put volume vs 90-day baseline |
| `z_score` | >= 3.0 | Max of call/put volume z-scores |
| `vol_oi_ratio_score` | >= 1.2 | Z-score of volume/OI ratio vs 90-day baseline |
| `total_magnitude` | >= $50,000 | Sum of (volume x price x shares_per_contract) for all contracts |

Alert fires when **score >= 3** AND `total_magnitude >= $20K` AND `is_bot_driven = FALSE` AND `is_earnings_related = FALSE`.

### Tier 2 — Contract Selection

Among tradeable contracts ($0.05-$5.00, volume > 50, direction-aligned), select by configured strategy:
- **max_volume** (default): Highest trading volume (most liquid).
- **max_gamma**: Highest gamma (closest to ATM).
- **best_rr**: Risk/reward ratio (gamma x vega / |theta|).
- **atm_preference**: Closest to ATM with sufficient volume.
- **model_ranked**: ML P(TP100) prediction (if model trained).

### Enrichment (context for human review)

`enrichment/signal_enrichment.py` adds context before email alerts:
- **Novelty**: Trigger frequency history. First-time/rare triggers score higher.
- **News**: Polygon news API. No-news anomalies suggest information asymmetry.
- **EDGAR**: SEC Form 4 filings. Corporate insider alignment increases conviction.

Conviction modifiers range from -3 to +3. Displayed in email alert.

### Legacy Composite (0-10)

Still computed for reference: `volume_score` + `vol_oi_score` + `otm_score` + `directional_score` + `time_score`. Not used for alerting in the current system.

---

## 5. Analysis Findings (March 2026)

Based on 35,240 events over 29 trading days:

- **Baseline TP100 rate**: 24.2%
- **Scoring factors** (z_score, vol_oi_score, volume_score, otm_score): Do not discriminate winners from losers. TP500+/TP1000+ winners sit at median P50-P54 on all factors.
- **Best individual lifts**: First-time triggers (1.15x), put-heavy direction (1.24x), DTE 4-14 (1.24x).
- **Stock move prediction**: OTM-heavy + moderate z-score predicts volatility events at 1.52x lift, but direction accuracy is 45.7%.
- **No mechanical +EV** achievable at any filter combination with meaningful sample size.
- **System value**: Research/watchlist tool. Enrichment context helps human reviewers assess whether to act.

---

## 6. Known Limitations

1. **Entry price timing**: Anomalies trigger midday but entry is modeled at EOD close. The option may have already moved significantly by close.
2. **Direction accuracy**: Call/put classification predicts stock direction at 45.7% (below random). Put-heavy signals are slightly more reliable (55.1%).
3. **Single-contract representation**: Symbol-level anomaly is compressed to one recommended_option. The best contract for the signal may not be the highest-volume one.
4. **Retention window**: ~90 business days. Longer-dated options may not have full outcome data before retention cleanup.
5. **Static thresholds**: Event scoring thresholds are fixed. Market regime changes (0DTE growth, vol regimes) can shift optimal values.
6. **Greeks not predictive for gating**: Comprehensive analysis confirmed Greeks sit at P50 for winners. They are useful for contract selection, not event detection.
