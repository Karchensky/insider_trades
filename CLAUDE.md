# CLAUDE.md — Insider Trades / Options Signal System

This file orients AI assistants and humans to this repository: goals, architecture, scoring reality, and safe workflows.

## Product goal

Detect **anomalous options flow** that may indicate insider trading. Produce **low-frequency, high-conviction email alerts** with enrichment context for **human review**. This is a research/watchlist tool — the human makes the final trading decision.

**Evaluation rule** (for backtesting):
1. **Enter** on signal day at the **recommended contract's close**.
2. **Exit** the first later session where **close >= 2x entry** ("TP100"), **or** hold to **expiration** (often full loss).

## Runtime layout

| Entry point | Role |
|-------------|------|
| `intraday_schedule.py` | Every ~15 min: Polygon snapshots -> `temp_*` -> `InsiderAnomalyDetector` -> `daily_anomaly_snapshot` -> enrichment -> optional email |
| `daily_schedule.py` | Morning: daily OHLC, flat-file options, contract sync, Greeks/OI backfill, actionability flags, retention |

**Data flow:** live API -> `temp_stock` / `temp_option` -> scoring -> `daily_anomaly_snapshot`; S3 flat files -> `daily_option_snapshot` + Greeks/OI enrichment from temp.

## Core tables

- **`temp_stock` / `temp_option`** — intraday only; truncated after daily job.
- **`daily_stock_snapshot` / `daily_option_snapshot`** — historical OHLC; options get Greeks/IV/OI from temp + daily jobs.
- **`option_contracts`** — contract metadata.
- **`daily_anomaly_snapshot`** — one row per `(event_date, symbol, as_of_timestamp)` with scores, magnitudes, **recommended_option**, Greek values/flags, filters.
- **`earnings_calendar`** — earnings dates for proximity filtering.

Retention: **~90 business days** for snapshots (see `DataRetentionManager`).

## Scoring architecture: Two-tier system

### Tier 1 — Event scoring (symbol-level, gates alerts)

`high_conviction_score` (0-4) in `database/analysis/insider_anomaly_detection.py`:
- `volume_score >= 2.0` (volume anomaly z-score)
- `z_score >= 3.0` (statistical deviation from baseline)
- `vol_oi_ratio_score >= 1.2` (fresh positioning signal)
- `total_magnitude >= $50K` (institutional-scale flow)

Alert fires when **3+ of 4** factors met, **plus** not bot-driven and not earnings-related.

### Tier 2 — Contract selection (picks recommended option)

Among tradeable contracts ($0.05-$5.00, vol > 50, direction-aligned), selects by **max_volume** (most liquid, default). Alternative strategies (max_gamma, best_rr, atm_preference, model_ranked) configurable via `CONTRACT_SELECTION_STRATEGY` env var.

Greek values (gamma, vega, theta) are stored in `daily_anomaly_snapshot` for tracking. They are useful for contract **selection**, not for event **detection**.

### Enrichment (signal context for human review)

`enrichment/signal_enrichment.py` runs before email alerts:
- **Symbol novelty** (`enrichment/novelty.py`): How often this ticker triggers. First-time triggers score highest.
- **Polygon news** (`enrichment/polygon_news.py`): Recent news check. No-news anomalies are more suspicious.
- **SEC EDGAR Form 4** (`enrichment/edgar_insider.py`): Corporate insider filings. Alignment with anomaly direction increases conviction.

Conviction modifiers (-3 to +3) are computed and displayed in the email alert.

### Legacy composite (`total_score`, 0-10)

Volume, vol:OI, OTM, direction, time. Still computed and stored for reference. Individual components feed into the event scoring tier.

## Analysis findings (March 2026, n=35,240 events)

**Key results from comprehensive analysis:**
- Baseline TP100 rate: 24.2% (option doubles by expiry)
- **Scoring factors do not discriminate winners.** TP500+/TP1000+ winners sit at median P50-P54 on all factors (z_score, otm_score, vol_oi_score, volume_score).
- **Best lift**: 1.15x for first-time triggers, 1.24x for put-heavy direction, 1.24x for DTE 4-14.
- **Stock move prediction**: OTM-heavy + moderate z-score predicts volatility at 1.52x lift, but direction accuracy is 45.7% (below random).
- **No mechanical +EV** found at any signature with meaningful sample size. Best signature (FIRST+otm90) achieves 6.1% TP500 rate (needs 16.7% for breakeven).
- **Value**: The system detects unusual activity. Enrichment context (novelty, news, EDGAR) helps the human reviewer assess whether to act.

Analysis scripts in `analysis/`: `enriched_signal_analysis.py`, `stock_move_analysis.py`, `extreme_return_analysis.py`, `rapid_return_analysis.py`.

## Documentation map

| Doc | Purpose |
|-----|---------|
| `README.md` | Operator quick start, configuration, architecture |
| `breakdown.md` | Long-form system breakdown (data model, timing, scoring) |
| `docs/VALIDATED_SCORING_SYSTEM.md` | Historical: factor analysis showing volume metrics > Greeks |
| `docs/CLAUDE_CODE_CLI.md` | Claude Code CLI setup and usage |

## Environment

- **`SUPABASE_DB_URL`** — PostgreSQL connection string (required).
- **`POLYGON_API_KEY`** — Polygon.io API key (required).
- **`POLYGON_S3_ACCESS_KEY` / `POLYGON_S3_SECRET_KEY`** — for flat-file option data.
- **Email** — `SENDER_EMAIL`, `EMAIL_PASSWORD`, `RECIPIENT_EMAIL`, `ANOMALY_EMAIL_ENABLED`.

Use **`python-dotenv`**; local `.env` is not committed.

## Commands (from repo root)

```bash
pip install -r requirements.txt

# Intraday (market hours)
python intraday_schedule.py

# Daily pipeline
python daily_schedule.py --recent 3 --retention 90

# Migrations
python -c "from migrations.migration_manager import MigrationManager; MigrationManager().migrate()"

# Validation
python analysis.py --skip-contracts    # Event scoring validation (fast)
python analysis/enriched_signal_analysis.py  # Enrichment feature analysis
```

## Database: timeouts and migrations

- **`ALTER TABLE` can still "timeout"** if the session hits **`statement_timeout`** or if **another session holds locks**.
- Before migrations: **terminate long-running app queries** via Supabase SQL or dashboard.
- Prefer **`ADD COLUMN IF NOT EXISTS`** in small steps.

## Code conventions

- **Match existing style** in touched files; avoid drive-by refactors.
- **Prefer absolute paths** in tool args when the user/workspace uses them.
- **Heavy analytics**: batch by symbol/date or pre-aggregate; avoid correlated subqueries over full `daily_option_snapshot`.
- **Dependencies**: if you add imports, update **`requirements.txt`**.

## Email and Streamlit

- **`notifications/email_notifier.py`** — alerts fire on event score >= 3, magnitude >= $20K, not bot-driven, not earnings-related. Includes enrichment context.
- **`app/streamlit_app.py`** — "High Conviction" view shows event-scored alerts with contract recommendations.

## When changing the signal definition

1. Update **detection** (Python or SQL).
2. Update **alerts/UI/docs** in the same change.
3. Re-run analysis scripts on a defined date range and document **sample size** and **filters**.
4. If schema changes needed, add a **migration** and ensure **`_store_anomalies_bulk`** column lists match.

## AI / IDE setup

| Artifact | Role |
|----------|------|
| **`CLAUDE.md`** (this file) | Canonical project brief for any assistant; keep current. |
| **`AGENTS.md`** | Short index + pointer here. |
| **`.cursor/skills/`** | Cursor-only skill definitions. |

**Claude Code CLI** reference: **`docs/CLAUDE_CODE_CLI.md`**.

---

*Last updated: March 2026. Two-tier architecture + enrichment pipeline.*
