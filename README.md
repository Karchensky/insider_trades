# Insider Trading Detection System

Real-time options flow anomaly detection. Scans the full US options market every 15 minutes during trading hours, compares against 90-day statistical baselines, and emails high-conviction alerts with enrichment context (news, SEC filings, symbol novelty) for human review.

**This is a research/watchlist tool, not an automated trading system.** The alerts surface unusual activity; the human reviewer makes the final call.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (see Configuration below)
cp env.template.txt .env  # then edit with your keys

# Run database migrations (first time only)
python -c "from migrations.migration_manager import MigrationManager; MigrationManager().migrate()"

# Intraday monitoring (run during market hours, typically via cron/GitHub Actions)
python intraday_schedule.py

# Daily ETL (run once morning after market close)
python daily_schedule.py --recent 3 --retention 90
```

## How It Works

### Two-Tier Detection Architecture

**Tier 1 — Event Scoring** (symbol-level, gates alerts):

| Factor | Threshold | What It Captures |
|--------|-----------|------------------|
| Volume Score | >= 2.0 | Unusual options volume vs 90-day baseline |
| Z-Score | >= 3.0 | Statistical deviation from normal activity |
| Vol:OI Ratio | >= 1.2 | Fresh positioning (new positions, not churn) |
| Magnitude | >= $50,000 | Institutional-scale dollar volume |

Alert fires when **3+ of 4** factors exceed thresholds, AND the event is not bot-driven (<5% intraday stock move) and not earnings-related (>4 days from earnings).

**Tier 2 — Contract Selection** (picks recommended option):

Among tradeable contracts ($0.05-$5.00, volume > 50, direction-aligned), the system selects by highest volume (most liquid). Greeks (gamma, vega, theta) are stored for informational tracking but do not gate alerts.

**Enrichment** (context for human review):

Each alert is enriched with:
- **Symbol novelty**: How often this ticker triggers. First-time triggers on quiet stocks are more suspicious than daily triggers on volatile tickers.
- **Polygon news**: Recent news articles around the ticker. No-news anomalies suggest possible information asymmetry.
- **SEC EDGAR Form 4**: Recent corporate insider filings. Insider buying aligned with call-heavy anomalies increases conviction.

### Data Flow

```
Intraday (every 15 min):
  Polygon API  -->  temp_stock / temp_option  -->  Anomaly Detection  -->  Email Alert
                                                         |
                                                   daily_anomaly_snapshot

Daily (morning after close):
  S3 flat files  -->  daily_option_snapshot  -->  Greeks/OI backfill from temp
  Polygon API    -->  daily_stock_snapshot       Earnings flags, bot flags
                      option_contracts           Retention cleanup
```

The intraday process captures live snapshots with Greeks. The daily process loads historical OHLC from S3 flat files and backfills Greeks/OI from the previous day's temp data, building the 90-day baseline used by detection.

### Filtering

Excluded from alerts:
- **Bot-driven**: Stock already moved >= 5% intraday (news already priced in)
- **Earnings-related**: Within 4 days of earnings (speculation, not insider knowledge)
- **Low magnitude**: < $20K total option dollar volume

## Database Schema

| Table | Purpose | Retention |
|-------|---------|-----------|
| `temp_stock` | Intraday stock snapshots | Truncated after daily ETL |
| `temp_option` | Intraday option snapshots (with Greeks/IV) | Truncated after daily ETL |
| `daily_stock_snapshot` | Historical stock OHLCV | ~90 business days |
| `daily_option_snapshot` | Historical option OHLCV + Greeks/OI | ~90 business days |
| `daily_anomaly_snapshot` | Detection results + scores + recommendations | ~90 business days |
| `option_contracts` | Contract metadata (strike, expiry, type) | Until expiration |
| `earnings_calendar` | Earnings dates for proximity filtering | ~90 days |

### Key Columns in `daily_anomaly_snapshot`

**Event scoring**: `high_conviction_score` (0-4), `is_high_conviction` (bool), `volume_score`, `z_score`, `volume_oi_ratio_score`, `total_magnitude`

**Contract recommendation**: `recommended_option`, `direction` (call_heavy/put_heavy/mixed), `selected_strategy`

**Filters**: `is_bot_driven`, `is_earnings_related`, `is_actionable`, `intraday_price_move_pct`, `earnings_proximity_days`

**Legacy**: `total_score` (0-10 composite), `otm_score`, `directional_score`, `time_score` — still computed, not used for alerting

## Email Alerts

Sends when:
- Event score >= 3/4
- Magnitude >= $20K
- Not bot-driven, not earnings-related

Each email includes:
- Symbol, direction, event score breakdown
- Recommended contract ticker
- Volume and magnitude details
- **Signal context** (enrichment): novelty score, recent news, SEC insider filings, conviction modifier

## Streamlit Dashboard

```bash
cd app
streamlit run streamlit_app.py
```

Live at: https://bk-insidertrades.streamlit.app

**High Conviction tab**: Event-scored alerts with factor breakdown and contract recommendations.
**Legacy tab**: Historical composite-score view for comparison.
**Performance Overview**: TP100 hit rates and forward return analysis.

## Configuration

Required environment variables (see `env.template.txt`):

```bash
# Database (required)
SUPABASE_DB_URL=postgresql://user:pass@host:port/db

# Polygon API (required)
POLYGON_API_KEY=your_key
POLYGON_S3_ACCESS_KEY=s3_key
POLYGON_S3_SECRET_KEY=s3_secret

# Email alerts (optional)
SENDER_EMAIL=alerts@domain.com
EMAIL_PASSWORD=app_password
RECIPIENT_EMAIL=you@domain.com
ANOMALY_EMAIL_ENABLED=true

# Tuning (optional, defaults shown)
CONTRACT_SELECTION_STRATEGY=max_volume
INTRADAY_OPTIONS_LIMIT=250
INTRADAY_OPTIONS_BATCH_CALLS=100
INTRADAY_OPTIONS_WORKERS=20
```

## Scheduled Runs (GitHub Actions)

- **Intraday**: `*/15 14-21 * * 1-5` (every 15 min, ~9:45 AM - 4:45 PM EST, Mon-Fri)
- **Daily**: `0 13 * * 1-6` (8:00 AM EST, Mon-Sat)

## Migrations

Schema changes are versioned in `migrations/`. Run all pending:

```bash
python -c "from migrations.migration_manager import MigrationManager; MigrationManager().migrate()"
```

## Analysis & Validation

```bash
# Two-tier event scoring validation (Wilson CIs, walk-forward)
python analysis.py                    # Full (slow for contract comparison)
python analysis.py --skip-contracts   # Fast (event scoring only)

# Factor analysis (which factors predict outcomes)
python analysis/comprehensive_factor_analysis.py --days 90 --limit 3000

# Enrichment feature testing (novelty, direction, stock price, DTE)
python analysis/enriched_signal_analysis.py

# Stock move prediction (do anomalies predict underlying stock moves?)
python analysis/stock_move_analysis.py

# Extreme return analysis (TP500+/TP1000+ targets)
python analysis/extreme_return_analysis.py
```

### What the Analysis Shows (March 2026)

Based on 35,240 events over 29 trading days:
- **Baseline TP100 rate**: 24.2% (option doubles by expiry)
- **Best factor lift**: 1.15x (first-time triggers), 1.24x (put-heavy direction)
- **Scoring factors**: Do not discriminate winners from losers. TP500+/TP1000+ winners sit at P50 on all factors.
- **Stock move prediction**: OTM-heavy + moderate z-score predicts volatility events at 1.52x lift, but direction accuracy is 45.7% (below random).
- **Enrichment value**: Context for human judgment (novelty, news, EDGAR), not mechanical filtering.

The system detects unusual activity. Whether that activity is profitable depends on the human reviewer's judgment, timing, and risk management.

## Project Structure

```
insider_trades/
  intraday_schedule.py        # Entry point: 15-min snapshot + detection + alerts
  daily_schedule.py           # Entry point: morning ETL + enrichment + retention
  analysis.py                 # Two-tier validation framework
  database/
    core/                     # DB connection, bulk ops, stock data manager
    analysis/                 # InsiderAnomalyDetector (core scoring logic)
    maintenance/              # Data retention manager
  scrapers/                   # Polygon API + S3 flat file loaders
  notifications/              # Email notifier with enrichment integration
  enrichment/                 # Signal enrichment (news, EDGAR, novelty)
  app/                        # Streamlit dashboard
  config/                     # Contract selection strategy config
  migrations/                 # Versioned schema migrations
  analysis/                   # Research & validation scripts
  docs/                       # Reference documentation
```

## Troubleshooting

**No email alerts**: Check `ANOMALY_EMAIL_ENABLED=true`, verify SMTP credentials, confirm `is_high_conviction=TRUE` rows exist in `daily_anomaly_snapshot` (may be filtered by bot/earnings flags).

**Greeks not populating**: Ensure daily process runs AFTER intraday (it backfills from temp tables). Check `temp_option` has data: `SELECT COUNT(*) FROM temp_option WHERE greeks_gamma IS NOT NULL`.

**API errors**: Polygon API has occasional degradation. Built-in retry logic handles transient failures. Check https://polygon.io/status. Reduce `--options-workers` if hitting rate limits.

**Migration timeouts**: Usually caused by locks from long-running queries, not slow DDL. Kill stuck queries in Supabase dashboard first.

## License

Research/educational purposes only. Not financial advice. Options trading carries significant risk. Comply with all applicable securities laws.
