# Insider Trading Detection System

Real-time options flow analysis to detect unusual institutional activity. Pulls full-market snapshots every 15 minutes, compares against 90-day baselines, and sends email alerts when greeks indicate high-conviction plays.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Intraday monitoring (market hours)
python intraday_schedule.py

# Daily ETL (runs once after market close)
python daily_schedule.py --recent 3 --retention 90
```

## Architecture Overview

### Daily vs Intraday

**Daily Process** (`daily_schedule.py` - runs 9 AM EST):
- Loads historical OHLC from Polygon S3 flat files (stock + options)
- Transfers greeks from yesterday's intraday snapshots to permanent storage
- Updates contract metadata, applies retention policies
- Flags bot-driven and earnings-related activity

**Intraday Process** (`intraday_schedule.py` - every 15 min):
- Fetches full market snapshot (11K stocks, 300K+ option contracts)
- Runs anomaly detection against 90-day baseline
- Sends email alerts for high-conviction signals
- Preserves temp data for next morning's daily process

**Data Flow:**
```
Intraday:  Live API → temp_option (with greeks) → Anomaly Detection → Email
Daily:     S3 OHLC → daily_option_snapshot + temp_option greeks → Baseline DB
```

The intraday process captures real-time greeks that the daily process uses the next morning to enrich historical data for baseline calculations.

## Detection Algorithm

### Greeks-Based High Conviction Score (Primary System)

Scores 0-4 based on how many factors exceed 93rd percentile thresholds. Send email alert if score >= 3.

| Greek | Threshold | What It Captures | Why It Matters |
|-------|-----------|------------------|----------------|
| **Theta** | >= 0.1624 | Time decay rate | High theta = near expiration → urgency/conviction. Insiders buy short-dated options when they expect imminent move. |
| **Gamma** | >= 0.4683 | Delta acceleration | High gamma = near ATM strikes → precise timing. Not lottery tickets, actual conviction at current price. |
| **Vega** | >= 0.1326 | IV sensitivity | High vega = longer-dated or volatile → significant expected move. Insiders paying up for vol. |
| **OTM Score** | >= 1.4 | Out-of-money concentration | Clustering in OTM strikes = directional bet vs hedging. Calculated from short-term OTM call percentage. |

**Test Criteria:**
- Backtest showed 50% hit rate for +100% returns (14 trades, 7 winners)
- Theta+Gamma+Vega+OTM ranked #1 out of 414 factor combinations tested
- 93rd percentile optimal (92nd = too many alerts, 95th = missed trades)

**Why These Greeks:**
- **Theta**: Insiders don't waste money on time decay unless they know timeline
- **Gamma**: ATM positioning shows price conviction, not speculation
- **Vega**: Willingness to pay volatility premium indicates expected magnitude
- **OTM**: Pure directionality (vs ATM hedging or income strategies)

### Legacy Composite Score (Secondary)

Still tracked for comparison:
- Volume Anomaly (0-3): Z-score vs 90-day baseline
- Volume:OI Ratio (0-2): Unusual trading vs existing positions
- OTM Concentration (0-2): Strike clustering
- Directional Bias (0-1): Call/put imbalance
- Time Pressure (0-2): Near-term expiration focus

Alert if composite >= 7.5 OR greeks >= 3 (both require $20K+ magnitude).

## Filtering

**Excluded from alerts:**
- **Bot-driven**: Stock moved >= 5% intraday (news already priced in)
- **Earnings-related**: Within 4 days of earnings (speculation, not insider knowledge)
- **Low magnitude**: < $20K total option volume (not institutional scale)

These filters reduce false positives by ~70% while preserving actionable signals.

## Database Schema

### Core Tables

| Table | Purpose | Retention |
|-------|---------|-----------|
| `temp_option` | Intraday snapshots with greeks | 1 day (truncated after daily ETL) |
| `temp_stock` | Intraday stock OHLC | 1 day |
| `daily_option_snapshot` | Historical options OHLC + greeks | 90 days |
| `daily_stock_snapshot` | Historical stock OHLC | 90 days |
| `daily_anomaly_snapshot` | Detection results | 90 days |
| `option_contracts` | Contract metadata | Until expiration |
| `earnings_calendar` | Earnings dates | 90 days |

### Anomaly Table Columns

**Scoring:**
- `high_conviction_score` (0-4): Count of greeks above threshold
- `is_high_conviction` (bool): Score >= 3
- `total_score` (0-10): Legacy composite score

**Greeks Details:**
- `greeks_theta_value`, `greeks_theta_percentile`: Actual theta & ranking
- `greeks_gamma_value`, `greeks_gamma_percentile`: Actual gamma & ranking
- `greeks_vega_value`, `greeks_vega_percentile`: Actual vega & ranking
- `greeks_otm_value`, `greeks_otm_percentile`: OTM score & ranking

**Filtering:**
- `is_bot_driven` (bool): Intraday move >= 5%
- `is_earnings_related` (bool): Within 4 days of earnings
- `is_actionable` (bool): NOT (bot_driven OR earnings_related)

**Recommendation:**
- `recommended_option` (varchar): Highest volume contract in direction
- `direction` (varchar): call_heavy / put_heavy / mixed

## Email Alerts

Sends email when:
- Greeks score >= 3/4, OR
- Legacy score >= 7.5
- AND magnitude >= $20K
- AND actionable (not bot/earnings)

Email includes:
- Symbol & direction
- Greeks score breakdown (which factors triggered)
- Recommended contract ticker
- Entry price, volume, OI data
- Historical baseline comparison

## Configuration

Required environment variables:

```bash
# Database
SUPABASE_DB_URL=postgresql://user:pass@host:port/db

# Polygon API
POLYGON_API_KEY=your_key_here
POLYGON_S3_ACCESS_KEY=s3_key
POLYGON_S3_SECRET_KEY=s3_secret

# Email (optional, for alerts)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=465
SENDER_EMAIL=alerts@yourdomain.com
EMAIL_PASSWORD=app_password
RECIPIENT_EMAIL=you@yourdomain.com
ANOMALY_EMAIL_ENABLED=true
```

## GitHub Actions

Scheduled runs (edit `.github/workflows/`):

- `intraday.yml`: `*/15 14-21 * * 1-5` (every 15 min, 9:45 AM - 4:45 PM EST, Mon-Fri)
- `daily.yml`: `0 13 * * 1-6` (8:00 AM EST, Mon-Sat)

Adjust for your timezone. Actions include timeout protection and retry logic.

## Streamlit Dashboard

```bash
cd app
streamlit run streamlit_app.py
```

Features:
- High conviction alerts (filterable by date, score)
- Historical performance tracking
- Symbol drilldown with option chain heatmaps
- Greeks distribution analysis

## Testing & Validation

### Unit Tests
```bash
python verifications/test_greeks_sanitization.py  # Greek bounds validation
python verifications/test_polygon_api_quality.py   # API health check
```

### Performance Analysis
```bash
python verifications/analyze_trigger_performance.py --min-score 7.5
python verifications/find_missed_opportunities.py
```

Generates reports in `verifications/` showing:
- Forward returns (T+1, T+3, T+5, T+10)
- Win rates by score buckets
- Correlation analysis (which factors predict returns)
- False negatives (missed moves)

### Backtesting Criteria

Original validation (Nov-Dec 2025):
- 410 triggers at score >= 7.5 → mean T+5 return: -2.24% (LOSING)
- 23 triggers at score >= 9.0 → mean T+5 return: +5.04% (WINNING)
- **Conclusion**: Threshold too low. Greeks system (score >= 3) filters better.

Greeks validation (Jan 2026):
- 14 triggers at greeks >= 3 → 7 hit +100% (50% success rate)
- Theta+Gamma+Vega+OTM best 4-factor combo (tested 414 combinations)

## Migrations

Database schema changes are in `migrations/`. Run on fresh install:

```bash
python -c "from migrations.migration_manager import MigrationManager; MigrationManager().migrate()"
```

Key migrations:
- `20250825_000001_create_complete_database.py`: Initial schema
- `20260112_000001_create_earnings_calendar_table.py`: Earnings filtering
- `20260113_000001_add_high_conviction_scoring.py`: Greeks system
- `20260131_000001_add_greek_component_columns.py`: Individual tracking
- `20260131_000002_add_greek_values_and_percentiles.py`: Percentile ranks

## Production Readiness Checklist

- [x] API rate limiting & retry logic (with exponential backoff)
- [x] Bulk insert optimization (COPY command, not row-by-row)
- [x] Data sanitization (greeks bounds checking)
- [x] Graceful degradation (missing greeks → NULL, not failure)
- [x] Monitoring & alerting (email on detection, logs on errors)
- [x] Idempotent operations (ON CONFLICT DO UPDATE)
- [x] Timezone handling (EST for market hours)
- [x] Concurrency controls (ThreadPoolExecutor with limits)
- [x] Security (no secrets in code, env vars only)

## Troubleshooting

**Greeks not populating:**
- Check `temp_option` has data: `SELECT COUNT(*) FROM temp_option WHERE greeks_gamma IS NOT NULL;`
- Verify daily process runs after intraday: Check timestamps
- Ensure temp tables aren't truncated before daily runs

**No email alerts:**
- Check `ANOMALY_EMAIL_ENABLED=true` in env
- Verify SMTP credentials are correct
- Look for `is_high_conviction=TRUE` in database (may be filtered by bot/earnings flags)

**API errors (502, timeout):**
- Polygon API occasionally degrades. Retry logic handles this.
- Check API status: https://polygon.io/status
- Reduce `--options-workers` if hitting rate limits

**Performance issues:**
- Increase `--options-batch-calls` and `--options-workers` for speed
- Use `--options-limit` for testing (limits contract count)
- Check database indexes (auto-created by migrations)

## Development

```bash
# Local development
python intraday_schedule.py --retention 1 --options-limit 100

# Check linter
python -m pylint database/ scrapers/ notifications/

# Run single anomaly detection
python -c "from database.analysis.insider_anomaly_detection import InsiderAnomalyDetector; print(InsiderAnomalyDetector().run_detection())"
```

## License

Research/educational purposes only. Not financial advice. Comply with all securities laws and regulations. Options trading carries significant risk.
