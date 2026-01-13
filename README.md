# Insider Trading Detection System

Scrapes Polygon API to find insider trading patterns. High level: scrape options & stock data to establish 90-day baseline → check throughout the day for anomalous activity → send email when something stands out.

## Quick Start

```bash
# Intraday (every 15 min during market hours)
python intraday_schedule.py

# Daily (morning after market close)
python daily_schedule.py --recent 3 --retention 90
```

## Project Structure

```
insider_trades/
├── app/                          # Streamlit dashboard
│   ├── streamlit_app.py
│   └── dashboard_functions.py
├── database/
│   ├── core/                     # DB connections, bulk ops
│   ├── analysis/                 # Anomaly detection algorithm
│   └── maintenance/              # Data retention
├── scrapers/                     # Polygon API scrapers
├── migrations/                   # DB schema migrations
├── notifications/                # Email alerts
├── .github/workflows/            # GitHub Actions schedules
├── daily_schedule.py             # Daily orchestrator
├── intraday_schedule.py          # Intraday orchestrator
└── analysis.py                   # Algorithm validation script
```

## Scoring System

Two-tier detection: Legacy composite score (0-10) + new Greeks-based high conviction score (0-4).

### High Conviction Score (Primary)

Based on validated analysis of historical +100% option returns. Score = count of factors meeting 93rd percentile thresholds:

| Factor | Threshold | Description |
|--------|-----------|-------------|
| Theta | >= 0.1624 | Time decay (absolute value) |
| Gamma | >= 0.4683 | Delta sensitivity |
| Vega | >= 0.1326 | IV sensitivity |
| OTM Score | >= 1.4 | Out-of-money concentration |

**High Conviction**: Score >= 3/4
**Expected Performance**: ~50% hit rate for +100% option returns
**Exit Strategy**: Take profit at +100% gain or hold to expiration

### Legacy Composite Score (Secondary)

Still tracked for backwards compatibility:

- Volume Anomaly (0-3): Z-score vs 90-day baseline
- Volume:OI Ratio (0-2): Trading vs open interest
- OTM Concentration (0-2): Out-of-money options focus
- Directional Bias (0-1): Call/put preference
- Time Pressure (0-2): Near-term expiration clustering

**Alert Threshold**: Score >= 7.5/10.0 AND magnitude >= $20K

## Database Tables

| Table | Purpose | Retention |
|-------|---------|-----------|
| `temp_stock` | Intraday stock snapshots | 1 day |
| `temp_option` | Intraday options snapshots | 1 day |
| `daily_stock_snapshot` | Historical stock data | 90 days |
| `daily_option_snapshot` | Historical options data | 90 days |
| `daily_anomaly_snapshot` | Detected anomalies | 90 days |
| `option_contracts` | Contract metadata | Until expiration |
| `earnings_calendar` | Earnings dates | 90 days |

### New Columns in `daily_anomaly_snapshot`

| Column | Type | Description |
|--------|------|-------------|
| `high_conviction_score` | INTEGER | Count of Greeks factors above threshold (0-4) |
| `is_high_conviction` | BOOLEAN | True if score >= 3 |
| `recommended_option` | VARCHAR | Highest volume option ticker in direction |
| `is_bot_driven` | BOOLEAN | Stock moved >= 5% intraday (filtered out) |
| `is_earnings_related` | BOOLEAN | Within 3 days of earnings (filtered out) |

## Filtering

Alerts exclude:
- **Bot-driven**: Stock already moved >= 5% that day
- **Earnings-related**: Within 3 days of earnings date
- **Low magnitude**: Total $ volume < $20K

## Email Alerts

High conviction alerts are highlighted at the top of emails with:
- Symbol and direction (BULLISH/BEARISH)
- Greeks Score (0-4)
- Recommended option ticker

## Streamlit Dashboard

```bash
cd app
streamlit run streamlit_app.py
```

Shows:
- High conviction alerts prominently at top
- All detected anomalies by date
- Performance matrix
- Symbol-level analysis with options heatmaps

## Configuration

Environment variables:

```bash
SUPABASE_DB_URL=postgresql://...
POLYGON_API_KEY=your_key
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=465
SENDER_EMAIL=your@email.com
EMAIL_PASSWORD=your_app_password
RECIPIENT_EMAIL=recipient@email.com
```

## GitHub Actions

- **daily.yml**: Runs at 9 AM EST, processes previous days' data
- **intraday.yml**: Every 15 min during market hours (9:30 AM - 4:00 PM EST)

## Analysis

`analysis.py` contains the validation script that tested all factor combinations to arrive at the current scoring system. `analysis.png` shows the visualization.

Key findings:
- `theta + gamma + vega + otm_score` ranked #1 of 414 tested combinations
- 93rd percentile is optimal threshold balance
- Adding 5th factor doesn't improve results
- 14 alerts over test period, 50% hit +100% take profit

## Migrations

```bash
# Run pending migrations
python -c "from migrations.migration_manager import MigrationManager; MigrationManager().migrate()"
```

## License

For research/educational purposes. Not financial advice. Comply with securities laws.
