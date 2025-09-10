# Insider Trading Detection System

Scrapes polygon API to try & find insder trading patterns

## Project Structure

```text
insider_trades/
├── app/                           # Streamlit dashboard application
│   ├── streamlit_app.py          # Main dashboard interface
│   ├── dashboard_functions.py    # Dashboard utility functions
│   └── requirements.txt          # Dashboard dependencies
├── database/                      # All database-related functionality
│   ├── core/                     # Database connections, bulk operations
│   │   ├── connection.py         # PostgreSQL connection management
│   │   ├── bulk_operations.py    # Bulk data loading operations
│   │   └── stock_data.py         # Stock data management utilities
│   ├── analysis/                 # Anomaly detection algorithms
│   │   └── insider_anomaly_detection.py  # High-conviction insider trading detection
│   └── maintenance/              # Data retention and cleanup
│       └── data_retention.py     # Automated data lifecycle management
├── scrapers/                     # Data collection from external APIs
│   ├── polygon_full_market_snapshot_scraper.py      # Real-time stock data
│   ├── polygon_unified_options_snapshot_scraper.py  # Real-time options data
│   ├── polygon_daily_scraper.py                     # Historical stock data
│   ├── polygon_option_flatfile_loader.py            # Historical options data
│   └── polygon_option_contracts_scraper.py          # Options contract metadata
├── migrations/                   # Database schema migrations
│   ├── migration_manager.py      # Migration orchestration
│   └── [YYYYMMDD_######_name.py] # Individual migration files
├── scripts/                      # Utility and testing scripts
├── .github/                      # GitHub Actions for automated scheduling
│   └── workflows/
│       ├── daily.yml             # Daily process automation
│       └── intraday.yml          # Intraday process automation
├── daily_schedule.py             # Daily data processing orchestrator
├── intraday_schedule.py          # Intraday data processing orchestrator
└── README.md                     # This comprehensive documentation
```

## System Overview

This system operates on a dual-process architecture designed to capture and analyze options trading patterns for potential insider trading activity:

### Intraday Process (Every 15 Minutes)

Real-time market monitoring and anomaly detection during trading hours.

### Daily Process (Morning After Business Days)

Historical data archival, contract metadata management, and cleanup at 8:00 AM EST.

## Data Sources

### Primary Data Provider: Polygon.io

- **Stock Market Data**: Real-time and historical stock prices, volume, VWAP
- **Options Data**: Real-time and historical options prices, volume, implied volatility, Greeks (delta, gamma, theta, vega)
- **Options Contracts**: Contract specifications including type, strike price, expiration date, exercise style
- **API Rate Limits**: 20 requests/second

### Database: PostgreSQL (Supabase)

- **Temporary Tables**: `temp_stock`, `temp_option` (intraday data, stored with 1-day retention)
- **Anomaly Storage**: `daily_anomaly_snapshot` (persistent anomaly records, stored with 30-day retention)
- **Historical Tables**: `daily_stock_snapshot`, `daily_option_snapshot` (baseline data, stored with 30-day retention)
- **Metadata Tables**: `option_contracts` (contract specifications, all active contracts)
- **Security**: Row Level Security (RLS) enabled with proper access controls

## Intraday Process Details

**Schedule**: Every 15 minutes during market hours (9:30 AM - 4:00 PM EST) via GitHub Actions
**Purpose**: Real-time insider trading detection

### Intraday Step-by-Step Process

1. **Stock Snapshot Collection**

   - Fetches current prices for all active stocks via Polygon.io Full Market Snapshot API
   - Captures: symbol, day_open, day_high, day_low, day_close, day_volume, day_vwap
   - Stores in `temp_stock` table with timestamp
2. **Options Snapshot Collection**

   - Fetches options data for active contracts via Polygon.io Options Snapshot API
   - Captures: contract_ticker, session_volume, session_close, implied_volatility, open_interest
   - Captures Greeks: delta, gamma, theta, vega
   - Stores in `temp_option` table with timestamp
3. **High-Conviction Anomaly Detection**

   - Analyzes current activity against 30-day baseline from `daily_option_snapshot`
   - Uses statistical Z-score analysis to identify genuine anomalies
   - Focuses on insider trading patterns: OTM calls, volume spikes, directional bias
   - Flags only high-conviction cases (score >= 7.0/10.0)
   - Stores results in `daily_anomaly_snapshot` table
4. **Email Notifications**

   - Sends detailed HTML email alerts for high-conviction anomalies
   - Separates high volume (≥500) and low volume (<500) anomalies into distinct sections
   - Shows appropriate call/put multipliers based on dominant direction
   - Includes "Insider Pattern" column indicating bear/bull direction
   - Configurable via environment variables (SMTP settings, thresholds)

#### Intraday Usage

```bash
python intraday_schedule.py --retention 1 --options-limit 50000
```

## Daily Process Details

**Schedule**: Daily at 8:00 AM EST (morning after business days) via GitHub Actions
**Purpose**: Historical data archival, contract metadata management, and cleanup

### Daily Step-by-Step Process

1. **Stock Data Archival**

   - Processes previous trading day's stock data
   - Archives from `temp_stock` to `daily_stock_snapshot` table
   - Captures end-of-day prices and volume for baseline calculations
2. **Options Data Archival**

   - Processes previous trading day's options data
   - Archives from `temp_option` to `daily_option_snapshot` table
   - Preserves implied volatility, Greeks, and volume data for anomaly baselines
3. **Options Contract Metadata Management**

   - Incremental update of `option_contracts` table to capture all active options contractts
   - Compares `daily_option_snapshot` contracts with existing `option_contracts`
   - Only fetches metadata for genuinely new contracts (efficiency optimization)
   - Captures: contract_type (call/put), strike_price, expiration_date, exercise_style
4. **Temp Table Cleanup**

   - Truncates `temp_stock` and `temp_option` tables (intraday data no longer needed)
   - Preserves `daily_anomaly_snapshot` for historical analysis
5. **Bulk Data Retention**

   - Removes `daily_stock_snapshot` and `daily_option_snapshot` data older than 30 days (configurable)
   - Removes `option_contracts` where expiration_date < (current_date - retention_days)
   - Removes `daily_anomaly_snapshot` records older than retention period
   - Uses bulk operations for efficiency

#### Daily Usage

```bash
python daily_schedule.py --recent 3 --retention 30 --ticker-limit 1000
```

## Anomaly Detection System

### High-Conviction Insider Trading Detection

The system uses a **1-10 scoring scale** focusing on **statistical anomalies** rather than absolute volumes to identify genuine insider trading patterns.

### Scoring Components

#### 1. Volume Anomaly Score (0-3 points)

**Purpose**: Detect unusual volume spikes vs 30-day historical baseline

**Calculation Method**:

- Analyzes call and put volume separately against symbol-specific baselines
- Calculates Z-scores: `z = (current_volume - baseline_avg) / baseline_stddev`
- Call Score: `min(call_z_score / 3.0, 1.5)` (max 3.0 points)
- Put Score: `min(put_z_score / 3.0, 1.5)` (max 3.0 points)
- Total: Max of call or put score

#### 2. OTM Options Concentration Score (0-2 points)

**Purpose**: Identify out-of-the-money options concentration

**Calculation Method**:

- OTM Calls: `strike_price > underlying_price * 1.05` (5% out-of-money)
- OTM Puts: `strike_price < underlying_price * 0.95` (5% out-of-money)
- Short-term OTM: `expiration_date <= current_date + 21 days`
- Call Score: `(otm_call_ratio * 1.0) + (short_term_call_ratio * 1.0)`
- Put Score: `(otm_put_ratio * 1.0) + (short_term_put_ratio * 1.0)`
- Final Score: Use score from direction with most volume (calls vs puts) (max 2.0 points)

**Example**: ARES with heavy short-term OTM call focus = 2.0/2.0 points

#### 3. Directional Bias Score (0-1 points)

**Purpose**: Detect strong call/put preference indicating insider conviction

**Calculation Method**:

- Call Ratio: `call_volume / (call_volume + put_volume)`
- Distance from 50/50: `abs(call_ratio - 0.5)`
- Score: `distance_from_50_50 * 2` (max 1.0 points)
- 100% calls = 1.0 points (maximum bullish conviction)
- 100% puts = 1.0 points (maximum bearish conviction)
- 50/50 split = 0.0 points (no directional bias)

**Note**: This scoring method weights heavy call and put directions equally, providing fair scoring for both bullish and bearish insider activity.

#### 4. Open Interest Change Score (0-2 points)

**Purpose**: Detect unusual increases in open interest (new positions being established)

**Calculation Method**:

- Current Day Open Interest: Sum of all contract open interest
- Prior Day Open Interest: Previous trading day's total open interest
- Multiplier: `current_open_interest / prior_open_interest`
- New Scoring Formula: `(multiplier - 1) / 2` (capped at 2.0)
- 5.0x increase = 2.0 points (maximum score)
- 3.0x increase = 1.0 points
- 1.0x increase = 0.0 points (no change)

**Note**: This scoring method provides linear scaling where a 5x multiplier equals the maximum score of 2.0 points.

#### 5. Time Pressure Score (0-2 points)

**Purpose**: Detect clustering in near-term expirations (insider urgency)

**Calculation Method**:

- This Week Ratio: `volume_expiring_<=7_days / total_volume`
- Short-term Ratio: `volume_expiring_<=21_days / total_volume` (includes this week)
- Score: `(this_week_ratio * 1.2) + (short_term_ratio * 0.8)` (max 2.0 points)

**Note**: Contracts expiring within 7 days are counted towards both "this week" and "short-term" volume components to ensure accurate scoring.

### Composite Scoring and Alerting

**Total Score Calculation**:

```text
Composite Score = Volume Score + Open Interest Score + OTM Score + Directional Score + Time Pressure Score
Maximum Possible: 10.0 points (3+2+2+1+2)
```

**Alert Threshold**: Only symbols with `composite_score >= 7.0` are flagged as high-conviction

**Volume Filtering**:

- High Volume Anomalies: Volume ≥ 500 (primary alerts)
- Low Volume Anomalies: Volume < 500 (secondary alerts, shown separately)
- Both categories are included in email notifications and dashboard displays
- Volume information is displayed in all anomaly summary tables
- Email notifications show appropriate call/put multipliers based on dominant direction

**Example High-Conviction Detection**:

```text
ARES: 1.8 + 3.0 + 2.0 + 0.8 = 7.6/10.0 (HIGH CONVICTION ALERT)
- Volume: 9.6x normal call activity (statistical anomaly)
- OTM: Heavy concentration in short-term out-of-money calls
- Directional: 90% bias toward calls (extreme conviction)
- Time: Moderate clustering in near-term expirations
```

**Recent Improvements**:

- **OTM Scoring Logic**: Now uses the score from the direction (calls vs puts) with the most total volume, rather than taking the maximum score
- **Time Pressure Bug Fix**: Fixed calculation to ensure contracts expiring within 7 days are counted towards both "this week" and "short-term" volume components
- **Volume Display**: Added total volume column to all anomaly summary tables in both email notifications and Streamlit dashboard
- **Directional Indicators**: Email and dashboard now show appropriate call/put multipliers based on dominant direction (e.g., "3.8x normal call volume" for bullish, "14.1x normal put volume" for bearish)

### Detection Performance Metrics

**System Efficiency**:

- Contracts Analyzed: ~236,000 per intraday run
- Symbols Processed: ~2,000+ active symbols
- High-Conviction Alerts: Typically 0-5 symbols (top 0.1-0.2%)
- Execution Time: ~60 seconds for full analysis

**Alert Quality Distribution**:

- Score 7.0-10.0: Ultra-high conviction (manual investigation required)
- Score 5.0-6.9: High anomaly (automated monitoring)
- Score < 5.0: Normal market activity (no alert)

## Database Schema

### Temporary Tables (Intraday Data)

#### `temp_stock`

- **Purpose**: Current intraday stock snapshots
- **Key Fields**: symbol, day_close, day_vwap, day_volume, as_of_timestamp
- **Retention**: 1 day (configurable)
- **Update Frequency**: Every 15 minutes

#### `temp_option`

- **Purpose**: Current intraday options snapshots
- **Key Fields**: symbol, contract_ticker, session_volume, session_close, implied_volatility, greeks_delta, greeks_gamma, greeks_theta, greeks_vega, as_of_timestamp
- **Retention**: 1 day (configurable)
- **Update Frequency**: Every 15 minutes

### Aggregate Tables (Baseline Data, Anomalies)

#### `daily_stock_snapshot`

- **Purpose**: End-of-day stock data for baseline calculations
- **Key Fields**: date, symbol, open, high, low, close, volume, vwap
- **Retention**: 30 days (configurable)
- **Update Frequency**: Once daily after market close

#### `daily_option_snapshot`

- **Purpose**: End-of-day options data for anomaly baselines
- **Key Fields**: date, symbol, contract_ticker, volume, close, implied_volatility, delta, gamma, theta, vega
- **Retention**: 30 days (configurable)
- **Update Frequency**: Once daily after market close

#### `daily_anomaly_snapshot`

- **Purpose**: High-conviction insider trading alerts
- **Key Fields**: event_date, symbol, total_score, volume_score, open_interest_score, otm_score, directional_score, time_score, call_volume, put_volume, total_volume, call_baseline_avg, put_baseline_avg, call_multiplier, put_multiplier, direction, pattern_description, z_score, otm_call_percentage, short_term_percentage, call_put_ratio, open_interest_change, open_interest, prior_open_interest, as_of_timestamp
- **Retention**: 7 days (fixed)
- **Update Frequency**: Every 15 minutes (during anomaly detection)

### Metadata Tables

#### `option_contracts`

- **Purpose**: Options contract specifications and metadata
- **Key Fields**: symbol, contract_ticker (composite primary key), contract_type, strike_price, expiration_date, exercise_style
- **Retention**: Based on expiration_date (expires contracts older than retention period)
- **Update Frequency**:  Incremental (only new contracts daily)

## Streamlit Dashboard

The system includes a comprehensive Streamlit dashboard for visualizing and analyzing anomaly data.

### Dashboard Features

- **Anomaly Overview**: Summary table showing all detected anomalies with scores and key metrics
- **Volume Filtering**: Separate sections for high volume (≥500) and low volume (<500) anomalies
- **Date-based Analysis**: Tables organized by date showing anomalies for specific trading days
- **Key Indicators**: Displays appropriate call/put multipliers based on dominant direction
- **Insider Pattern**: Shows directional bias (bullish, bearish, mixed) for each anomaly
- **Interactive Filtering**: Filter by date range, score threshold, and volume levels

### Running the Dashboard

```bash
cd app
streamlit run streamlit_app.py
```

The dashboard will be available at `http://localhost:8501` and provides real-time visualization of the anomaly detection system's output.

## Notification System

The system includes a comprehensive email notification system for alerting on high-conviction anomalies.

### Email Notification Features

- **HTML Email Format**: Rich, formatted HTML emails with tables and styling
- **Volume-based Filtering**: Separate sections for high volume (≥500) and low volume (<500) anomalies
- **Directional Indicators**: Shows appropriate call/put multipliers based on dominant direction
- **Insider Pattern Detection**: Indicates bullish, bearish, or mixed directional positioning
- **Key Metrics Display**: Shows OTM scores, open interest changes, and other critical metrics
- **Configurable Thresholds**: Customizable via environment variables

### Email Content Structure

- **High Volume Anomalies**: Primary alerts for significant volume anomalies
- **Low Volume Anomalies**: Secondary alerts for lower volume but still significant patterns
- **Summary Tables**: Detailed breakdown of each anomaly with scores and metrics
- **Pattern Analysis**: Clear indication of insider trading patterns and directional bias

### Configuration

Email notifications are configured via environment variables:

```bash
SMTP_SERVER=your_smtp_server
SMTP_PORT=587
SMTP_USERNAME=your_email@domain.com
SMTP_PASSWORD=your_password
NOTIFICATION_EMAIL=recipient@domain.com
```

## Database Migrations

The system includes a comprehensive migration system for managing database schema changes.

### Migration System Features

- **Automatic Migration Detection**: Automatically detects and runs pending migrations
- **Rollback Support**: Each migration includes both `up()` and `down()` functions for rollback capability
- **Version Control**: Migrations are timestamped and versioned for proper ordering
- **Error Handling**: Comprehensive error handling with rollback on failure

### Running Migrations

```bash
# Run all pending migrations
python migrations/migration_manager.py

# Run a specific migration
python migrations/YYYYMMDD_######_migration_name.py

# Rollback a specific migration
python migrations/YYYYMMDD_######_migration_name.py down
```

### Migration Naming Convention

- Format: `YYYYMMDD_######_descriptive_name.py`
- Example: `20250909_000001_add_max_total_score_column.py`
- Migrations are executed in chronological order

## Testing and Debugging

The system includes comprehensive testing and debugging capabilities for ensuring accuracy and reliability.

### Testing Features

- **Ad-hoc Query Testing**: Ability to test specific symbols and compare results with database records
- **Score Verification**: Detailed breakdown of individual scoring components for validation
- **Data Accuracy Checks**: Verification of calculations against expected results
- **Round-trip Testing**: End-to-end testing of data flow from collection to storage

### Debugging Tools

- **Detailed Logging**: Comprehensive logging throughout the anomaly detection process
- **Score Breakdown**: Individual component scores for each anomaly (volume, OTM, directional, etc.)
- **Data Validation**: Checks for data integrity and calculation accuracy
- **Performance Monitoring**: Tracking of processing times and system performance

### Common Testing Scenarios

- **Score Discrepancy Investigation**: When calculated scores don't match expected values
- **Data Source Verification**: Confirming data accuracy from different sources (temp vs daily tables)
- **Calculation Validation**: Verifying mathematical formulas and scoring logic
- **Edge Case Testing**: Testing unusual market conditions and data patterns

### Debugging Commands

```bash
# Test specific symbol scoring
python -c "from database.analysis.insider_anomaly_detection import InsiderAnomalyDetector; detector = InsiderAnomalyDetector(); print(detector.analyze_symbol('SYMBOL'))"

# Verify database data
python -c "from database.core.connection import db; conn = db.connect(); cursor = conn.cursor(); cursor.execute('SELECT * FROM daily_anomaly_snapshot WHERE symbol = %s', ('SYMBOL',)); print(cursor.fetchall())"
```

## Configuration and Usage

### Environment Variables

```bash
DATABASE_URL=postgresql://user:pass@host:port/db
POLYGON_API_KEY=your_polygon_api_key
INTRADAY_RETENTION_DAYS=1
DAILY_RETENTION_DAYS=30
```

## Automated Scheduling

### GitHub Actions

**Daily Process** (`.github/workflows/daily.yml`):

- Triggers: Daily at 8:00 AM EST (morning after business days)
- Runs: `python daily_schedule.py --recent 3 --retention 30`
- Purpose: Archive previous 3 days' data, update contracts, cleanup temp tables

**Intraday Process** (`.github/workflows/intraday.yml`):

- Triggers: Every 15 minutes, Monday-Friday, 9:30 AM - 4:00 PM EST
- Runs: `python intraday_schedule.py --retention 1 --options-limit 250`
- Purpose: Real-time data collection, anomaly detection, and email alerts

### Monitoring and Alerts

**Success Indicators**:

- Intraday: "✓ X high-conviction anomalies detected from Y contracts"
- Daily: "✓ Archived X stock records, Y option records, Z contracts updated"

**Failure Indicators**:

- "✗ Detection failed: [error message]"
- "✗ Failed to load contract metadata: [error message]"

## System Performance

### Typical Performance Metrics

**Intraday Run (15-minute cycle)**:

- Data Collection: 5-10 seconds
- Anomaly Detection: 45-60 seconds
- Total Runtime: ~60-70 seconds
- Memory Usage: <500MB
- API Calls: 2-3 per run (stock + options snapshots)

**Daily Run (once per day)**:

- Data Archival: 2-5 minutes
- Contract Updates: 5-15 minutes (depending on new contracts)
- Total Runtime: ~10-20 minutes
- API Calls: 0-50 (only for new contracts via smart incremental)

### Scalability Considerations

**Current Capacity**:

- Symbols Tracked: 2,000+ active stocks
- Options Contracts: 50,000+ active contracts
- Daily Volume: 500,000+ option contract records
- Historical Baseline: 30 days of complete market data

**Performance Optimizations**:

- Bulk database operations (COPY vs INSERT)
- Smart incremental updates (only new data)
- Efficient Z-score calculations with pre-computed baselines
- Connection pooling and query optimization

## Troubleshooting

### Common Issues

**"No high-conviction anomalies detected"**:

- Normal condition - system is designed for quality over quantity
- Indicates market activity is within normal statistical ranges

**"No baseline data available"**:

- Occurs on first run or after database reset
- Requires 1-2 days of daily_option_snapshot data for baselines

**Import errors after folder restructure**:

- Ensure all imports use `database.core.connection` format
- Check migrations have correct import paths

### Performance Tuning

**Increase Detection Sensitivity** (not recommended):

- Lower alert threshold from 7.0 to 6.0 in `insider_anomaly_detection.py`
- Reduce minimum volume threshold from 500 to 200

**Optimize for Speed**:

- Reduce `--options-limit` parameter for faster intraday runs
- Increase retention periods to reduce daily processing load

## License and Compliance

This system is designed for legitimate financial analysis and research purposes. Users must:

- Comply with all applicable securities laws and regulations
- Use data in accordance with Polygon.io terms of service
- Not use for actual trading decisions without proper due diligence
- Respect insider trading laws and regulations

The detection of potential insider trading patterns does not constitute proof of illegal activity and should be used only as a starting point for further investigation by qualified professionals.
