# Insider Trading Anomaly Detection System

Advanced options flow analysis system designed to identify suspicious trading patterns that may indicate insider activity.

## System Overview

This system monitors **10,000+ tickers** in real-time during market hours, analyzing options and stock data to detect anomalous patterns suggesting informed trading ahead of price movements.

### Key Features

- **Real-time Detection**: Intraday monitoring with 15-minute delayed data
- **Advanced Algorithms**: 5 sophisticated detection algorithms with multi-factor scoring
- **Email Alerts**: Immediate notifications for high-conviction opportunities
- **Historical Analysis**: End-of-day processing and retention for backtesting
- **Clean Architecture**: Single migration, optimized performance, streamlined codebase

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL database connection
- Polygon API key with options data access

### Installation

```bash
pip install -r requirements.txt
```

### Environment Setup

Create `.env` file:

```env
# Database
SUPABASE_DB_URL=postgresql://user:password@host:port/database

# API Access
POLYGON_API_KEY=your_polygon_api_key

# Email Alerts (Optional)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_app_password
ALERT_EMAIL_FROM=alerts@yourdomain.com
ALERT_EMAIL_TO=trader@yourdomain.com
ANOMALY_ALERT_MIN_SCORE=7.0
ANOMALY_EMAIL_ENABLED=true
```

### Database Setup

```bash
python migrate.py migrate
```

## Database Schema

### Core Tables

- **`daily_stock_snapshot`**: Historical daily stock OHLC data
- **`daily_option_snapshot`**: Historical daily option OHLC data
- **`full_daily_option_snapshot`**: End-of-day option snapshots with Greeks
- **`full_daily_anomaly_snapshot`**: Historical anomaly events
- **`temp_stock`**: Intraday stock snapshots (15-min delayed)
- **`temp_option`**: Intraday option snapshots with Greeks and volumes
- **`temp_anomaly`**: Real-time anomaly detection results

## Operational Workflows

### Intraday Process (Market Hours)

```bash
python intraday_schedule.py --retention 1 --options-limit 250
```

**What it does:**

1. Fetches full market stock snapshot → `temp_stock`
2. Fetches options snapshots for active contracts → `temp_option`
3. Runs enhanced anomaly detection across 5 algorithms
4. Stores results in `temp_anomaly` with upsert capability
5. Sends email alerts for high-score anomalies (≥7.0)
6. Applies retention policy to all temp tables (keeps 1 business day)

### Daily Process (Next Business Day, 8AM ET)

```bash
python daily_schedule.py --recent 3 --retention 60
```

**What it does:**

1. Processes historical stock data → `daily_stock_snapshot`
2. Processes historical option data → `daily_option_snapshot`
3. Captures final intraday snapshots → `full_daily_option_snapshot`
4. Migrates final anomaly events → `temp_anomaly` to `full_daily_anomaly_snapshot`
5. Truncates temp tables for fresh start
6. Applies retention policies (60 business days default)

## Enhanced Anomaly Detection

### Detection Algorithms

1. **Volume Anomalies with Greeks Context**

   - Unusual volume patterns combined with options Greeks analysis
   - Directional insight through delta/gamma positioning
2. **Suspicious Open Interest Spikes**

   - Rapid OI increases suggesting informed positioning
   - Requires baseline data (won't trigger on first day)
3. **Coordinated Multi-Strike Activity**

   - Ladder strategies across multiple strikes
   - Sophisticated positioning detection
4. **Pre-Move Positioning**

   - Options activity validated post-move
   - High conviction after price movement confirmation
5. **Low Volume High Conviction**

   - Unusual options activity on illiquid stocks
   - Prime insider trading territory

### Scoring Engine

**Multi-Factor Analysis** combines:

- Volume Z-scores and statistical significance
- Open interest momentum and flow analysis
- Price momentum and directional alignment
- Volatility expansion and skew patterns
- Greeks alignment (delta/gamma positioning)
- Strike concentration patterns
- Market cap factors (small cap premium)
- Liquidity surge detection

**Conviction Levels**: LOW (1-3) → MEDIUM (4-6) → HIGH (7-8) → EXTREME (9-10)

### Email Alert System

**Rich HTML Alerts** include:

- Symbol-grouped alerts with priority scoring
- Detailed contract information and Greeks
- Expected return calculations and risk factors
- Supporting evidence and pattern analysis

## Dashboard

Launch the Streamlit dashboard:

```bash
streamlit run app/streamlit_app.py
```

Features:

- Real-time anomaly monitoring
- Historical pattern analysis
- Options chain visualization
- Performance metrics and backtesting

## Maintenance

### Backfill Historical Anomalies

```bash
python scripts/enhanced_backfill_anomalies.py --start-date 2024-01-01 --end-date 2024-12-31
```

### Database Cleanup

```bash
python scripts/truncate_for_test.py  # Clear temp tables for testing
```

### Data Retention

- **Temp tables**: 1 business day (configurable via `--retention`)
- **Daily tables**: 60 business days (configurable via `--retention`)
- **Anomaly events**: Permanent historical record

## Production Recommendations

### Scheduling

- **Intraday**: Run every 5-15 minutes during market hours (9:30 AM - 4:00 PM ET)
- **Daily**: Run at 8:00 AM ET next business day
- **Monitoring**: Set up alerts for script failures

### Performance

- **Options Limit**: 250-500 contracts per batch for balance of coverage vs speed
- **Workers**: 20-50 parallel workers depending on system resources
- **Database**: Ensure proper indexing for high-frequency queries

### Alerts

- **High Scores**: Set email threshold at 7.0+ for actionable alerts
- **Volume Control**: Limit to top 10-20 alerts per day to avoid noise
- **Response Time**: System sends alerts within 2-3 minutes of detection

## Troubleshooting

### Common Issues

- **No anomalies detected**: Check if `full_daily_option_snapshot` has baseline data
- **Database connection**: Verify `SUPABASE_DB_URL` format and permissions
- **API limits**: Monitor Polygon API usage and rate limits
- **Email delivery**: Check SMTP credentials and firewall settings

### Debug Mode

```bash
# Run with enhanced logging
python intraday_schedule.py --retention 1 --options-limit 50 --debug
```

**The system is optimized for identifying high-conviction insider trading patterns with maximum return potential and minimal false positives.**
