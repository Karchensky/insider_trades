# Database Setup Guide

This guide covers the complete setup of your Supabase PostgreSQL database for capturing Polygon API market data.

## Overview

The database setup includes:
- **DAILY_STOCK_SNAPSHOT** table to store daily market data from Polygon API
- Composite primary key (date, symbol) to prevent duplicates
- Automatic timestamp tracking for data lineage
- Efficient indexing for fast queries

## Database Schema

### DAILY_STOCK_SNAPSHOT Table

| Column | Type | Description | Polygon API Field |
|--------|------|-------------|-------------------|
| `date` | DATE | Trading date | `t` (converted from Unix timestamp) |
| `symbol` | VARCHAR(10) | Stock ticker | `T` |
| `close` | DECIMAL(12,4) | Closing price | `c` |
| `high` | DECIMAL(12,4) | High price | `h` |
| `low` | DECIMAL(12,4) | Low price | `l` |
| `transaction_volume` | INTEGER | Number of transactions | `n` |
| `open` | DECIMAL(12,4) | Opening price | `o` |
| `trading_volume` | BIGINT | Trading volume | `v` |
| `weighted_average_price` | DECIMAL(12,4) | Volume weighted average price | `vw` |
| `created_at` | TIMESTAMP | Record creation time | Auto-generated |
| `updated_at` | TIMESTAMP | Last update time | Auto-updated |

**Primary Key:** Composite of `date` + `symbol`

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Up Environment

Copy `env.template.txt` to `.env` and fill in your credentials:

```env
SUPABASE_DB_URL=postgresql://postgres.<URL>:<PASSWORD>@aws-1-us-east-2.pooler.supabase.com:6543/postgres
POLYGON_API_KEY=your_polygon_api_key
```

### 3. Quick Setup & Test

```bash
python quick_test.py
```

This will:
- Initialize the database with RLS enabled
- Test with real Polygon API data
- Show usage examples

### 4. Alternative: Manual Setup

```bash
# Run database migrations
python migrate.py migrate

# Test with fake data
python verifications/test_database_setup.py

# Test with real Polygon data
python verifications/test_real_polygon_data.py
```

## Data Retention Management

Automatically clean up old data based on business day retention policies:

```bash
# Check what would be deleted (dry run)
python maintenance/data_retention.py daily_stock_snapshot 60 date

# Actually delete records older than 60 business days
python maintenance/data_retention.py daily_stock_snapshot 60 date --execute

# Show table statistics only
python maintenance/data_retention.py daily_stock_snapshot 60 date --stats-only

# Custom batch size for large deletions
python maintenance/data_retention.py daily_stock_snapshot 90 date --execute --batch-size 5000

# Clean up ALL tables at once
python maintenance/data_retention.py ALL 60 --execute

# Show statistics for all tables
python maintenance/data_retention.py ALL 60 --stats-only
```

The retention script:
- **Multi-table support**: Use `ALL` to process all tables with date columns
- **Business day counting**: Excludes weekends and holidays properly
- **Safety features**: Dry-run mode by default, requires `--execute` flag
- **Batch processing**: Efficient deletion for large datasets
- **Auto-discovery**: Automatically finds tables with date/timestamp columns

### 5. Migration Management

```bash
# Check migration status
python migrate.py status

# Apply all pending migrations
python migrate.py migrate

# Apply migrations up to specific version
python migrate.py migrate 20240101_000001

# Rollback to specific version
python migrate.py rollback 20240101_000001

# Create new migration
python migrate.py create add_new_table
```

### Migration System Features

- **Version Tracking**: Uses timestamp-based versioning (YYYYMMDD_HHMMSS_name)
- **Dependency Management**: Ensures migrations are applied in correct order
- **Rollback Support**: Can rollback to any previous version
- **Idempotent**: Safe to run multiple times
- **Audit Trail**: Tracks when migrations were applied and execution time
- **Template Generation**: Creates boilerplate migration files

## Polygon API Scraping

### Command Line Usage

```bash
# Scrape most recent trading day
python scrapers/polygon_daily_scraper.py --recent

# Scrape most recent 60 business days
python scrapers/polygon_daily_scraper.py --recent 60

# Scrape specific date range
python scrapers/polygon_daily_scraper.py --start-date 2024-01-01 --end-date 2024-01-31

# Force re-scrape existing data
python scrapers/polygon_daily_scraper.py --recent 30 --force

# Include OTC securities
python scrapers/polygon_daily_scraper.py --recent 5 --include-otc
```

The scraper automatically:
- Skips weekends and holidays
- Handles API rate limiting (12 seconds between requests)
- Uses high-performance bulk loading (8,000+ records/sec)
- Uses upsert operations for duplicate handling
- Provides detailed progress reporting

### Performance Comparison

| Method | Speed | Best For |
|--------|-------|----------|
| Original (execute_many) | ~26 records/sec | Small datasets (<100 records) |
| execute_values | ~4,800 records/sec | Medium datasets (100-5,000 records) |
| **COPY (recommended)** | **~12,000 records/sec** | **Large datasets (1,000+ records)** |

The scraper automatically chooses the best method based on dataset size.

### Testing Performance

```bash
# Test bulk loading performance with different methods
python test_bulk_performance.py
```

This will benchmark all loading methods and test with real Polygon data.

## Option Contracts Scraping

The system now supports option contracts data from the [Polygon Options Contracts API](https://polygon.io/docs/rest/options/contracts/all-contracts):

### Command Line Usage

```bash
# OPTIMIZED SCRAPER (RECOMMENDED) - Uses concurrent API calls
# Scrape options for most recent trading day
python scrapers/polygon_options_scraper_optimized.py --recent

# Scrape options for recent 5 trading days
python scrapers/polygon_options_scraper_optimized.py --recent 5

# Scrape options for specific date range
python scrapers/polygon_options_scraper_optimized.py --start-date 2024-08-01 --end-date 2024-08-05

# Test with limited tickers (useful for testing)
python scrapers/polygon_options_scraper_optimized.py --recent --ticker-limit 10

# Force re-scrape existing data
python scrapers/polygon_options_scraper_optimized.py --recent --force

# LEGACY SCRAPER (Sequential processing - slower)
python scrapers/polygon_options_scraper.py --recent --ticker-limit 10

# OPTION DAILY SNAPSHOTS (OHLC data for each contract)
# Scrape daily price data for option contracts
python scrapers/polygon_option_snapshots_scraper.py --recent
python scrapers/polygon_option_snapshots_scraper.py --recent 5
python scrapers/polygon_option_snapshots_scraper.py --start-date 2024-08-01 --end-date 2024-08-05
python scrapers/polygon_option_snapshots_scraper.py --recent --contract-limit 100 --force
```

### How It Works

#### Optimized Scraper (Recommended)
1. **Fetches underlying tickers** from `daily_stock_snapshot` table for each date
2. **Concurrent API calls** - Up to 20 requests/second (respects API limits)
3. **Batch processing** - Collects all symbols per date before bulk loading
4. **Single bulk operation** - Loads all contracts for a date in one transaction
5. **High-performance** - 17x faster than sequential processing

#### Performance Comparison
| Method | Speed | API Calls/sec | Throughput | Best For |
|--------|-------|---------------|------------|----------|
| **Optimized (concurrent)** | **3,275+ contracts/sec** | **10.8+** | **29,180 contracts in 8.9s** | **Production use** |
| Legacy (sequential) | ~180 contracts/sec | ~0.17 | ~180 contracts in 1s | Testing only |

**Real Performance Results:**
- **11,373 tickers** available per date (no artificial limits)
- **1,000 ticker batches** for optimal memory usage
- **Dynamic rate limiting** adapts to API responses
- **33.7x speedup** over sequential processing

#### Technical Details
- **Concurrent processing**: ThreadPoolExecutor with 20 workers
- **Dynamic rate limiting**: Adapts from 0.05s to 1.0s delays based on API responses
- **Optimized batch processing**: 500 ticker batches for better parallelism
- **No ticker limits**: Processes all ~11,373 available tickers per date
- **Single bulk operation**: One PostgreSQL COPY per date (8,940+ records/sec)
- **Memory efficient**: Processes in configurable batches
- **Error recovery**: Automatic retries with exponential backoff

### Database Schema

#### Option Contracts Table
- **Composite Primary Key**: `date + symbol + contract_ticker`
- **Foreign Key**: `(date, symbol)` → `daily_stock_snapshot(date, symbol)`
- **Data Fields**: contract_type, expiration_date, strike_price, exercise_style, etc.
- **Optimized Schema**: Removed unnecessary JSONB field for better performance

#### Daily Option Snapshot Table  
- **Composite Primary Key**: `date + symbol + contract_ticker`
- **Foreign Key**: `(date, symbol, contract_ticker)` → `option_contracts(date, symbol, contract_ticker)`
- **OHLC Data**: open_price, high_price, low_price, close_price, volume
- **Extended Hours**: pre_market_price, after_hours_price
- **API Source**: [Polygon Daily Ticker Summary](https://polygon.io/docs/rest/options/aggregates/daily-ticker-summary)

## Usage Examples

### Basic Data Insertion

```python
from database.stock_data import StockDataManager

# Sample Polygon API response
polygon_response = {
    "status": "OK",
    "results": [
        {
            "T": "AAPL",
            "c": 150.25,
            "h": 152.00,
            "l": 149.50,
            "n": 1250,
            "o": 151.00,
            "t": 1602705600000,
            "v": 2500000,
            "vw": 150.75
        }
    ]
}

# Insert data
StockDataManager.insert_daily_snapshots(polygon_response)
```

### Querying Data

```python
from datetime import date
from database.stock_data import StockDataManager

# Get specific snapshot
snapshot = StockDataManager.get_daily_snapshot("AAPL", date(2024, 1, 15))

# Get symbol history
history = StockDataManager.get_symbol_history("AAPL", limit=30)

# Get market snapshot for a date
market_data = StockDataManager.get_market_snapshot(date(2024, 1, 15))

# Get latest snapshots
latest = StockDataManager.get_latest_snapshots(limit=100)

# Get table statistics
stats = StockDataManager.get_table_stats()
```

## Key Features

### 1. Duplicate Handling
- Uses `ON CONFLICT` to update existing records automatically
- Maintains data integrity with composite primary key
- Updates `updated_at` timestamp on data changes

### 2. Performance Optimization
- Indexed on `symbol`, `date`, and `created_at`
- Efficient queries for common access patterns
- Proper data types for financial precision

### 3. Data Lineage
- Automatic timestamp tracking
- Update triggers for audit trail
- Created/updated time tracking

### 4. Error Handling
- Comprehensive error logging
- Transaction rollback on failures
- Graceful handling of malformed data

## File Structure

```
insider_trades/
├── database/
│   ├── __init__.py
│   ├── connection.py          # Database connection management
│   └── stock_data.py          # Data operations for stock data
├── migrations/
│   ├── __init__.py
│   └── 001_create_daily_stock_snapshot.py  # Table creation script
├── verifications/
│   └── test_database_setup.py # Comprehensive test suite
├── examples/
│   └── polygon_to_database.py # Example API to database workflow
├── init_database.py           # Database initialization script
└── requirements.txt           # Python dependencies
```

## API Integration

The setup is designed to work seamlessly with Polygon API responses. The `StockDataManager.insert_daily_snapshots()` method automatically:

1. Converts Unix timestamps to dates
2. Maps API fields to database columns
3. Handles type conversions
4. Manages duplicates via upsert operations

## Monitoring and Maintenance

### Table Statistics
```python
stats = StockDataManager.get_table_stats()
print(f"Total records: {stats['total_records']}")
print(f"Unique symbols: {stats['unique_symbols']}")
print(f"Date range: {stats['earliest_date']} to {stats['latest_date']}")
```

### Common Queries

```sql
-- Top 10 most active stocks by volume
SELECT symbol, trading_volume, date 
FROM daily_stock_snapshot 
ORDER BY trading_volume DESC 
LIMIT 10;

-- Daily statistics
SELECT date, COUNT(*) as stock_count, AVG(close) as avg_close
FROM daily_stock_snapshot 
GROUP BY date 
ORDER BY date DESC;
```

## Troubleshooting

### Connection Issues
1. Verify `SUPABASE_DB_URL` in `.env` file
2. Check database server accessibility
3. Confirm credentials are correct

### Data Issues
1. Run verification script to test data flow
2. Check logs for specific error messages
3. Verify Polygon API response format

### Performance Issues
1. Monitor index usage
2. Consider partitioning for large datasets
3. Optimize queries using table statistics

## Next Steps

1. Set up scheduled jobs for regular data collection
2. Implement data validation and quality checks
3. Add monitoring and alerting for data pipeline
4. Consider data archival strategies for historical data

## Support

For issues or questions:
1. Check the verification script output
2. Review the example scripts
3. Examine database logs for errors
4. Verify environment configuration
