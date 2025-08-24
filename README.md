# Insider Trades Data Pipelines

Minimal setup and usage for daily and intraday pipelines.

## Prerequisites

- Python 3.11+
- PostgreSQL connection URL in `.env`: `SUPABASE_DB_URL`
- Polygon REST API key: `POLYGON_API_KEY`
- Polygon Flat Files S3 access:
  - `POLYGON_S3_ACCESS_KEY`
  - `POLYGON_S3_SECRET_KEY`
  - Optional overrides (defaults shown):
    - `POLYGON_FLATFILES_ENDPOINT=https://files.polygon.io`
    - `POLYGON_FLATFILES_BUCKET=flatfiles`
    - `POLYGON_FLATFILES_PREFIX=us_options_opra/day_aggs_v1`

## Install

```bash
pip install -r requirements.txt
```

## Database

Run migrations:

```bash
python migrate.py migrate
```

## Pipelines

- Daily (8am ET via CI):
  - Stocks OHLC via API → `daily_stock_snapshot`
  - Options day aggregates via flat files → `daily_option_snapshot`
  - Copy latest intraday per contract → `daily_option_snapshot_full`
  - Truncate `temp_option_snapshot` and `temp_stock_snapshot`

Run manually for recent N business days:

```bash
python daily_schedule.py --recent 3 --retention 60
```

- Intraday (during market hours):
  - Full-market stocks snapshot → `temp_stock_snapshot`
  - Options snapshot by contracts (from latest `daily_option_snapshot`) → `temp_option_snapshot`

Run one iteration:

```bash
python intraday_schedule.py --retention 1 --options-limit 250 --options-batch-calls 50 --options-workers 20
```
