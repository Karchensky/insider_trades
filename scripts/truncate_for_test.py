import logging
import os
import sys

# Ensure project root is on sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def safe_truncate(table: str) -> None:
    try:
        db.execute_command(f"TRUNCATE TABLE {table} CASCADE;")
        logger.info("Truncated %s", table)
    except Exception as e:
        logger.warning("Skip truncate %s: %s", table, e)


def main():
    tables = [
        'daily_stock_snapshot',
        'daily_option_snapshot',
        'daily_option_snapshot_full',
        'temp_stock_snapshot',
        'temp_option_snapshot',
    ]
    for t in tables:
        safe_truncate(t)
    print("Done truncating tables")


if __name__ == '__main__':
    main()


