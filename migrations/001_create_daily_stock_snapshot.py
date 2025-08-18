"""
Migration: Create DAILY_STOCK_SNAPSHOT table
Maps Polygon API response fields to database columns.

Polygon API fields mapping:
- T (ticker) -> SYMBOL
- c (close) -> CLOSE
- h (high) -> HIGH  
- l (low) -> LOW
- n (number of transactions) -> TRANSACTION_VOLUME
- o (open) -> OPEN
- t (timestamp) -> DATE (converted from Unix timestamp)
- v (volume) -> TRADING_VOLUME
- vw (volume weighted average price) -> WEIGHTED_AVERAGE_PRICE

Primary Key: Composite of DATE + SYMBOL
"""

import logging
from datetime import datetime
from database.connection import db

logger = logging.getLogger(__name__)

def create_daily_stock_snapshot_table():
    """Create the DAILY_STOCK_SNAPSHOT table with proper schema."""
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_stock_snapshot (
        date DATE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        close DECIMAL(12, 4) NOT NULL,
        high DECIMAL(12, 4) NOT NULL,
        low DECIMAL(12, 4) NOT NULL,
        transaction_volume INTEGER NOT NULL,
        open DECIMAL(12, 4) NOT NULL,
        trading_volume BIGINT NOT NULL,
        weighted_average_price DECIMAL(12, 4) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        -- Composite primary key
        PRIMARY KEY (date, symbol)
    );
    """
    
    # Create indexes for better query performance
    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_daily_stock_symbol ON daily_stock_snapshot (symbol);",
        "CREATE INDEX IF NOT EXISTS idx_daily_stock_date ON daily_stock_snapshot (date);",
        "CREATE INDEX IF NOT EXISTS idx_daily_stock_created_at ON daily_stock_snapshot (created_at);"
    ]
    
    # Create trigger for updating 'updated_at' timestamp
    create_trigger_sql = """
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ language 'plpgsql';
    
    DROP TRIGGER IF EXISTS update_daily_stock_snapshot_updated_at ON daily_stock_snapshot;
    
    CREATE TRIGGER update_daily_stock_snapshot_updated_at
        BEFORE UPDATE ON daily_stock_snapshot
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    
    try:
        # Create table
        logger.info("Creating DAILY_STOCK_SNAPSHOT table...")
        db.execute_command(create_table_sql)
        
        # Create indexes
        logger.info("Creating indexes...")
        for index_sql in create_indexes_sql:
            db.execute_command(index_sql)
        
        # Create trigger
        logger.info("Creating update trigger...")
        db.execute_command(create_trigger_sql)
        
        logger.info("DAILY_STOCK_SNAPSHOT table created successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise


def rollback_daily_stock_snapshot_table():
    """Drop the DAILY_STOCK_SNAPSHOT table and related objects."""
    
    rollback_sql = [
        "DROP TRIGGER IF EXISTS update_daily_stock_snapshot_updated_at ON daily_stock_snapshot;",
        "DROP FUNCTION IF EXISTS update_updated_at_column();",
        "DROP TABLE IF EXISTS daily_stock_snapshot CASCADE;"
    ]
    
    try:
        logger.info("Rolling back DAILY_STOCK_SNAPSHOT table...")
        for sql in rollback_sql:
            db.execute_command(sql)
        
        logger.info("DAILY_STOCK_SNAPSHOT table rolled back successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Failed to rollback table: {e}")
        raise


def get_table_info():
    """Get information about the DAILY_STOCK_SNAPSHOT table."""
    
    info_sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns 
    WHERE table_name = 'daily_stock_snapshot'
    ORDER BY ordinal_position;
    """
    
    try:
        result = db.execute_query(info_sql)
        return result
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Creating DAILY_STOCK_SNAPSHOT table...")
    create_daily_stock_snapshot_table()
    
    print("\nTable structure:")
    info = get_table_info()
    for column in info:
        nullable = "NULL" if column['is_nullable'] == 'YES' else "NOT NULL"
        default = f" DEFAULT {column['column_default']}" if column['column_default'] else ""
        print(f"  {column['column_name']}: {column['data_type']} {nullable}{default}")
    
    print("\nTable created successfully!")
