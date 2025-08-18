"""
Migration: Create DAILY_STOCK_SNAPSHOT table
Version: 20240101_000001
Created: 2024-01-01 00:00:01

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
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the migration - create table and related objects."""
    logger.info("Creating DAILY_STOCK_SNAPSHOT table...")
    
    # Create main table
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
    
    # RLS policies
    rls_sql = [
        # Enable RLS on the table
        "ALTER TABLE daily_stock_snapshot ENABLE ROW LEVEL SECURITY;",
        
        # Create policy for authenticated users to read all data
        """
        CREATE POLICY "Allow read access for authenticated users" ON daily_stock_snapshot
        FOR SELECT
        USING (auth.role() = 'authenticated');
        """,
        
        # Create policy for authenticated users to insert data
        """
        CREATE POLICY "Allow insert for authenticated users" ON daily_stock_snapshot
        FOR INSERT
        WITH CHECK (auth.role() = 'authenticated');
        """,
        
        # Create policy for authenticated users to update data
        """
        CREATE POLICY "Allow update for authenticated users" ON daily_stock_snapshot
        FOR UPDATE
        USING (auth.role() = 'authenticated')
        WITH CHECK (auth.role() = 'authenticated');
        """,
        
        # Create policy for service role (for API access)
        """
        CREATE POLICY "Allow all for service role" ON daily_stock_snapshot
        FOR ALL
        USING (auth.jwt() ->> 'role' = 'service_role');
        """
    ]
    
    try:
        # Create table
        db.execute_command(create_table_sql)
        logger.info("✓ Table created")
        
        # Create indexes
        for index_sql in create_indexes_sql:
            db.execute_command(index_sql)
        logger.info("✓ Indexes created")
        
        # Create trigger
        db.execute_command(create_trigger_sql)
        logger.info("✓ Update trigger created")
        
        # Set up Row Level Security
        for sql in rls_sql:
            try:
                db.execute_command(sql)
            except Exception as e:
                logger.warning(f"RLS setup warning: {e}")
        logger.info("✓ Row Level Security configured")
        
        logger.info("DAILY_STOCK_SNAPSHOT migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    """Rollback the migration - drop table and related objects."""
    logger.info("Rolling back DAILY_STOCK_SNAPSHOT table...")
    
    rollback_sql = [
        "DROP TRIGGER IF EXISTS update_daily_stock_snapshot_updated_at ON daily_stock_snapshot;",
        "DROP FUNCTION IF EXISTS update_updated_at_column();",
        "DROP TABLE IF EXISTS daily_stock_snapshot CASCADE;"
    ]
    
    try:
        for sql in rollback_sql:
            db.execute_command(sql)
        
        logger.info("DAILY_STOCK_SNAPSHOT rollback completed successfully!")
        
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
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


# Legacy function names for backward compatibility
def create_daily_stock_snapshot_table():
    """Legacy function name - calls up() for backward compatibility."""
    return up()


def rollback_daily_stock_snapshot_table():
    """Legacy function name - calls down() for backward compatibility."""
    return down()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("Running DAILY_STOCK_SNAPSHOT migration...")
    up()
    
    print("\nTable structure:")
    info = get_table_info()
    for column in info:
        nullable = "NULL" if column['is_nullable'] == 'YES' else "NOT NULL"
        default = f" DEFAULT {column['column_default']}" if column['column_default'] else ""
        print(f"  {column['column_name']}: {column['data_type']} {nullable}{default}")
    
    print("\nMigration completed successfully!")
