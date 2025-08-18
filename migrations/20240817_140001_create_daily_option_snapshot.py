"""
Migration: Create DAILY_OPTION_SNAPSHOT table
Version: 20240817_140001
Created: 2024-08-17 14:00:01

Creates the DAILY_OPTION_SNAPSHOT table for storing daily OHLC data for option contracts.

Table Structure:
- Composite Primary Key: date + contract_ticker
- Foreign Key Relationship: (date, contract_ticker) -> option_contracts(date, contract_ticker)
- Maps Polygon Daily Ticker Summary API response fields to database columns

Polygon API endpoint: /v1/open-close/{optionsTicker}/{date}
API fields mapping:
- from -> DATE
- symbol -> CONTRACT_TICKER  
- open -> OPEN_PRICE
- high -> HIGH_PRICE
- low -> LOW_PRICE
- close -> CLOSE_PRICE
- volume -> VOLUME
- preMarket -> PRE_MARKET_PRICE
- afterHours -> AFTER_HOURS_PRICE
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the migration - create daily option snapshot table and related objects."""
    logger.info("Creating DAILY_OPTION_SNAPSHOT table...")
    
    # Create main table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_option_snapshot (
        date DATE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        contract_ticker VARCHAR(50) NOT NULL,
        open_price DECIMAL(12, 4),
        high_price DECIMAL(12, 4),
        low_price DECIMAL(12, 4),
        close_price DECIMAL(12, 4),
        volume BIGINT DEFAULT 0,
        pre_market_price DECIMAL(12, 4),
        after_hours_price DECIMAL(12, 4),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        -- Composite primary key
        PRIMARY KEY (date, symbol, contract_ticker),
        
        -- Foreign key relationship to option_contracts
        FOREIGN KEY (date, symbol, contract_ticker) REFERENCES option_contracts(date, symbol, contract_ticker)
            ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    
    # Create indexes for better query performance
    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_date ON daily_option_snapshot (date);",
        "CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_contract ON daily_option_snapshot (contract_ticker);",
        "CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_close ON daily_option_snapshot (close_price);",
        "CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_volume ON daily_option_snapshot (volume);",
        "CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_created_at ON daily_option_snapshot (created_at);",
        "CREATE INDEX IF NOT EXISTS idx_daily_option_snapshot_compound ON daily_option_snapshot (date, symbol, contract_ticker);"
    ]
    
    # Create trigger for updating 'updated_at' timestamp
    create_trigger_sql = """
    DROP TRIGGER IF EXISTS update_daily_option_snapshot_updated_at ON daily_option_snapshot;
    
    CREATE TRIGGER update_daily_option_snapshot_updated_at
        BEFORE UPDATE ON daily_option_snapshot
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    
    # RLS policies
    rls_sql = [
        # Enable RLS on the table
        "ALTER TABLE daily_option_snapshot ENABLE ROW LEVEL SECURITY;",
        
        # Create policy for authenticated users to read all data
        """
        CREATE POLICY "Allow read access for authenticated users" ON daily_option_snapshot
        FOR SELECT
        USING (auth.role() = 'authenticated');
        """,
        
        # Create policy for authenticated users to insert data
        """
        CREATE POLICY "Allow insert for authenticated users" ON daily_option_snapshot
        FOR INSERT
        WITH CHECK (auth.role() = 'authenticated');
        """,
        
        # Create policy for authenticated users to update data
        """
        CREATE POLICY "Allow update for authenticated users" ON daily_option_snapshot
        FOR UPDATE
        USING (auth.role() = 'authenticated')
        WITH CHECK (auth.role() = 'authenticated');
        """,
        
        # Create policy for service role (for API access)
        """
        CREATE POLICY "Allow all for service role" ON daily_option_snapshot
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
        
        # Create trigger (reuse existing function from daily_stock_snapshot)
        db.execute_command(create_trigger_sql)
        logger.info("✓ Update trigger created")
        
        # Set up Row Level Security
        for sql in rls_sql:
            try:
                db.execute_command(sql)
            except Exception as e:
                logger.warning(f"RLS setup warning: {e}")
        logger.info("✓ Row Level Security configured")
        
        logger.info("DAILY_OPTION_SNAPSHOT migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    """Rollback the migration - drop daily option snapshot table and related objects."""
    logger.info("Rolling back DAILY_OPTION_SNAPSHOT table...")
    
    rollback_sql = [
        "DROP TRIGGER IF EXISTS update_daily_option_snapshot_updated_at ON daily_option_snapshot;",
        "DROP TABLE IF EXISTS daily_option_snapshot CASCADE;"
    ]
    
    try:
        for sql in rollback_sql:
            db.execute_command(sql)
        
        logger.info("DAILY_OPTION_SNAPSHOT rollback completed successfully!")
        
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


def get_table_info():
    """Get information about the DAILY_OPTION_SNAPSHOT table."""
    
    info_sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns 
    WHERE table_name = 'daily_option_snapshot'
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
    
    print("Running DAILY_OPTION_SNAPSHOT migration...")
    up()
    
    print("\nTable structure:")
    info = get_table_info()
    for column in info:
        nullable = "NULL" if column['is_nullable'] == 'YES' else "NOT NULL"
        default = f" DEFAULT {column['column_default']}" if column['column_default'] else ""
        print(f"  {column['column_name']}: {column['data_type']} {nullable}{default}")
    
    print("\nMigration completed successfully!")
