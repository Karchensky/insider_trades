"""
Migration: Create OPTION_CONTRACTS table
Version: 20240816_130001
Created: 2024-08-16 13:00:01

Creates the OPTION_CONTRACTS table for storing options contract data from Polygon API.

Table Structure:
- Composite Primary Key: date + symbol + contract_ticker
- Foreign Key Relationship: (date, symbol) -> daily_stock_snapshot(date, symbol)
- Maps Polygon API response fields to database columns

Polygon API fields mapping:
- as_of date -> DATE
- underlying_ticker -> SYMBOL
- ticker -> CONTRACT_TICKER
- contract_type -> CONTRACT_TYPE
- expiration_date -> EXPIRATION_DATE
- strike_price -> STRIKE_PRICE
- exercise_style -> EXERCISE_STYLE
- shares_per_contract -> SHARES_PER_CONTRACT
- primary_exchange -> PRIMARY_EXCHANGE
- cfi -> CFI_CODE
"""

import logging
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

logger = logging.getLogger(__name__)


def up():
    """Apply the migration - create option contracts table and related objects."""
    logger.info("Creating OPTION_CONTRACTS table...")
    
    # Create main table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS option_contracts (
        date DATE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        contract_ticker VARCHAR(50) NOT NULL,
        contract_type VARCHAR(10) NOT NULL,
        expiration_date DATE NOT NULL,
        strike_price DECIMAL(12, 4) NOT NULL,
        exercise_style VARCHAR(20),
        shares_per_contract INTEGER DEFAULT 100,
        primary_exchange VARCHAR(10),
        cfi_code VARCHAR(10),
        additional_underlyings JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        -- Composite primary key
        PRIMARY KEY (date, symbol, contract_ticker),
        
        -- Foreign key relationship to daily_stock_snapshot
        FOREIGN KEY (date, symbol) REFERENCES daily_stock_snapshot(date, symbol)
            ON DELETE CASCADE ON UPDATE CASCADE
    );
    """
    
    # Create indexes for better query performance
    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_symbol ON option_contracts (symbol);",
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_date ON option_contracts (date);",
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_expiration ON option_contracts (expiration_date);",
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_strike ON option_contracts (strike_price);",
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_type ON option_contracts (contract_type);",
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_created_at ON option_contracts (created_at);",
        "CREATE INDEX IF NOT EXISTS idx_option_contracts_underlying ON option_contracts (date, symbol);"
    ]
    
    # Create trigger for updating 'updated_at' timestamp
    create_trigger_sql = """
    DROP TRIGGER IF EXISTS update_option_contracts_updated_at ON option_contracts;
    
    CREATE TRIGGER update_option_contracts_updated_at
        BEFORE UPDATE ON option_contracts
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    
    # RLS policies
    rls_sql = [
        # Enable RLS on the table
        "ALTER TABLE option_contracts ENABLE ROW LEVEL SECURITY;",
        
        # Create policy for authenticated users to read all data
        """
        CREATE POLICY "Allow read access for authenticated users" ON option_contracts
        FOR SELECT
        USING (auth.role() = 'authenticated');
        """,
        
        # Create policy for authenticated users to insert data
        """
        CREATE POLICY "Allow insert for authenticated users" ON option_contracts
        FOR INSERT
        WITH CHECK (auth.role() = 'authenticated');
        """,
        
        # Create policy for authenticated users to update data
        """
        CREATE POLICY "Allow update for authenticated users" ON option_contracts
        FOR UPDATE
        USING (auth.role() = 'authenticated')
        WITH CHECK (auth.role() = 'authenticated');
        """,
        
        # Create policy for service role (for API access)
        """
        CREATE POLICY "Allow all for service role" ON option_contracts
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
        
        logger.info("OPTION_CONTRACTS migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def down():
    """Rollback the migration - drop option contracts table and related objects."""
    logger.info("Rolling back OPTION_CONTRACTS table...")
    
    rollback_sql = [
        "DROP TRIGGER IF EXISTS update_option_contracts_updated_at ON option_contracts;",
        "DROP TABLE IF EXISTS option_contracts CASCADE;"
    ]
    
    try:
        for sql in rollback_sql:
            db.execute_command(sql)
        
        logger.info("OPTION_CONTRACTS rollback completed successfully!")
        
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise


def get_table_info():
    """Get information about the OPTION_CONTRACTS table."""
    
    info_sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns 
    WHERE table_name = 'option_contracts'
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
    
    print("Running OPTION_CONTRACTS migration...")
    up()
    
    print("\nTable structure:")
    info = get_table_info()
    for column in info:
        nullable = "NULL" if column['is_nullable'] == 'YES' else "NOT NULL"
        default = f" DEFAULT {column['column_default']}" if column['column_default'] else ""
        print(f"  {column['column_name']}: {column['data_type']} {nullable}{default}")
    
    print("\nMigration completed successfully!")
