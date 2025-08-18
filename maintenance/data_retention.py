"""
Data Retention Management Script
Deletes records from database tables based on business day retention periods.
Supports configurable table names, retention periods, and date columns.
"""

import os
import sys
import logging
import argparse
from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataRetentionManager:
    """
    Manages data retention by deleting old records based on business day calculations.
    """
    
    def __init__(self):
        self.market_holidays = self._get_market_holidays()
    
    def _get_market_holidays(self) -> List[str]:
        """
        Get list of market holidays to exclude from business day calculations.
        
        Returns:
            List of holiday dates in YYYY-MM-DD format
        """
        current_year = datetime.now().year
        # Basic U.S. market holidays - can be expanded
        holidays = []
        
        # Add holidays for current and previous year
        for year in [current_year - 1, current_year]:
            holidays.extend([
                f"{year}-01-01",  # New Year's Day
                f"{year}-01-15",  # MLK Day (approximate)
                f"{year}-02-19",  # Presidents Day (approximate)
                f"{year}-05-27",  # Memorial Day (approximate)
                f"{year}-07-04",  # Independence Day
                f"{year}-09-02",  # Labor Day (approximate)
                f"{year}-11-28",  # Thanksgiving (approximate)
                f"{year}-12-25",  # Christmas Day
            ])
        
        return holidays
    
    def is_business_day(self, check_date: date) -> bool:
        """
        Check if a given date is a business/trading day.
        
        Args:
            check_date: Date to check
            
        Returns:
            True if it's a business day, False otherwise
        """
        # Skip weekends
        if check_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
            return False
        
        # Skip holidays
        date_str = check_date.strftime('%Y-%m-%d')
        if date_str in self.market_holidays:
            return False
        
        return True
    
    def calculate_cutoff_date(self, retention_days: int, reference_date: Optional[date] = None) -> date:
        """
        Calculate the cutoff date based on business days retention.
        
        Args:
            retention_days: Number of business days to retain
            reference_date: Reference date (defaults to today)
            
        Returns:
            Cutoff date - records older than this should be deleted
        """
        if reference_date is None:
            reference_date = date.today()
        
        if retention_days <= 0:
            raise ValueError("Retention days must be positive")
        
        current_date = reference_date
        business_days_counted = 0
        
        # Count backwards to find the cutoff date
        while business_days_counted < retention_days:
            current_date -= timedelta(days=1)
            if self.is_business_day(current_date):
                business_days_counted += 1
            
            # Safety check - don't go back more than 5 years
            if (reference_date - current_date).days > 1825:
                logger.warning(f"Reached 5-year limit while calculating {retention_days} business days")
                break
        
        logger.info(f"Cutoff date for {retention_days} business days retention: {current_date}")
        return current_date
    
    def validate_table_and_column(self, table_name: str, date_column: str) -> bool:
        """
        Validate that the table and date column exist.
        
        Args:
            table_name: Name of the table
            date_column: Name of the date column
            
        Returns:
            True if valid, False otherwise
        """
        # Check if table exists
        table_check_sql = """
        SELECT COUNT(*) as count
        FROM information_schema.tables 
        WHERE table_name = %s AND table_schema = 'public'
        """
        
        try:
            result = db.execute_query(table_check_sql, (table_name,))
            if not result or result[0]['count'] == 0:
                logger.error(f"Table '{table_name}' does not exist")
                return False
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False
        
        # Check if column exists
        column_check_sql = """
        SELECT COUNT(*) as count
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s AND table_schema = 'public'
        """
        
        try:
            result = db.execute_query(column_check_sql, (table_name, date_column))
            if not result or result[0]['count'] == 0:
                logger.error(f"Column '{date_column}' does not exist in table '{table_name}'")
                return False
        except Exception as e:
            logger.error(f"Failed to check column existence: {e}")
            return False
        
        return True
    
    def get_records_to_delete_count(self, table_name: str, date_column: str, cutoff_date: date) -> int:
        """
        Get count of records that would be deleted.
        
        Args:
            table_name: Name of the table
            date_column: Name of the date column
            cutoff_date: Records older than this date will be counted
            
        Returns:
            Number of records that would be deleted
        """
        count_sql = f"""
        SELECT COUNT(*) as count
        FROM {table_name}
        WHERE {date_column} < %s
        """
        
        try:
            result = db.execute_query(count_sql, (cutoff_date,))
            return result[0]['count'] if result else 0
        except Exception as e:
            logger.error(f"Failed to count records for deletion: {e}")
            raise
    
    def delete_old_records(self, table_name: str, date_column: str, retention_days: int, 
                          dry_run: bool = True, batch_size: int = 1000) -> dict:
        """
        Delete old records from a table based on business day retention.
        
        Args:
            table_name: Name of the table to clean up
            date_column: Name of the date column to use for retention
            retention_days: Number of business days to retain
            dry_run: If True, only report what would be deleted
            batch_size: Number of records to delete per batch
            
        Returns:
            Dictionary with deletion statistics
        """
        logger.info(f"Starting retention cleanup for table '{table_name}'")
        logger.info(f"Retention policy: {retention_days} business days")
        logger.info(f"Date column: {date_column}")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL DELETION'}")
        
        # Validate inputs
        if not self.validate_table_and_column(table_name, date_column):
            raise ValueError(f"Invalid table '{table_name}' or column '{date_column}'")
        
        # Calculate cutoff date
        cutoff_date = self.calculate_cutoff_date(retention_days)
        
        # Get count of records to delete
        records_to_delete = self.get_records_to_delete_count(table_name, date_column, cutoff_date)
        
        logger.info(f"Records older than {cutoff_date}: {records_to_delete:,}")
        
        if records_to_delete == 0:
            logger.info("No records to delete")
            return {
                'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
                'records_identified': 0,
                'records_deleted': 0,
                'batches_processed': 0,
                'dry_run': dry_run
            }
        
        if dry_run:
            logger.info(f"DRY RUN: Would delete {records_to_delete:,} records older than {cutoff_date}")
            return {
                'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
                'records_identified': records_to_delete,
                'records_deleted': 0,
                'batches_processed': 0,
                'dry_run': True
            }
        
        # Perform actual deletion in batches
        logger.info(f"Deleting {records_to_delete:,} records in batches of {batch_size}")
        
        delete_sql = f"""
        DELETE FROM {table_name}
        WHERE {date_column} < %s
        AND ctid IN (
            SELECT ctid FROM {table_name}
            WHERE {date_column} < %s
            LIMIT %s
        )
        """
        
        total_deleted = 0
        batch_count = 0
        
        try:
            while True:
                # Delete a batch
                conn = db.connect()
                with conn.cursor() as cursor:
                    cursor.execute(delete_sql, (cutoff_date, cutoff_date, batch_size))
                    deleted_in_batch = cursor.rowcount
                    conn.commit()
                
                if deleted_in_batch == 0:
                    break
                
                total_deleted += deleted_in_batch
                batch_count += 1
                
                logger.info(f"Batch {batch_count}: Deleted {deleted_in_batch:,} records "
                           f"(Total: {total_deleted:,}/{records_to_delete:,})")
                
                # Safety check
                if batch_count > 10000:  # Prevent runaway deletion
                    logger.warning("Reached maximum batch limit - stopping deletion")
                    break
            
            logger.info(f"Deletion completed: {total_deleted:,} records deleted in {batch_count} batches")
            
            return {
                'cutoff_date': cutoff_date.strftime('%Y-%m-%d'),
                'records_identified': records_to_delete,
                'records_deleted': total_deleted,
                'batches_processed': batch_count,
                'dry_run': False
            }
            
        except Exception as e:
            logger.error(f"Deletion failed: {e}")
            raise
    
    def get_table_statistics(self, table_name: str, date_column: str) -> dict:
        """
        Get statistics about a table's date distribution.
        
        Args:
            table_name: Name of the table
            date_column: Name of the date column
            
        Returns:
            Dictionary with table statistics
        """
        stats_sql = f"""
        SELECT 
            COUNT(*) as total_records,
            MIN({date_column}) as earliest_date,
            MAX({date_column}) as latest_date,
            COUNT(DISTINCT {date_column}) as unique_dates
        FROM {table_name}
        """
        
        try:
            result = db.execute_query(stats_sql)
            return result[0] if result else {}
        except Exception as e:
            logger.error(f"Failed to get table statistics: {e}")
            raise


    def get_all_tables_with_date_columns(self) -> List[Dict[str, str]]:
        """
        Get all tables in the database that have date columns.
        
        Returns:
            List of dictionaries with table_name and date_column
        """
        # Known tables with their date columns
        known_tables = [
            {'table_name': 'daily_stock_snapshot', 'date_column': 'date'},
            {'table_name': 'option_contracts', 'date_column': 'date'},
            {'table_name': 'daily_option_snapshot', 'date_column': 'date'},
            {'table_name': 'schema_migrations', 'date_column': 'applied_at'}
        ]
        
        # Query for tables with date/timestamp columns
        query_sql = """
        SELECT DISTINCT 
            table_name,
            column_name as date_column
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND (data_type = 'date' OR data_type LIKE 'timestamp%')
        AND table_name NOT LIKE 'temp_%'
        ORDER BY table_name, column_name
        """
        
        try:
            discovered_tables = db.execute_query(query_sql)
            
            # Merge known tables with discovered ones
            all_tables = {}
            
            # Add known tables first (with preferred date columns)
            for table_info in known_tables:
                table_name = table_info['table_name']
                if self.validate_table_and_column(table_name, table_info['date_column']):
                    all_tables[table_name] = table_info
            
            # Add discovered tables if not already known
            for table_info in discovered_tables:
                table_name = table_info['table_name']
                if table_name not in all_tables:
                    all_tables[table_name] = {
                        'table_name': table_name,
                        'date_column': table_info['date_column']
                    }
            
            return list(all_tables.values())
            
        except Exception as e:
            logger.error(f"Failed to discover tables: {e}")
            return known_tables
    
    def cleanup_all_tables(self, retention_days: int, dry_run: bool = True, 
                          batch_size: int = 1000) -> Dict[str, Any]:
        """
        Clean up old records from all tables with date columns.
        
        Args:
            retention_days: Number of business days to retain
            dry_run: If True, only report what would be deleted
            batch_size: Number of records to delete per batch
            
        Returns:
            Dictionary with cleanup results for all tables
        """
        logger.info(f"Starting retention cleanup for ALL tables")
        logger.info(f"Retention policy: {retention_days} business days")
        logger.info(f"Mode: {'DRY RUN' if dry_run else 'ACTUAL DELETION'}")
        
        all_tables = self.get_all_tables_with_date_columns()
        
        if not all_tables:
            logger.warning("No tables with date columns found")
            return {}
        
        logger.info(f"Found {len(all_tables)} tables to process")
        
        results = {
            'total_tables': len(all_tables),
            'successful_tables': 0,
            'failed_tables': 0,
            'total_records_deleted': 0,
            'table_results': {}
        }
        
        for table_info in all_tables:
            table_name = table_info['table_name']
            date_column = table_info['date_column']
            
            logger.info(f"\nProcessing table: {table_name} (date column: {date_column})")
            
            try:
                table_result = self.delete_old_records(
                    table_name, date_column, retention_days, 
                    dry_run=dry_run, batch_size=batch_size
                )
                
                results['table_results'][table_name] = table_result
                results['successful_tables'] += 1
                results['total_records_deleted'] += table_result['records_deleted']
                
                logger.info(f"✓ Completed {table_name}: {table_result['records_deleted']:,} records processed")
                
            except Exception as e:
                logger.error(f"✗ Failed to process {table_name}: {e}")
                results['failed_tables'] += 1
                results['table_results'][table_name] = {
                    'error': str(e),
                    'records_deleted': 0
                }
        
        return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Data Retention Management')
    
    parser.add_argument('table_name', nargs='?', default='ALL',
                       help='Name of the table to clean up, or "ALL" for all tables')
    parser.add_argument('retention_days', type=int,
                       help='Number of business days to retain')
    parser.add_argument('date_column', nargs='?', default='date',
                       help='Name of the date column for retention calculation (ignored for ALL)')
    
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Perform dry run (default: true)')
    parser.add_argument('--execute', action='store_true',
                       help='Actually perform deletion (overrides --dry-run)')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Number of records to delete per batch (default: 1000)')
    parser.add_argument('--stats-only', action='store_true',
                       help='Only show table statistics')
    
    args = parser.parse_args()
    
    # Override dry_run if execute is specified
    if args.execute:
        dry_run = False
    else:
        dry_run = args.dry_run
    
    try:
        manager = DataRetentionManager()
        
        print("=" * 70)
        print("DATA RETENTION MANAGEMENT")
        print("=" * 70)
        print(f"Target: {args.table_name}")
        if args.table_name != 'ALL':
            print(f"Date Column: {args.date_column}")
        print(f"Retention: {args.retention_days} business days")
        print(f"Batch Size: {args.batch_size}")
        print(f"Mode: {'STATS ONLY' if args.stats_only else ('DRY RUN' if dry_run else 'EXECUTE')}")
        print("=" * 70)
        
        if args.table_name.upper() == 'ALL':
            # Process all tables
            if args.stats_only:
                # Show stats for all tables
                all_tables = manager.get_all_tables_with_date_columns()
                for table_info in all_tables:
                    table_name = table_info['table_name']
                    date_column = table_info['date_column']
                    print(f"\nTable: {table_name} (Date Column: {date_column})")
                    try:
                        stats = manager.get_table_statistics(table_name, date_column)
                        print(f"  Total records: {stats.get('total_records', 'N/A'):,}")
                        print(f"  Date range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}")
                        print(f"  Unique dates: {stats.get('unique_dates', 'N/A'):,}")
                    except Exception as e:
                        print(f"  Error: {e}")
                return
            
            # Perform cleanup on all tables
            print(f"\nProcessing ALL tables:")
            results = manager.cleanup_all_tables(
                args.retention_days,
                dry_run=dry_run,
                batch_size=args.batch_size
            )
            
            print(f"\nOverall Results:")
            print(f"  Total tables processed: {results['total_tables']}")
            print(f"  Successful tables: {results['successful_tables']}")
            print(f"  Failed tables: {results['failed_tables']}")
            print(f"  Total records deleted: {results['total_records_deleted']:,}")
            
            # Show per-table results
            print(f"\nPer-table Results:")
            for table_name, table_result in results['table_results'].items():
                if 'error' in table_result:
                    print(f"  ✗ {table_name}: {table_result['error']}")
                else:
                    print(f"  ✓ {table_name}: {table_result['records_deleted']:,} records")
            
        else:
            # Process single table
            # Show table statistics
            print(f"\nTable Statistics:")
            stats = manager.get_table_statistics(args.table_name, args.date_column)
            print(f"  Total records: {stats.get('total_records', 'N/A'):,}")
            print(f"  Date range: {stats.get('earliest_date', 'N/A')} to {stats.get('latest_date', 'N/A')}")
            print(f"  Unique dates: {stats.get('unique_dates', 'N/A'):,}")
            
            if args.stats_only:
                return
            
            # Perform retention cleanup
            print(f"\nRetention Cleanup:")
            results = manager.delete_old_records(
                args.table_name,
                args.date_column,
                args.retention_days,
                dry_run=dry_run,
                batch_size=args.batch_size
            )
            
            print(f"\nResults:")
            print(f"  Cutoff date: {results['cutoff_date']}")
            print(f"  Records identified: {results['records_identified']:,}")
            print(f"  Records deleted: {results['records_deleted']:,}")
            print(f"  Batches processed: {results['batches_processed']}")
        
        if dry_run:
            print(f"\n💡 This was a dry run. Use --execute to actually delete records.")
        else:
            print(f"\n✅ Deletion completed successfully!")
            
    except Exception as e:
        logger.error(f"Retention cleanup failed: {e}")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
