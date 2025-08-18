"""
High-performance bulk database operations for large-scale data imports.
Optimized for Supabase PostgreSQL with multiple loading strategies.
"""

import os
import sys
import logging
import tempfile
import time
from io import StringIO
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.stock_data import StockDataManager

logger = logging.getLogger(__name__)


class BulkStockDataLoader:
    """
    High-performance bulk loader for stock data with multiple optimization strategies.
    """
    
    def __init__(self):
        self.batch_size = 5000  # Records per batch
        self.use_copy = True    # Use COPY for maximum speed
        self.stats = {
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'batches_processed': 0,
            'total_time': 0,
            'records_per_second': 0
        }
    
    def prepare_data_for_copy(self, polygon_response: Dict[str, Any]) -> Tuple[StringIO, int]:
        """
        Prepare data in CSV format for PostgreSQL COPY command.
        This is the fastest way to bulk load data.
        """
        if not polygon_response.get('results'):
            logger.warning("No results found in Polygon response")
            return StringIO(""), 0
        
        csv_buffer = StringIO()
        valid_records = 0
        
        for result in polygon_response['results']:
            try:
                # Prepare CSV row
                date_val = StockDataManager.polygon_timestamp_to_date(result['t'])
                symbol = result['T']
                close = float(result['c'])
                high = float(result['h'])
                low = float(result['l'])
                transaction_volume = int(result.get('n', 0))
                open_price = float(result['o'])
                trading_volume = int(result['v'])
                weighted_avg_price = float(result.get('vw', result['c']))
                
                # Write CSV row (tab-separated for better performance)
                csv_buffer.write(f"{date_val}\t{symbol}\t{close}\t{high}\t{low}\t{transaction_volume}\t{open_price}\t{trading_volume}\t{weighted_avg_price}\n")
                valid_records += 1
                
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid record: {e}")
                continue
        
        csv_buffer.seek(0)  # Reset buffer position
        return csv_buffer, valid_records
    
    def bulk_upsert_copy(self, polygon_response: Dict[str, Any]) -> bool:
        """
        Ultra-fast bulk upsert using PostgreSQL COPY to temporary table + UPSERT.
        This is the fastest method for large datasets.
        """
        start_time = time.time()
        
        # Prepare data
        csv_buffer, record_count = self.prepare_data_for_copy(polygon_response)
        
        if record_count == 0:
            logger.warning("No valid records to process")
            return False
        
        logger.info(f"Starting bulk COPY upsert for {record_count:,} records...")
        
        conn = db.connect()
        temp_table = f"temp_stock_import_{int(time.time())}"
        
        try:
            with conn.cursor() as cursor:
                # Create temporary table with same structure
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        date DATE,
                        symbol VARCHAR(10),
                        close DECIMAL(12, 4),
                        high DECIMAL(12, 4),
                        low DECIMAL(12, 4),
                        transaction_volume INTEGER,
                        open DECIMAL(12, 4),
                        trading_volume BIGINT,
                        weighted_average_price DECIMAL(12, 4)
                    );
                """)
                
                # Bulk load into temporary table using COPY
                logger.info("Performing COPY operation...")
                cursor.copy_from(
                    csv_buffer, 
                    temp_table,
                    sep='\t',
                    columns=('date', 'symbol', 'close', 'high', 'low', 'transaction_volume', 
                            'open', 'trading_volume', 'weighted_average_price')
                )
                
                # Perform bulk upsert from temporary table
                logger.info("Performing bulk upsert...")
                cursor.execute(f"""
                    INSERT INTO daily_stock_snapshot 
                    (date, symbol, close, high, low, transaction_volume, open, trading_volume, weighted_average_price)
                    SELECT date, symbol, close, high, low, transaction_volume, open, trading_volume, weighted_average_price
                    FROM {temp_table}
                    ON CONFLICT (date, symbol) 
                    DO UPDATE SET
                        close = EXCLUDED.close,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        transaction_volume = EXCLUDED.transaction_volume,
                        open = EXCLUDED.open,
                        trading_volume = EXCLUDED.trading_volume,
                        weighted_average_price = EXCLUDED.weighted_average_price,
                        updated_at = CURRENT_TIMESTAMP;
                """)
                
                # Get affected rows count
                affected_rows = cursor.rowcount
                
                # Commit transaction
                conn.commit()
                
                # Update statistics
                end_time = time.time()
                elapsed_time = end_time - start_time
                
                self.stats['total_records'] += record_count
                self.stats['successful_records'] += affected_rows
                self.stats['batches_processed'] += 1
                self.stats['total_time'] += elapsed_time
                self.stats['records_per_second'] = self.stats['successful_records'] / self.stats['total_time']
                
                logger.info(f"✓ Bulk COPY completed: {affected_rows:,} rows affected in {elapsed_time:.2f}s "
                           f"({affected_rows/elapsed_time:.0f} records/sec)")
                
                return True
                
        except Exception as e:
            logger.error(f"Bulk COPY upsert failed: {e}")
            conn.rollback()
            raise
        
        finally:
            # Cleanup temp table (automatically dropped with temp table)
            pass
    
    def bulk_upsert_execute_values(self, polygon_response: Dict[str, Any]) -> bool:
        """
        Fast bulk upsert using execute_values (fallback method).
        Faster than execute_many but slower than COPY.
        """
        start_time = time.time()
        
        if not polygon_response.get('results'):
            logger.warning("No results found in Polygon response")
            return False
        
        # Prepare data
        insert_data = []
        for result in polygon_response['results']:
            try:
                record = (
                    StockDataManager.polygon_timestamp_to_date(result['t']),
                    result['T'],
                    float(result['c']),
                    float(result['h']),
                    float(result['l']),
                    int(result.get('n', 0)),
                    float(result['o']),
                    int(result['v']),
                    float(result.get('vw', result['c']))
                )
                insert_data.append(record)
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid record: {e}")
                continue
        
        if not insert_data:
            logger.warning("No valid records to process")
            return False
        
        logger.info(f"Starting execute_values upsert for {len(insert_data):,} records...")
        
        conn = db.connect()
        
        try:
            with conn.cursor() as cursor:
                # Use execute_values for batch insert
                execute_values(
                    cursor,
                    """
                    INSERT INTO daily_stock_snapshot 
                    (date, symbol, close, high, low, transaction_volume, open, trading_volume, weighted_average_price)
                    VALUES %s
                    ON CONFLICT (date, symbol) 
                    DO UPDATE SET
                        close = EXCLUDED.close,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        transaction_volume = EXCLUDED.transaction_volume,
                        open = EXCLUDED.open,
                        trading_volume = EXCLUDED.trading_volume,
                        weighted_average_price = EXCLUDED.weighted_average_price,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    insert_data,
                    template=None,
                    page_size=self.batch_size
                )
                
                affected_rows = cursor.rowcount
                conn.commit()
                
                # Update statistics
                end_time = time.time()
                elapsed_time = end_time - start_time
                
                self.stats['total_records'] += len(insert_data)
                self.stats['successful_records'] += affected_rows
                self.stats['batches_processed'] += 1
                self.stats['total_time'] += elapsed_time
                self.stats['records_per_second'] = self.stats['successful_records'] / self.stats['total_time']
                
                logger.info(f"✓ execute_values completed: {affected_rows:,} rows affected in {elapsed_time:.2f}s "
                           f"({affected_rows/elapsed_time:.0f} records/sec)")
                
                return True
                
        except Exception as e:
            logger.error(f"execute_values upsert failed: {e}")
            conn.rollback()
            raise
    
    def prepare_option_data_for_copy(self, polygon_response: Dict[str, Any], as_of_date: str) -> Tuple[StringIO, int]:
        """
        Prepare option contracts data in CSV format for PostgreSQL COPY command.
        """
        if not polygon_response.get('results'):
            logger.warning("No results found in Polygon response")
            return StringIO(""), 0
        
        csv_buffer = StringIO()
        valid_records = 0
        
        for result in polygon_response['results']:
            try:
                # Prepare CSV row for option contracts
                date_val = as_of_date
                symbol = result['underlying_ticker']
                contract_ticker = result['ticker']
                contract_type = result.get('contract_type', '')
                expiration_date = result.get('expiration_date', '')
                strike_price = float(result.get('strike_price', 0))
                exercise_style = result.get('exercise_style', '')
                shares_per_contract = int(result.get('shares_per_contract', 100))
                primary_exchange = result.get('primary_exchange', '')
                cfi_code = result.get('cfi', '')
                
                # Write CSV row (tab-separated for better performance)
                csv_buffer.write(f"{date_val}\t{symbol}\t{contract_ticker}\t{contract_type}\t{expiration_date}\t{strike_price}\t{exercise_style}\t{shares_per_contract}\t{primary_exchange}\t{cfi_code}\n")
                valid_records += 1
                
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid option contract record: {e}")
                continue
        
        csv_buffer.seek(0)  # Reset buffer position
        return csv_buffer, valid_records
    
    def bulk_upsert_option_contracts_copy(self, polygon_response: Dict[str, Any], as_of_date: str) -> bool:
        """
        Ultra-fast bulk upsert for option contracts using PostgreSQL COPY.
        """
        start_time = time.time()
        
        # Prepare data
        csv_buffer, record_count = self.prepare_option_data_for_copy(polygon_response, as_of_date)
        
        if record_count == 0:
            logger.warning("No valid option contracts to process")
            return False
        
        logger.info(f"Starting bulk COPY upsert for {record_count:,} option contracts...")
        
        conn = db.connect()
        temp_table = f"temp_option_import_{int(time.time())}"
        
        try:
            with conn.cursor() as cursor:
                # Create temporary table with same structure
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        date DATE,
                        symbol VARCHAR(10),
                        contract_ticker VARCHAR(50),
                        contract_type VARCHAR(10),
                        expiration_date DATE,
                        strike_price DECIMAL(12, 4),
                        exercise_style VARCHAR(20),
                        shares_per_contract INTEGER,
                        primary_exchange VARCHAR(10),
                        cfi_code VARCHAR(10)
                    );
                """)
                
                # Bulk load into temporary table using COPY
                logger.info("Performing COPY operation...")
                cursor.copy_from(
                    csv_buffer, 
                    temp_table,
                    sep='\t',
                    columns=('date', 'symbol', 'contract_ticker', 'contract_type', 
                            'expiration_date', 'strike_price', 'exercise_style', 
                            'shares_per_contract', 'primary_exchange', 'cfi_code')
                )
                
                # Perform bulk upsert from temporary table
                logger.info("Performing bulk upsert...")
                cursor.execute(f"""
                    INSERT INTO option_contracts 
                    (date, symbol, contract_ticker, contract_type, expiration_date, 
                     strike_price, exercise_style, shares_per_contract, primary_exchange, 
                     cfi_code)
                    SELECT date, symbol, contract_ticker, contract_type, expiration_date,
                           strike_price, exercise_style, shares_per_contract, primary_exchange,
                           cfi_code
                    FROM {temp_table}
                    ON CONFLICT (date, symbol, contract_ticker) 
                    DO UPDATE SET
                        contract_type = EXCLUDED.contract_type,
                        expiration_date = EXCLUDED.expiration_date,
                        strike_price = EXCLUDED.strike_price,
                        exercise_style = EXCLUDED.exercise_style,
                        shares_per_contract = EXCLUDED.shares_per_contract,
                        primary_exchange = EXCLUDED.primary_exchange,
                        cfi_code = EXCLUDED.cfi_code,
                        updated_at = CURRENT_TIMESTAMP;
                """)
                
                # Get affected rows count
                affected_rows = cursor.rowcount
                
                # Commit transaction
                conn.commit()
                
                # Update statistics
                end_time = time.time()
                elapsed_time = end_time - start_time
                
                self.stats['total_records'] += record_count
                self.stats['successful_records'] += affected_rows
                self.stats['batches_processed'] += 1
                self.stats['total_time'] += elapsed_time
                self.stats['records_per_second'] = self.stats['successful_records'] / self.stats['total_time']
                
                logger.info(f"✓ Option contracts bulk COPY completed: {affected_rows:,} rows affected in {elapsed_time:.2f}s "
                           f"({affected_rows/elapsed_time:.0f} records/sec)")
                
                return True
                
        except Exception as e:
            logger.error(f"Option contracts bulk COPY upsert failed: {e}")
            conn.rollback()
            raise
    
    def bulk_insert_option_contracts_batch(self, batch_responses: List[Dict[str, Any]], as_of_date: str,
                                          method: str = 'copy') -> bool:
        """
        High-performance bulk insert for multiple option contract API responses.
        Processes all symbols for a date in a single batch operation.
        
        Args:
            batch_responses: List of Polygon API responses
            as_of_date: Date string for the as_of parameter
            method: Loading method ('copy', 'execute_values', or 'auto')
            
        Returns:
            bool: Success status
        """
        if not batch_responses:
            logger.warning("No API responses to process")
            return False
        
        # Combine all results from all API responses
        combined_results = []
        total_contracts = 0
        
        for response in batch_responses:
            if response and response.get('results'):
                combined_results.extend(response['results'])
                total_contracts += len(response['results'])
        
        if not combined_results:
            logger.warning("No option contracts found in batch responses")
            return True  # Not an error, just no data
        
        # Create a combined response structure
        combined_response = {
            "status": "OK",
            "results": combined_results,
            "resultsCount": total_contracts
        }
        
        logger.info(f"Processing batch of {total_contracts:,} option contracts from {len(batch_responses)} API responses using {method} method...")
        
        try:
            if method == 'copy' or (method == 'auto' and total_contracts > 100):
                return self.bulk_upsert_option_contracts_copy(combined_response, as_of_date)
            else:
                return self.bulk_upsert_option_contracts_copy(combined_response, as_of_date)
                
        except Exception as e:
            logger.error(f"Option contracts batch bulk insert failed with {method} method: {e}")
            raise
    
    def bulk_insert_option_snapshots_batch(self, batch_responses: List[Dict[str, Any]], 
                                          method: str = 'copy') -> bool:
        """
        High-performance bulk insert for multiple option snapshot API responses.
        Processes all snapshots for a date in a single batch operation.
        
        Args:
            batch_responses: List of Polygon Daily Ticker Summary API responses
            method: Loading method ('copy', 'execute_values', or 'auto')
            
        Returns:
            bool: Success status
        """
        if not batch_responses:
            logger.warning("No option snapshot responses to process")
            return False
        
        # Combine all results from all API responses
        combined_results = []
        total_snapshots = 0
        
        for response in batch_responses:
            if response and response.get('status') == 'OK':
                # Each response is a single snapshot (not an array)
                combined_results.append(response)
                total_snapshots += 1
        
        if not combined_results:
            logger.warning("No valid option snapshots found in batch responses")
            return True  # Not an error, just no data
        
        logger.info(f"Processing batch of {total_snapshots:,} option snapshots using {method} method...")
        
        try:
            return self.bulk_upsert_option_snapshots_copy(combined_results)
                
        except Exception as e:
            logger.error(f"Option snapshots batch bulk insert failed with {method} method: {e}")
            raise
    
    def bulk_upsert_option_snapshots_copy(self, snapshots: List[Dict[str, Any]]) -> bool:
        """
        Ultra-fast bulk upsert for option snapshots using PostgreSQL COPY.
        """
        start_time = time.time()
        record_count = len(snapshots)
        
        if record_count == 0:
            logger.warning("No valid option snapshots to process")
            return False
        
        logger.info(f"Starting bulk COPY upsert for {record_count:,} option snapshots...")
        
        # Prepare data for COPY
        csv_buffer = StringIO()
        valid_records = 0
        
        for snapshot in snapshots:
            try:
                # Extract data from Polygon Daily Ticker Summary response
                date_val = snapshot.get('from', '')
                contract_ticker = snapshot.get('symbol', '')
                
                # Extract underlying symbol from contract ticker (e.g., O:AAPL211119C00085000 -> AAPL)
                symbol = ''
                if contract_ticker and ':' in contract_ticker:
                    # Extract symbol from option ticker format
                    parts = contract_ticker.split(':')
                    if len(parts) > 1:
                        # Remove date and option type info to get just the symbol
                        option_part = parts[1]
                        # Extract symbol (everything before numbers/letters indicating date/strike)
                        import re
                        symbol_match = re.match(r'^([A-Z]+)', option_part)
                        if symbol_match:
                            symbol = symbol_match.group(1)
                
                open_price = float(snapshot.get('open', 0)) if snapshot.get('open') is not None else None
                high_price = float(snapshot.get('high', 0)) if snapshot.get('high') is not None else None
                low_price = float(snapshot.get('low', 0)) if snapshot.get('low') is not None else None
                close_price = float(snapshot.get('close', 0)) if snapshot.get('close') is not None else None
                volume = int(snapshot.get('volume', 0)) if snapshot.get('volume') is not None else 0
                pre_market_price = float(snapshot.get('preMarket', 0)) if snapshot.get('preMarket') is not None else None
                after_hours_price = float(snapshot.get('afterHours', 0)) if snapshot.get('afterHours') is not None else None
                
                # Handle None values for COPY (use \N for NULL)
                def format_value(val):
                    return str(val) if val is not None else '\\N'
                
                # Write CSV row (tab-separated for better performance)
                csv_buffer.write(f"{date_val}\t{symbol}\t{contract_ticker}\t{format_value(open_price)}\t{format_value(high_price)}\t{format_value(low_price)}\t{format_value(close_price)}\t{volume}\t{format_value(pre_market_price)}\t{format_value(after_hours_price)}\n")
                valid_records += 1
                
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid option snapshot record: {e}")
                continue
        
        if valid_records == 0:
            logger.warning("No valid option snapshots after processing")
            return False
        
        csv_buffer.seek(0)  # Reset buffer position
        
        conn = db.connect()
        temp_table = f"temp_option_snapshot_import_{int(time.time())}"
        
        try:
            with conn.cursor() as cursor:
                # Create temporary table with same structure
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        date DATE,
                        symbol VARCHAR(10),
                        contract_ticker VARCHAR(50),
                        open_price DECIMAL(12, 4),
                        high_price DECIMAL(12, 4),
                        low_price DECIMAL(12, 4),
                        close_price DECIMAL(12, 4),
                        volume BIGINT,
                        pre_market_price DECIMAL(12, 4),
                        after_hours_price DECIMAL(12, 4)
                    );
                """)
                
                # Bulk load into temporary table using COPY
                logger.info("Performing COPY operation...")
                cursor.copy_from(
                    csv_buffer, 
                    temp_table,
                    sep='\t',
                    null='\\N',
                    columns=('date', 'symbol', 'contract_ticker', 'open_price', 'high_price', 
                            'low_price', 'close_price', 'volume', 'pre_market_price', 
                            'after_hours_price')
                )
                
                # Perform bulk upsert from temporary table
                logger.info("Performing bulk upsert...")
                cursor.execute(f"""
                    INSERT INTO daily_option_snapshot 
                    (date, symbol, contract_ticker, open_price, high_price, low_price, 
                     close_price, volume, pre_market_price, after_hours_price)
                    SELECT date, symbol, contract_ticker, open_price, high_price, low_price,
                           close_price, volume, pre_market_price, after_hours_price
                    FROM {temp_table}
                    ON CONFLICT (date, symbol, contract_ticker) 
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        pre_market_price = EXCLUDED.pre_market_price,
                        after_hours_price = EXCLUDED.after_hours_price,
                        updated_at = CURRENT_TIMESTAMP;
                """)
                
                # Get affected rows count
                affected_rows = cursor.rowcount
                
                # Commit transaction
                conn.commit()
                
                # Update statistics
                end_time = time.time()
                elapsed_time = end_time - start_time
                
                self.stats['total_records'] += valid_records
                self.stats['successful_records'] += affected_rows
                self.stats['batches_processed'] += 1
                self.stats['total_time'] += elapsed_time
                self.stats['records_per_second'] = self.stats['successful_records'] / self.stats['total_time']
                
                logger.info(f"✓ Option snapshots bulk COPY completed: {affected_rows:,} rows affected in {elapsed_time:.2f}s "
                           f"({affected_rows/elapsed_time:.0f} records/sec)")
                
                return True
                
        except Exception as e:
            logger.error(f"Option snapshots bulk COPY upsert failed: {e}")
            conn.rollback()
            raise
    
    def bulk_insert_option_contracts(self, polygon_response: Dict[str, Any], as_of_date: str,
                                   method: str = 'copy') -> bool:
        """
        High-performance bulk insert for option contracts.
        
        Args:
            polygon_response: Response from Polygon Options API
            as_of_date: Date string for the as_of parameter
            method: Loading method ('copy', 'execute_values', or 'auto')
            
        Returns:
            bool: Success status
        """
        if not polygon_response.get('results'):
            logger.warning("No results found in Polygon response")
            return False
        
        record_count = len(polygon_response['results'])
        logger.info(f"Processing {record_count:,} option contracts using {method} method...")
        
        try:
            if method == 'copy' or (method == 'auto' and record_count > 100):
                return self.bulk_upsert_option_contracts_copy(polygon_response, as_of_date)
            else:
                # For smaller datasets, could implement execute_values method
                # For now, fallback to copy method
                return self.bulk_upsert_option_contracts_copy(polygon_response, as_of_date)
                
        except Exception as e:
            logger.error(f"Option contracts bulk insert failed with {method} method: {e}")
            raise
    
    def bulk_insert_daily_snapshots(self, polygon_response: Dict[str, Any], 
                                   method: str = 'copy') -> bool:
        """
        High-performance bulk insert with multiple loading strategies.
        
        Args:
            polygon_response: Response from Polygon API
            method: Loading method ('copy', 'execute_values', or 'auto')
            
        Returns:
            bool: Success status
        """
        if not polygon_response.get('results'):
            logger.warning("No results found in Polygon response")
            return False
        
        record_count = len(polygon_response['results'])
        logger.info(f"Processing {record_count:,} records using {method} method...")
        
        try:
            if method == 'copy' or (method == 'auto' and record_count > 1000):
                return self.bulk_upsert_copy(polygon_response)
            elif method == 'execute_values' or method == 'auto':
                return self.bulk_upsert_execute_values(polygon_response)
            else:
                # Fallback to original method
                return StockDataManager.insert_daily_snapshots(polygon_response)
                
        except Exception as e:
            logger.error(f"Bulk insert failed with {method} method: {e}")
            
            # Try fallback method if copy fails
            if method == 'copy':
                logger.info("Retrying with execute_values method...")
                return self.bulk_upsert_execute_values(polygon_response)
            else:
                raise
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return {
            'total_records_processed': self.stats['total_records'],
            'successful_records': self.stats['successful_records'],
            'failed_records': self.stats['failed_records'],
            'batches_processed': self.stats['batches_processed'],
            'total_time_seconds': round(self.stats['total_time'], 2),
            'average_records_per_second': round(self.stats['records_per_second'], 0),
            'success_rate_percent': round((self.stats['successful_records'] / max(self.stats['total_records'], 1)) * 100, 2)
        }
    
    def reset_stats(self):
        """Reset performance statistics."""
        self.stats = {
            'total_records': 0,
            'successful_records': 0,
            'failed_records': 0,
            'batches_processed': 0,
            'total_time': 0,
            'records_per_second': 0
        }


class DatabaseOptimizer:
    """
    Database connection and configuration optimizer for bulk operations.
    """
    
    @staticmethod
    def optimize_connection_for_bulk_ops():
        """
        Optimize database connection settings for bulk operations.
        """
        optimization_sql = [
            # Disable autocommit for better transaction control
            "SET autocommit = false;",
            
            # Increase work memory for sorting/hashing
            "SET work_mem = '256MB';",
            
            # Disable synchronous commits for speed (less durable but faster)
            "SET synchronous_commit = off;",
            
            # Increase checkpoint segments
            "SET checkpoint_segments = 32;",
            
            # Increase shared buffers if possible
            "SET shared_buffers = '256MB';"
        ]
        
        try:
            conn = db.connect()
            with conn.cursor() as cursor:
                for sql in optimization_sql:
                    try:
                        cursor.execute(sql)
                    except Exception as e:
                        logger.debug(f"Optimization setting skipped: {sql} - {e}")
            
            logger.info("Database optimized for bulk operations")
            
        except Exception as e:
            logger.warning(f"Failed to optimize database settings: {e}")
    
    @staticmethod
    def disable_triggers_temporarily(table_name: str = "daily_stock_snapshot"):
        """
        Temporarily disable triggers for faster bulk loading.
        """
        try:
            db.execute_command(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL;")
            logger.info(f"Triggers disabled for {table_name}")
        except Exception as e:
            logger.warning(f"Failed to disable triggers: {e}")
    
    @staticmethod
    def enable_triggers(table_name: str = "daily_stock_snapshot"):
        """
        Re-enable triggers after bulk loading.
        """
        try:
            db.execute_command(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL;")
            logger.info(f"Triggers enabled for {table_name}")
        except Exception as e:
            logger.warning(f"Failed to enable triggers: {e}")


# Convenience functions
def bulk_insert_polygon_data(polygon_response: Dict[str, Any], 
                            method: str = 'auto') -> bool:
    """
    Convenience function for bulk inserting Polygon data.
    
    Args:
        polygon_response: Polygon API response
        method: 'copy', 'execute_values', or 'auto'
    
    Returns:
        bool: Success status
    """
    loader = BulkStockDataLoader()
    
    # Optimize database for bulk operations
    DatabaseOptimizer.optimize_connection_for_bulk_ops()
    
    try:
        # Disable triggers for maximum speed
        DatabaseOptimizer.disable_triggers_temporarily()
        
        # Perform bulk insert
        success = loader.bulk_insert_daily_snapshots(polygon_response, method)
        
        if success:
            stats = loader.get_performance_stats()
            logger.info(f"Bulk insert performance: {stats['average_records_per_second']:.0f} records/sec")
        
        return success
        
    finally:
        # Always re-enable triggers
        DatabaseOptimizer.enable_triggers()


if __name__ == "__main__":
    # Test the bulk loader
    logging.basicConfig(level=logging.INFO)
    
    # Example usage
    sample_response = {
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
    
    success = bulk_insert_polygon_data(sample_response)
    print(f"Bulk insert test: {'SUCCESS' if success else 'FAILED'}")
