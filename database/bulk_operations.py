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
from typing import List, Dict, Any, Optional, Tuple, Iterable
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

    # ===== Full Market Snapshot (stocks) → temp_stock =====
    def prepare_temp_snapshot_data_for_copy(self, polygon_response: Dict[str, Any]) -> Tuple[StringIO, int]:
        """
        Prepare full market snapshot data (single-call) for COPY into temp_stock.
        Maps: day, lastQuote, lastTrade, and updated (as_of_timestamp), ticker.
        """
        tickers = polygon_response.get('tickers')
        if not tickers:
            logger.warning("No tickers found in Polygon response")
            return StringIO(""), 0

        buffer = StringIO()
        rows = 0

        for t in tickers:
            try:
                as_of_ts = t.get('updated')
                symbol_val = t.get('ticker')
                if not as_of_ts or not symbol_val:
                    continue

                day = (t.get('day') or {})
                dq_o = day.get('o'); dq_h = day.get('h'); dq_l = day.get('l'); dq_c = day.get('c')
                dq_v = day.get('v'); dq_vw = day.get('vw')

                q = (t.get('lastQuote') or {})
                # We are dropping last quote/trade capture per requirements

                def f(v):
                    return str(v) if v is not None else '\\N'

                # Type normalization for integer columns
                def to_int(v):
                    if v is None:
                        return None
                    try:
                        # Handle floats like 1245823.0
                        return int(float(v))
                    except Exception:
                        return None

                def to_ts(v):
                    if v is None:
                        return None
                    try:
                        # Convert epoch in nanoseconds to ISO timestamp string for COPY into TIMESTAMPTZ
                        # Accept ints or floats
                        ns = int(float(v))
                        # seconds as float
                        secs = ns / 1_000_000_000
                        from datetime import datetime, timezone
                        dt = datetime.fromtimestamp(secs, tz=timezone.utc)
                        return dt.isoformat()
                    except Exception:
                        return None

                # Normalize numeric types
                as_of_ts = to_ts(as_of_ts)
                dq_v = to_int(dq_v)
                # no bid/ask/trade fields used

                buffer.write(
                    f"{f(as_of_ts)}\t{symbol_val}\t"
                    f"{f(dq_o)}\t{f(dq_h)}\t{f(dq_l)}\t{f(dq_c)}\t{f(dq_v)}\t{f(dq_vw)}\n"
                )
                rows += 1
            except Exception as e:
                logger.debug(f"Skipping malformed ticker snapshot: {e}")
                continue

        buffer.seek(0)
        return buffer, rows

    def bulk_upsert_temp_snapshots_copy(self, polygon_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Bulk upsert temp stock snapshots using PostgreSQL COPY for maximum performance.
        Returns a result dict with success, records_processed, execution_time, records_per_second.
        """
        start = time.time()
        try:
            csv_data, record_count = self.prepare_temp_snapshot_data_for_copy(polygon_response)
            if record_count == 0:
                return {'success': True, 'records_processed': 0, 'execution_time': time.time() - start, 'records_per_second': 0}

            conn = db.connect()
            # Use uuid to avoid name collisions in rapid successive calls
            import uuid
            temp_table = f"temp_snapshot_bulk_{uuid.uuid4().hex}"
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TEMP TABLE {temp_table} (
                        as_of_timestamp TIMESTAMPTZ,
                        symbol VARCHAR(10),
                        day_open DECIMAL(12, 4),
                        day_high DECIMAL(12, 4),
                        day_low DECIMAL(12, 4),
                        day_close DECIMAL(12, 4),
                        day_volume BIGINT,
                        day_vwap DECIMAL(12, 4)
                    );
                    """
                )

                logger.info("Performing COPY into temp table…")
                cursor.copy_from(
                    csv_data,
                    temp_table,
                    sep='\t',
                    null='\\N',
                    columns=(
                        'as_of_timestamp','symbol',
                        'day_open','day_high','day_low','day_close','day_volume','day_vwap'
                    )
                )

                logger.info("Upserting into temp_stock…")
                cursor.execute(
                    f"""
                    INSERT INTO temp_stock (
                        as_of_timestamp, symbol,
                        day_open, day_high, day_low, day_close, day_volume, day_vwap
                    )
                    SELECT 
                        as_of_timestamp, symbol,
                        day_open, day_high, day_low, day_close, day_volume, day_vwap
                    FROM {temp_table}
                    ON CONFLICT (as_of_timestamp, symbol)
                    DO UPDATE SET
                        day_open = EXCLUDED.day_open,
                        day_high = EXCLUDED.day_high,
                        day_low = EXCLUDED.day_low,
                        day_close = EXCLUDED.day_close,
                        day_volume = EXCLUDED.day_volume,
                        day_vwap = EXCLUDED.day_vwap,
                        updated_at = CURRENT_TIMESTAMP;
                    """
                )

                affected = cursor.rowcount
                conn.commit()

            elapsed = time.time() - start
            rps = (affected or 0) / elapsed if elapsed > 0 else 0
            logger.info(f"✓ Temp snapshots COPY completed: {affected} rows in {elapsed:.2f}s ({rps:.0f} rec/s)")
            return {'success': True, 'records_processed': affected, 'execution_time': elapsed, 'records_per_second': rps}

        except Exception as e:
            try:
                conn = db.connect()
                conn.rollback()
            except Exception:
                pass
            logger.error(f"Temp snapshots COPY failed: {e}")
            return {'success': False, 'error': str(e), 'records_processed': 0, 'execution_time': time.time() - start, 'records_per_second': 0}

    # ===== Unified Snapshot (options) → temp_option =====
    def prepare_temp_option_for_copy(self, snapshot_response: Dict[str, Any]) -> Tuple[StringIO, int]:
        results = snapshot_response.get('results') or []
        buf = StringIO()
        rows = 0

        from datetime import datetime, timezone
        def to_ts(ns):
            if ns is None:
                return None
            try:
                ns_int = int(float(ns))
                return datetime.fromtimestamp(ns_int / 1_000_000_000, tz=timezone.utc).isoformat()
            except Exception:
                return None

        def f(v):
            return str(v) if v is not None else '\\N'

        for r in results:
            try:
                if r.get('type') != 'options':
                    continue
                contract_ticker = r.get('ticker')
                # as_of: prefer r.last_updated else now
                as_of = to_ts(r.get('last_updated')) or datetime.now(timezone.utc).isoformat()

                details = r.get('details') or {}
                greeks = r.get('greeks') or {}
                session = r.get('session') or {}
                underlying = r.get('underlying_asset') or {}

                # underlying stock symbol
                symbol_val = underlying.get('ticker') or ''

                buf.write(
                    f"{f(as_of)}\t{symbol_val}\t{contract_ticker}\t"
                    f"{f(r.get('break_even_price'))}\t{f(details.get('strike_price'))}\t{f(r.get('implied_volatility'))}\t{f(r.get('open_interest'))}\t"
                    f"{f(greeks.get('delta'))}\t{f(greeks.get('gamma'))}\t{f(greeks.get('theta'))}\t{f(greeks.get('vega'))}\t"
                    f"{f(details.get('contract_type'))}\t{f(details.get('exercise_style'))}\t{f(details.get('expiration_date'))}\t{f(details.get('shares_per_contract'))}\t"
                    f"{f(session.get('open'))}\t{f(session.get('high'))}\t{f(session.get('low'))}\t{f(session.get('close'))}\t{f(session.get('volume'))}\t"
                    f"{f(session.get('change'))}\t{f(session.get('change_percent'))}\t"
                    f"{f(session.get('early_trading_change'))}\t{f(session.get('early_trading_change_percent'))}\t"
                    f"{f(session.get('regular_trading_change'))}\t{f(session.get('regular_trading_change_percent'))}\t"
                    f"{f(session.get('late_trading_change'))}\t{f(session.get('late_trading_change_percent'))}\t{f(session.get('previous_close'))}\t"
                    f"{f(underlying.get('ticker'))}\t{f(underlying.get('price'))}\t{f(underlying.get('change_to_break_even'))}\t{f(to_ts(underlying.get('last_updated')))}\n"
                )
                rows += 1
            except Exception as e:
                logger.debug(f"Skipping malformed option snapshot: {e}")
                continue

        buf.seek(0)
        return buf, rows

    def bulk_upsert_temp_option_copy(self, snapshot_response: Dict[str, Any]) -> Dict[str, Any]:
        start = time.time()
        try:
            csv_data, count = self.prepare_temp_option_for_copy(snapshot_response)
            if count == 0:
                return {'success': True, 'records_processed': 0, 'execution_time': time.time() - start, 'records_per_second': 0}

            conn = db.connect()
            import uuid
            temp_table = f"temp_opt_snapshot_{uuid.uuid4().hex}"
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TEMP TABLE {temp_table} (
                        as_of_timestamp TIMESTAMPTZ,
                        symbol VARCHAR(10),
                        contract_ticker VARCHAR(50),
                        break_even_price DECIMAL(18,6),
                        strike_price DECIMAL(18,6),
                        implied_volatility DECIMAL(18,8),
                        open_interest BIGINT,
                        greeks_delta DECIMAL(18,8),
                        greeks_gamma DECIMAL(18,8),
                        greeks_theta DECIMAL(18,8),
                        greeks_vega DECIMAL(18,8),
                        contract_type VARCHAR(10),
                        exercise_style VARCHAR(20),
                        expiration_date DATE,
                        shares_per_contract INTEGER,
                        session_open DECIMAL(18,6),
                        session_high DECIMAL(18,6),
                        session_low DECIMAL(18,6),
                        session_close DECIMAL(18,6),
                        session_volume BIGINT,
                        session_change DECIMAL(18,6),
                        session_change_percent DECIMAL(18,6),
                        session_early_trading_change DECIMAL(18,6),
                        session_early_trading_change_percent DECIMAL(18,6),
                        session_regular_trading_change DECIMAL(18,6),
                        session_regular_trading_change_percent DECIMAL(18,6),
                        session_late_trading_change DECIMAL(18,6),
                        session_late_trading_change_percent DECIMAL(18,6),
                        session_previous_close DECIMAL(18,6),
                        underlying_ticker VARCHAR(10),
                        underlying_price DECIMAL(18,6),
                        underlying_change_to_break_even DECIMAL(18,6),
                        underlying_last_updated TIMESTAMPTZ
                    );
                    """
                )

                cursor.copy_from(
                    csv_data,
                    temp_table,
                    sep='\t',
                    null='\\N',
                    columns=(
                        'as_of_timestamp','symbol','contract_ticker',
                        'break_even_price','strike_price','implied_volatility','open_interest',
                        'greeks_delta','greeks_gamma','greeks_theta','greeks_vega',
                        'contract_type','exercise_style','expiration_date','shares_per_contract',
                        'session_open','session_high','session_low','session_close','session_volume',
                        'session_change','session_change_percent','session_early_trading_change','session_early_trading_change_percent',
                        'session_regular_trading_change','session_regular_trading_change_percent','session_late_trading_change','session_late_trading_change_percent',
                        'session_previous_close','underlying_ticker','underlying_price','underlying_change_to_break_even','underlying_last_updated'
                    )
                )

                cursor.execute(
                    f"""
                    INSERT INTO temp_option (
                        as_of_timestamp, symbol, contract_ticker,
                        break_even_price, strike_price, implied_volatility, open_interest,
                        greeks_delta, greeks_gamma, greeks_theta, greeks_vega,
                        contract_type, exercise_style, expiration_date, shares_per_contract,
                        session_open, session_high, session_low, session_close, session_volume,
                        session_change, session_change_percent, session_early_trading_change, session_early_trading_change_percent,
                        session_regular_trading_change, session_regular_trading_change_percent, session_late_trading_change, session_late_trading_change_percent,
                        session_previous_close, underlying_ticker, underlying_price, underlying_change_to_break_even, underlying_last_updated
                    )
                    SELECT 
                        as_of_timestamp, symbol, contract_ticker,
                        break_even_price, strike_price, implied_volatility, open_interest,
                        greeks_delta, greeks_gamma, greeks_theta, greeks_vega,
                        contract_type, exercise_style, expiration_date, shares_per_contract,
                        session_open, session_high, session_low, session_close, session_volume,
                        session_change, session_change_percent, session_early_trading_change, session_early_trading_change_percent,
                        session_regular_trading_change, session_regular_trading_change_percent, session_late_trading_change, session_late_trading_change_percent,
                        session_previous_close, underlying_ticker, underlying_price, underlying_change_to_break_even, underlying_last_updated
                    FROM {temp_table}
                    ON CONFLICT (as_of_timestamp, symbol, contract_ticker)
                    DO UPDATE SET
                        break_even_price = EXCLUDED.break_even_price,
                        strike_price = EXCLUDED.strike_price,
                        implied_volatility = EXCLUDED.implied_volatility,
                        open_interest = EXCLUDED.open_interest,
                        greeks_delta = EXCLUDED.greeks_delta,
                        greeks_gamma = EXCLUDED.greeks_gamma,
                        greeks_theta = EXCLUDED.greeks_theta,
                        greeks_vega = EXCLUDED.greeks_vega,
                        contract_type = EXCLUDED.contract_type,
                        exercise_style = EXCLUDED.exercise_style,
                        expiration_date = EXCLUDED.expiration_date,
                        shares_per_contract = EXCLUDED.shares_per_contract,
                        session_open = EXCLUDED.session_open,
                        session_high = EXCLUDED.session_high,
                        session_low = EXCLUDED.session_low,
                        session_close = EXCLUDED.session_close,
                        session_volume = EXCLUDED.session_volume,
                        session_change = EXCLUDED.session_change,
                        session_change_percent = EXCLUDED.session_change_percent,
                        session_early_trading_change = EXCLUDED.session_early_trading_change,
                        session_early_trading_change_percent = EXCLUDED.session_early_trading_change_percent,
                        session_regular_trading_change = EXCLUDED.session_regular_trading_change,
                        session_regular_trading_change_percent = EXCLUDED.session_regular_trading_change_percent,
                        session_late_trading_change = EXCLUDED.session_late_trading_change,
                        session_late_trading_change_percent = EXCLUDED.session_late_trading_change_percent,
                        session_previous_close = EXCLUDED.session_previous_close,
                        underlying_ticker = EXCLUDED.underlying_ticker,
                        underlying_price = EXCLUDED.underlying_price,
                        underlying_change_to_break_even = EXCLUDED.underlying_change_to_break_even,
                        underlying_last_updated = EXCLUDED.underlying_last_updated,
                        updated_at = CURRENT_TIMESTAMP;
                    """
                )

                affected = cursor.rowcount
                conn.commit()

            elapsed = time.time() - start
            rps = (affected or 0) / elapsed if elapsed > 0 else 0
            logger.info(f"✓ Temp option snapshots COPY completed: {affected} rows in {elapsed:.2f}s ({rps:.0f} rec/s)")
            return {'success': True, 'records_processed': affected, 'execution_time': elapsed, 'records_per_second': rps}
        except Exception as e:
            try:
                conn = db.connect()
                conn.rollback()
            except Exception:
                pass
            logger.error(f"Temp option snapshots COPY failed: {e}")
            return {'success': False, 'error': str(e), 'records_processed': 0, 'execution_time': time.time() - start, 'records_per_second': 0}
    
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
    
    def bulk_upsert_option_contracts_copy(self, polygon_response: Dict[str, Any], as_of_date: str, upsert_batch_size: int = 50000) -> bool:
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

                # Bulk load into temporary table using COPY (single buffer)
                logger.info("Performing COPY into temp table…")
                cursor.copy_from(
                    csv_buffer, 
                    temp_table,
                    sep='\t',
                    columns=('date', 'symbol', 'contract_ticker', 'contract_type', 
                            'expiration_date', 'strike_price', 'exercise_style', 
                            'shares_per_contract', 'primary_exchange', 'cfi_code')
                )
                
                # Create index to help ON CONFLICT matching
                cursor.execute(f"CREATE INDEX ON {temp_table}(date, symbol, contract_ticker);")

                # Perform bulk upsert from temporary table in slices to avoid server overload
                logger.info("Upserting from temp table in batches of %s…", upsert_batch_size)
                total_upserted = 0
                while True:
                    cursor.execute(
                        f"""
                        WITH batch AS (
                            SELECT ctid, date, symbol, contract_ticker, contract_type, expiration_date,
                                   strike_price, exercise_style, shares_per_contract, primary_exchange, cfi_code
                            FROM {temp_table}
                            LIMIT %s
                        )
                        , ins AS (
                            INSERT INTO option_contracts (
                                date, symbol, contract_ticker, contract_type, expiration_date,
                                strike_price, exercise_style, shares_per_contract, primary_exchange, cfi_code
                            )
                            SELECT date, symbol, contract_ticker, contract_type, expiration_date,
                                   strike_price, exercise_style, shares_per_contract, primary_exchange, cfi_code
                            FROM batch
                            ON CONFLICT (date, symbol, contract_ticker) DO UPDATE SET
                                contract_type = EXCLUDED.contract_type,
                                expiration_date = EXCLUDED.expiration_date,
                                strike_price = EXCLUDED.strike_price,
                                exercise_style = EXCLUDED.exercise_style,
                                shares_per_contract = EXCLUDED.shares_per_contract,
                                primary_exchange = EXCLUDED.primary_exchange,
                                cfi_code = EXCLUDED.cfi_code,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING 1
                        )
                        DELETE FROM {temp_table} WHERE ctid IN (SELECT ctid FROM batch)
                        RETURNING 1;
                        """,
                        (upsert_batch_size,)
                    )
                    deleted_in_batch = cursor.rowcount or 0
                    total_upserted += deleted_in_batch
                    if deleted_in_batch == 0:
                        break
                    logger.info("Upsert progress: %s / %s rows done", total_upserted, record_count)

                affected_rows = total_upserted
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
                
                # Handle None values for COPY (use \N for NULL)
                def format_value(val):
                    return str(val) if val is not None else '\\N'
                
                # Write CSV row (tab-separated for better performance)
                csv_buffer.write(f"{date_val}\t{symbol}\t{contract_ticker}\t{format_value(open_price)}\t{format_value(high_price)}\t{format_value(low_price)}\t{format_value(close_price)}\t{volume}\n")
                valid_records += 1
                
            except (KeyError, ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid option snapshot record: {e}")
                continue
        
        if valid_records == 0:
            logger.warning("No valid option snapshots after processing")
            return False
        
        csv_buffer.seek(0)  # Reset buffer position
        
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
                        open_price DECIMAL(12, 4),
                        high_price DECIMAL(12, 4),
                        low_price DECIMAL(12, 4),
                        close_price DECIMAL(12, 4),
                        volume BIGINT
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
                            'low_price', 'close_price', 'volume')
                )
                
                # Perform bulk upsert from temporary table
                logger.info("Performing bulk upsert...")
                cursor.execute(f"""
                    INSERT INTO daily_option_snapshot 
                    (date, symbol, contract_ticker, open_price, high_price, low_price, 
                     close_price, volume)
                    SELECT date, symbol, contract_ticker, open_price, high_price, low_price,
                           close_price, volume
                    FROM {temp_table}
                    ON CONFLICT (date, symbol, contract_ticker) 
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
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

    def bulk_upsert_option_snapshots_from_flat_rows(self, rows: Iterable[Dict[str, Any]], default_date: Optional[str] = None) -> bool:
        """
        Bulk upsert option daily snapshots from Polygon flat-file day aggregates rows.

        Expected row fields (CSV headers):
        - ticker, volume, open, close, high, low, window_start, transactions

        Maps to daily_option_snapshot columns via COPY into a temp table, then UPSERT.
        """
        start_time = time.time()

        # Prepare data for COPY directly from the iterator
        buffer = StringIO()
        valid_records = 0

        def to_date_from_window_start(ws_val: Any) -> Optional[str]:
            if ws_val in (None, "", "\\N"):
                return default_date
            try:
                # window_start is epoch in nanoseconds; accept scientific notation
                ns = int(float(ws_val))
                # Convert to seconds
                secs = ns / 1_000_000_000
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(secs, tz=timezone.utc)
                return dt.strftime('%Y-%m-%d')
            except Exception:
                return default_date

        for r in rows:
            try:
                contract_ticker = r.get('ticker') or r.get('symbol') or ''
                if not contract_ticker:
                    continue

                # Date for the record
                date_val = default_date or to_date_from_window_start(r.get('window_start')) or ''

                # Derive underlying symbol from contract ticker (same logic as API path)
                symbol = ''
                if contract_ticker and ':' in contract_ticker:
                    parts = contract_ticker.split(':')
                    if len(parts) > 1:
                        option_part = parts[1]
                        import re
                        m = re.match(r'^([A-Z]+)', option_part)
                        if m:
                            symbol = m.group(1)

                def f_num(val: Any) -> Optional[float]:
                    if val in (None, "", "\\N"):
                        return None
                    try:
                        return float(val)
                    except Exception:
                        return None

                def f_int(val: Any) -> int:
                    if val in (None, "", "\\N"):
                        return 0
                    try:
                        return int(float(val))
                    except Exception:
                        return 0

                open_price = f_num(r.get('open'))
                high_price = f_num(r.get('high'))
                low_price = f_num(r.get('low'))
                close_price = f_num(r.get('close'))
                volume = f_int(r.get('volume'))

                def fmt(val: Optional[float]) -> str:
                    return str(val) if val is not None else '\\N'

                # Write one TSV line for COPY
                buffer.write(
                    f"{date_val}\t{symbol}\t{contract_ticker}\t{fmt(open_price)}\t{fmt(high_price)}\t{fmt(low_price)}\t{fmt(close_price)}\t{volume}\n"
                )
                valid_records += 1
            except Exception:
                continue

        if valid_records == 0:
            logger.warning("No valid option snapshot rows to process from flat file")
            return False

        buffer.seek(0)

        conn = db.connect()
        temp_table = f"temp_option_import_{int(time.time())}"

        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        date DATE,
                        symbol VARCHAR(10),
                        contract_ticker VARCHAR(50),
                        open_price DECIMAL(12, 4),
                        high_price DECIMAL(12, 4),
                        low_price DECIMAL(12, 4),
                        close_price DECIMAL(12, 4),
                        volume BIGINT
                    );
                    """
                )

                cursor.copy_from(
                    buffer,
                    temp_table,
                    sep='\t',
                    null='\\N',
                    columns=(
                        'date', 'symbol', 'contract_ticker', 'open_price', 'high_price',
                        'low_price', 'close_price', 'volume'
                    )
                )

                cursor.execute(
                    f"""
                    INSERT INTO daily_option_snapshot 
                    (date, symbol, contract_ticker, open_price, high_price, low_price, 
                     close_price, volume)
                    SELECT date, symbol, contract_ticker, open_price, high_price, low_price,
                           close_price, volume
                    FROM {temp_table}
                    ON CONFLICT (date, symbol, contract_ticker) 
                    DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        updated_at = CURRENT_TIMESTAMP;
                    """
                )

                affected_rows = cursor.rowcount
                conn.commit()

            elapsed = time.time() - start_time
            logger.info(
                f"✓ Option snapshots flat-file COPY completed: {affected_rows:,} rows in {elapsed:.2f}s "
                f"({(affected_rows/elapsed) if elapsed > 0 else 0:.0f} rec/s)"
            )
            # Update stats
            self.stats['total_records'] += valid_records
            self.stats['successful_records'] += affected_rows
            self.stats['batches_processed'] += 1
            self.stats['total_time'] += elapsed
            self.stats['records_per_second'] = self.stats['successful_records'] / max(self.stats['total_time'], 1e-9)

            return True
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.error(f"Option snapshots flat-file COPY upsert failed: {e}")
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

    def update_daily_option_snapshot_analytics_from_temp(self, target_date: str) -> Dict[str, Any]:
        """
        For a given date, update the analytics columns in daily_option_snapshot
        with the latest data from temp_option.
        
        This replaces the previous approach of copying to full_daily_option_snapshot.
        Now we directly update the implied_volatility, open_interest, and Greeks columns
        in the daily_option_snapshot table.
        """
        start = time.time()
        conn = db.connect()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE daily_option_snapshot dos
                SET 
                    implied_volatility = temp.implied_volatility,
                    open_interest = temp.open_interest,
                    greeks_delta = temp.greeks_delta,
                    greeks_gamma = temp.greeks_gamma,
                    greeks_theta = temp.greeks_theta,
                    greeks_vega = temp.greeks_vega,
                    updated_at = CURRENT_TIMESTAMP
                FROM (
                    SELECT DISTINCT ON (symbol, contract_ticker)
                        symbol,
                        contract_ticker,
                        implied_volatility,
                        open_interest,
                        greeks_delta,
                        greeks_gamma,
                        greeks_theta,
                        greeks_vega
                    FROM temp_option
                    WHERE DATE(as_of_timestamp) = %s
                    ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                ) temp
                WHERE dos.date = %s
                  AND dos.symbol = temp.symbol
                  AND dos.contract_ticker = temp.contract_ticker;
                """,
                (target_date, target_date)
            )
            affected = cursor.rowcount or 0
            conn.commit()
        return {
            'success': True,
            'records_updated': affected,
            'execution_time': time.time() - start
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
