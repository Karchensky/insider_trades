"""
Stock data operations for DAILY_STOCK_SNAPSHOT table.
Handles insertion and retrieval of stock market data from Polygon API.
"""

import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from database.core.connection import db

logger = logging.getLogger(__name__)


class StockDataManager:
    """Manages stock data operations for the DAILY_STOCK_SNAPSHOT table."""
    
    @staticmethod
    def polygon_timestamp_to_date(timestamp: int) -> date:
        """Convert Polygon API Unix timestamp (milliseconds) to date."""
        return datetime.fromtimestamp(timestamp / 1000).date()
    
    @staticmethod
    def insert_daily_snapshots(polygon_response: Dict[str, Any]) -> bool:
        """
        Insert daily stock snapshots from Polygon API response.
        
        Args:
            polygon_response: Response from Polygon API with 'results' containing stock data
            
        Returns:
            bool: True if successful
        """
        if not polygon_response.get('results'):
            logger.warning("No results found in Polygon response")
            return False
        
        # Prepare data for insertion
        insert_data = []
        for result in polygon_response['results']:
            try:
                # Map Polygon API fields to database columns
                # Handle optional fields with defaults
                record = (
                    StockDataManager.polygon_timestamp_to_date(result['t']),  # date
                    result['T'],  # symbol
                    float(result['c']),  # close
                    float(result['h']),  # high
                    float(result['l']),  # low
                    int(result.get('n', 0)),  # transaction_volume (optional, default 0)
                    float(result['o']),  # open
                    int(result['v']),  # trading_volume
                    float(result.get('vw', result['c']))  # weighted_average_price (optional, default to close)
                )
                insert_data.append(record)
                
            except (KeyError, ValueError, TypeError) as e:
                logger.error(f"Error processing record {result}: {e}")
                logger.debug(f"Record data: {result}")
                continue
        
        if not insert_data:
            logger.error("No valid records to insert")
            return False
        
        # Insert data using ON CONFLICT to handle duplicates
        insert_sql = """
        INSERT INTO daily_stock_snapshot 
        (date, symbol, close, high, low, transaction_volume, open, trading_volume, weighted_average_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        """
        
        try:
            db.execute_many(insert_sql, insert_data)
            logger.info(f"Successfully inserted/updated {len(insert_data)} stock snapshots")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert stock snapshots: {e}")
            raise
    
    @staticmethod
    def get_daily_snapshot(symbol: str, snapshot_date: date) -> Optional[Dict[str, Any]]:
        """
        Get daily snapshot for a specific symbol and date.
        
        Args:
            symbol: Stock symbol
            snapshot_date: Date of the snapshot
            
        Returns:
            Dict containing the snapshot data or None if not found
        """
        query_sql = """
        SELECT * FROM daily_stock_snapshot 
        WHERE symbol = %s AND date = %s;
        """
        
        try:
            results = db.execute_query(query_sql, (symbol, snapshot_date))
            return results[0] if results else None
            
        except Exception as e:
            logger.error(f"Failed to get snapshot for {symbol} on {snapshot_date}: {e}")
            raise
    
    @staticmethod
    def get_symbol_history(symbol: str, start_date: Optional[date] = None, 
                          end_date: Optional[date] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get historical data for a symbol within a date range.
        
        Args:
            symbol: Stock symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            limit: Maximum number of records to return
            
        Returns:
            List of snapshot records
        """
        conditions = ["symbol = %s"]
        params = [symbol]
        
        if start_date:
            conditions.append("date >= %s")
            params.append(start_date)
        
        if end_date:
            conditions.append("date <= %s")
            params.append(end_date)
        
        query_sql = f"""
        SELECT * FROM daily_stock_snapshot 
        WHERE {' AND '.join(conditions)}
        ORDER BY date DESC
        LIMIT %s;
        """
        params.append(limit)
        
        try:
            return db.execute_query(query_sql, tuple(params))
            
        except Exception as e:
            logger.error(f"Failed to get history for {symbol}: {e}")
            raise
    
    @staticmethod
    def get_market_snapshot(snapshot_date: date, limit: Optional[int] = 1000) -> List[Dict[str, Any]]:
        """
        Get market snapshot for a specific date.
        
        Args:
            snapshot_date: Date of the snapshot
            limit: Maximum number of records to return (None for no limit)
            
        Returns:
            List of all stock snapshots for the date
        """
        if limit is None:
            # No limit - get all records
            query_sql = """
            SELECT * FROM daily_stock_snapshot 
            WHERE date = %s
            ORDER BY trading_volume DESC;
            """
            params = (snapshot_date,)
        else:
            # With limit
            query_sql = """
            SELECT * FROM daily_stock_snapshot 
            WHERE date = %s
            ORDER BY trading_volume DESC
            LIMIT %s;
            """
            params = (snapshot_date, limit)
        
        try:
            return db.execute_query(query_sql, params)
            
        except Exception as e:
            logger.error(f"Failed to get market snapshot for {snapshot_date}: {e}")
            raise
    
    @staticmethod
    def get_latest_snapshots(limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get the most recent snapshots across all symbols.
        
        Args:
            limit: Maximum number of records to return
            
        Returns:
            List of recent snapshots
        """
        query_sql = """
        SELECT * FROM daily_stock_snapshot 
        ORDER BY date DESC, trading_volume DESC
        LIMIT %s;
        """
        
        try:
            return db.execute_query(query_sql, (limit,))
            
        except Exception as e:
            logger.error(f"Failed to get latest snapshots: {e}")
            raise
    
    @staticmethod
    def get_table_stats() -> Dict[str, Any]:
        """
        Get basic statistics about the table.
        
        Returns:
            Dict containing table statistics
        """
        stats_sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT symbol) as unique_symbols,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            MIN(created_at) as first_insert,
            MAX(updated_at) as last_update
        FROM daily_stock_snapshot;
        """
        
        try:
            results = db.execute_query(stats_sql)
            return results[0] if results else {}
            
        except Exception as e:
            logger.error(f"Failed to get table stats: {e}")
            raise
