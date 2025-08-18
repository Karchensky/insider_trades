"""
Database connection utility for Supabase PostgreSQL database.
Handles connection management and basic database operations.
"""

import os
import logging
from typing import Optional, Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages PostgreSQL database connections for Supabase."""
    
    def __init__(self):
        self.connection = None
        self.db_url = os.getenv('SUPABASE_DB_URL')
        
        if not self.db_url:
            raise ValueError("SUPABASE_DB_URL not found in environment variables")
    
    def connect(self) -> psycopg2.extensions.connection:
        """Establish database connection."""
        try:
            if self.connection is None or self.connection.closed:
                self.connection = psycopg2.connect(
                    self.db_url,
                    cursor_factory=RealDictCursor
                )
                logger.info("Database connection established")
            return self.connection
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except psycopg2.Error as e:
            logger.error(f"Query execution failed: {e}")
            conn.rollback()
            raise
    
    def execute_command(self, command: str, params: Optional[tuple] = None) -> bool:
        """Execute an INSERT/UPDATE/DELETE command."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.execute(command, params)
                conn.commit()
                logger.info(f"Command executed successfully: {command[:50]}...")
                return True
        except psycopg2.Error as e:
            logger.error(f"Command execution failed: {e}")
            conn.rollback()
            raise
    
    def execute_many(self, command: str, params_list: List[tuple]) -> bool:
        """Execute a command multiple times with different parameters."""
        conn = self.connect()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(command, params_list)
                conn.commit()
                logger.info(f"Batch command executed successfully: {len(params_list)} rows")
                return True
        except psycopg2.Error as e:
            logger.error(f"Batch command execution failed: {e}")
            conn.rollback()
            raise
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Singleton instance for global use
db = DatabaseConnection()
