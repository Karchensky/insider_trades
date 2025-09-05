"""
Database package for insider trades application.
Provides connection management and data operations for Supabase PostgreSQL.
"""

from .connection import db, DatabaseConnection
from .stock_data import StockDataManager

__all__ = ['db', 'DatabaseConnection', 'StockDataManager']
