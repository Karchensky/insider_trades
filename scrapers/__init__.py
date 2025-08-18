"""
Scrapers package for insider trades application.
Contains scripts to fetch data from various financial APIs.
"""

from .polygon_daily_scraper import PolygonDailyScraper

__all__ = ['PolygonDailyScraper']
