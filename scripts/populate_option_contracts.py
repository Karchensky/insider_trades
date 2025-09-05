#!/usr/bin/env python3
"""
Option Contracts Population Script

This script populates the option_contracts table for all symbols in daily_stock_snapshot.
Optimized for minimal API calls and maximum bulk loading efficiency.

Usage:
    python scripts/populate_option_contracts.py [--symbol-limit N] [--dry-run] [--clear-first]
"""

import argparse
import logging
import time
import sys
import os
from datetime import datetime
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.polygon_option_contracts_scraper import PolygonOptionContractsScraper
from database.connection import db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_symbol_stats() -> Dict:
    """Get statistics about available symbols."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            # Get distinct symbols from daily_stock_snapshot
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT symbol) as total_symbols,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM daily_stock_snapshot
            """)
            stats = cur.fetchone()
            
            # Get sample symbols
            cur.execute("""
                SELECT DISTINCT symbol 
                FROM daily_stock_snapshot 
                ORDER BY symbol 
                LIMIT 10
            """)
            sample_symbols = [row['symbol'] for row in cur.fetchall()]
            
            return {
                'total_symbols': stats['total_symbols'],
                'earliest_date': stats['earliest_date'],
                'latest_date': stats['latest_date'],
                'sample_symbols': sample_symbols
            }
    finally:
        conn.close()


def get_existing_contracts_stats() -> Dict:
    """Get statistics about existing option contracts."""
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_contracts,
                    COUNT(DISTINCT symbol) as symbols_with_contracts,
                    MIN(expiration_date) as earliest_expiration,
                    MAX(expiration_date) as latest_expiration,
                    MIN(created_at) as first_created,
                    MAX(updated_at) as last_updated
                FROM option_contracts
            """)
            result = cur.fetchone()
            return {
                'total_contracts': result['total_contracts'] or 0,
                'symbols_with_contracts': result['symbols_with_contracts'] or 0,
                'earliest_expiration': result['earliest_expiration'],
                'latest_expiration': result['latest_expiration'],
                'first_created': result['first_created'],
                'last_updated': result['last_updated']
            }
    finally:
        conn.close()


def clear_option_contracts() -> Dict:
    """Clear all existing option contracts."""
    logger.info("Clearing existing option contracts...")
    start_time = time.time()
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM option_contracts")
            before_count = cur.fetchone()['count']
            
            cur.execute("TRUNCATE TABLE option_contracts")
            conn.commit()
            
            duration = time.time() - start_time
            logger.info(f"Cleared {before_count:,} contracts in {duration:.2f}s")
            
            return {
                'success': True,
                'contracts_cleared': before_count,
                'duration_seconds': duration
            }
    except Exception as e:
        logger.error(f"Failed to clear contracts: {e}")
        conn.rollback()
        return {
            'success': False,
            'error': str(e),
            'contracts_cleared': 0,
            'duration_seconds': time.time() - start_time
        }
    finally:
        conn.close()


def populate_contracts(symbol_limit: int = None, dry_run: bool = False, smart_incremental: bool = False) -> Dict:
    """Populate option contracts for all symbols."""
    logger.info("Starting option contracts population...")
    start_time = time.time()
    
    # Initialize scraper
    scraper = PolygonOptionContractsScraper()
    
    try:
        if smart_incremental:
            # Smart incremental mode - only process symbols with new contracts
            symbols = scraper.get_symbols_with_new_contracts()
            if symbol_limit:
                symbols = symbols[:symbol_limit]
            
            logger.info(f"SMART INCREMENTAL MODE: Processing {len(symbols)} symbols with new contracts")
            
            if dry_run:
                if symbols:
                    logger.info("DRY RUN: Would process symbols with new contracts: " + ", ".join(symbols[:10]) + 
                               ("..." if len(symbols) > 10 else ""))
                else:
                    logger.info("DRY RUN: No symbols have new contracts - all up to date!")
                return {
                    'success': True,
                    'dry_run': True,
                    'smart_incremental': True,
                    'symbols_would_process': len(symbols),
                    'duration_seconds': time.time() - start_time
                }
            
            # Run smart incremental scraper
            results = scraper.scrape_incremental_smart(symbol_limit=symbol_limit)
        else:
            # Full mode - process all symbols
            symbols = scraper.get_active_symbols()
            if symbol_limit:
                symbols = symbols[:symbol_limit]
            
            logger.info(f"FULL MODE: Processing {len(symbols)} symbols (limit: {symbol_limit or 'none'})")
            
            if dry_run:
                logger.info("DRY RUN: Would process symbols: " + ", ".join(symbols[:10]) + 
                           ("..." if len(symbols) > 10 else ""))
                return {
                    'success': True,
                    'dry_run': True,
                    'smart_incremental': False,
                    'symbols_would_process': len(symbols),
                    'duration_seconds': time.time() - start_time
                }
            
            # Run full scraper
            results = scraper.scrape_all_symbols(symbol_limit=symbol_limit)
        
        duration = time.time() - start_time
        
        # Log detailed results
        if results.get('success'):
            logger.info(f"✓ Successfully processed all {len(symbols)} symbols")
            logger.info(f"✓ Total contracts loaded: {results['total_contracts']:,}")
            logger.info(f"✓ Average contracts per symbol: {results['total_contracts'] / len(symbols):.1f}")
            logger.info(f"✓ Total duration: {duration:.2f}s")
            logger.info(f"✓ Average time per symbol: {duration / len(symbols):.2f}s")
            
            if results.get('api_calls_made'):
                logger.info(f"✓ Total API calls: {results['api_calls_made']:,}")
                logger.info(f"✓ Average API calls per symbol: {results['api_calls_made'] / len(symbols):.1f}")
                logger.info(f"✓ Contracts per API call: {results['total_contracts'] / results['api_calls_made']:.1f}")
        else:
            logger.warning(f"⚠ Partial success: {results['successful_symbols']}/{len(symbols)} symbols")
            if results.get('failed_symbols'):
                failed_list = [item[0] if isinstance(item, tuple) else str(item) for item in results['failed_symbols'][:5]]
                logger.warning(f"⚠ Failed symbols: {', '.join(failed_list)}" + 
                             ("..." if len(results['failed_symbols']) > 5 else ""))
        
        results['duration_seconds'] = duration
        return results
        
    except Exception as e:
        logger.error(f"Population failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'duration_seconds': time.time() - start_time
        }


def main():
    parser = argparse.ArgumentParser(description='Populate option contracts table for performance testing')
    parser.add_argument('--symbol-limit', type=int, help='Limit number of symbols to process')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')
    parser.add_argument('--clear-first', action='store_true', help='Clear existing contracts before populating')
    parser.add_argument('--smart-incremental', action='store_true', help='Only process symbols with new contracts in daily_option_snapshot')
    
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("OPTION CONTRACTS POPULATION TEST")
    logger.info("=" * 60)
    
    # Show current state
    logger.info("Analyzing current state...")
    symbol_stats = get_symbol_stats()
    existing_stats = get_existing_contracts_stats()
    
    logger.info(f"Available symbols: {symbol_stats['total_symbols']:,}")
    logger.info(f"Date range: {symbol_stats['earliest_date']} to {symbol_stats['latest_date']}")
    logger.info(f"Sample symbols: {', '.join(symbol_stats['sample_symbols'])}")
    logger.info(f"Existing contracts: {existing_stats['total_contracts']:,} across {existing_stats['symbols_with_contracts']} symbols")
    
    if existing_stats['total_contracts'] > 0:
        logger.info(f"Expiration range: {existing_stats['earliest_expiration']} to {existing_stats['latest_expiration']}")
        logger.info(f"Last updated: {existing_stats['last_updated']}")
    
    # Clear existing contracts if requested
    if args.clear_first and not args.dry_run:
        clear_result = clear_option_contracts()
        if not clear_result['success']:
            logger.error("Failed to clear existing contracts, aborting")
            return 1
    
    # Populate contracts
    logger.info("-" * 60)
    populate_result = populate_contracts(
        symbol_limit=args.symbol_limit,
        dry_run=args.dry_run,
        smart_incremental=args.smart_incremental
    )
    
    # Final summary
    logger.info("-" * 60)
    logger.info("FINAL RESULTS:")
    
    if populate_result.get('success'):
        if args.dry_run:
            logger.info(f"✓ DRY RUN: Would process {populate_result['symbols_would_process']} symbols")
        else:
            logger.info(f"✓ Success: {populate_result.get('total_contracts', 0):,} contracts loaded")
            logger.info(f"✓ Symbols processed: {populate_result.get('successful_symbols', 0)}")
            
            # Performance metrics
            duration = populate_result.get('duration_seconds', 0)
            contracts = populate_result.get('total_contracts', 0)
            if duration > 0 and contracts > 0:
                logger.info(f"✓ Performance: {contracts / duration:.0f} contracts/second")
                
            # Show final state
            final_stats = get_existing_contracts_stats()
            logger.info(f"✓ Total contracts in database: {final_stats['total_contracts']:,}")
    else:
        logger.error(f"✗ Failed: {populate_result.get('error', 'Unknown error')}")
        return 1
    
    logger.info("=" * 60)
    return 0


if __name__ == '__main__':
    exit(main())
