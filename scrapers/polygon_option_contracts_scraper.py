"""
Polygon Option Contracts Scraper

Scrapes option contract metadata from Polygon.io API for all symbols in our database.
This captures static contract information (type, expiration, strike price, etc.)
separate from the pricing/volume data.

API Documentation: https://polygon.io/docs/rest/options/contracts/all-contracts
"""

import os
import sys
import logging
import requests
import time
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from io import StringIO

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from database.bulk_operations import BulkStockDataLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PolygonOptionContractsScraper:
    """Scraper for option contracts metadata from Polygon.io API."""
    
    def __init__(self):
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY environment variable is required")
        
        self.base_url = "https://api.polygon.io/v3/reference/options/contracts"
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}',
            'User-Agent': 'InsiderTradesAnalyzer/1.0'
        })
        
        # Rate limiting - Paid tier: 20 requests/second
        self.requests_per_second = 20
        self.min_request_interval = 1.0 / self.requests_per_second  # 0.05 seconds between requests
        self.last_request_time = 0
        
        # Performance tracking
        self.api_calls_made = 0
        self.total_contracts_fetched = 0
        
        # Bulk loader
        self.bulk_loader = BulkStockDataLoader()
    
    def _rate_limit(self):
        """Implement rate limiting to respect API limits (20 requests/second)."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f} seconds")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def get_active_symbols(self) -> List[str]:
        """Get list of active symbols from daily_stock_snapshot table."""
        logger.info("Getting active symbols from daily_stock_snapshot...")
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Get symbols from the most recent date
                cur.execute("""
                    SELECT DISTINCT symbol 
                    FROM daily_stock_snapshot 
                    WHERE date = (
                        SELECT MAX(date) 
                        FROM daily_stock_snapshot
                    )
                    ORDER BY symbol
                """)
                
                symbols = [row[0] if isinstance(row, tuple) else row['symbol'] for row in cur.fetchall()]
                logger.info(f"Found {len(symbols)} active symbols")
                return symbols
                
        finally:
            conn.close()
    
    def get_symbols_with_new_contracts(self) -> List[str]:
        """Get symbols that have contracts in daily_option_snapshot but not in option_contracts."""
        logger.info("Comparing daily_option_snapshot vs option_contracts to find symbols with new contracts...")
        
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Find contracts that exist in daily trading but not in our contracts table
                cur.execute("""
                    SELECT DISTINCT dos.symbol
                    FROM daily_option_snapshot dos
                    WHERE dos.date >= CURRENT_DATE - INTERVAL '2 days'  -- Recent trading activity
                      AND dos.volume > 0  -- Only contracts with actual trading
                      AND NOT EXISTS (
                        SELECT 1 
                        FROM option_contracts oc 
                        WHERE oc.symbol = dos.symbol 
                          AND oc.contract_ticker = dos.contract_ticker
                      )
                    ORDER BY dos.symbol
                """)
                
                symbols = [row['symbol'] for row in cur.fetchall()]
                logger.info(f"Found {len(symbols)} symbols with new contracts not in option_contracts table")
                
                if symbols:
                    logger.info(f"Symbols with new contracts: {', '.join(symbols[:10])}" + 
                               ("..." if len(symbols) > 10 else ""))
                
                return symbols
                
        except Exception as e:
            logger.error(f"Failed to get symbols with new contracts: {e}")
            return []
        finally:
            conn.close()

    def fetch_contracts_for_symbol(self, symbol: str, limit: int = 1000) -> List[Dict]:
        """Fetch active (non-expired) option contracts for a specific symbol."""
        logger.info(f"Fetching ACTIVE option contracts for {symbol}...")
        
        all_contracts = []
        next_url = None
        page = 1
        
        while True:
            self._rate_limit()
            
            # Build request URL
            if next_url:
                url = next_url
            else:
                params = {
                    'underlying_ticker': symbol,
                    'expired': 'false',  # Only get active contracts
                    'limit': limit,
                    'order': 'asc',
                    'sort': 'expiration_date'
                }
                url = self.base_url
            
            try:
                if next_url:
                    response = self.session.get(url)
                else:
                    response = self.session.get(url, params=params)
                
                # Track API calls
                self.api_calls_made += 1
                
                response.raise_for_status()
                data = response.json()
                
                if data.get('status') != 'OK':
                    logger.error(f"API error for {symbol}: {data}")
                    break
                
                results = data.get('results', [])
                if not results:
                    logger.info(f"No contracts found for {symbol}")
                    break
                
                all_contracts.extend(results)
                self.total_contracts_fetched += len(results)
                logger.info(f"{symbol}: Page {page}, got {len(results)} contracts (total: {len(all_contracts)})")
                
                # Check for next page
                next_url = data.get('next_url')
                if not next_url:
                    break
                
                page += 1
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {symbol}: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error for {symbol}: {e}")
                break
        
        logger.info(f"Completed {symbol}: {len(all_contracts)} total contracts")
        return all_contracts
    
    def normalize_contract_data(self, contracts: List[Dict]) -> List[Dict]:
        """Normalize contract data for database insertion."""
        normalized = []
        
        for contract in contracts:
            try:
                # Extract required fields with validation
                normalized_contract = {
                    'symbol': contract.get('underlying_ticker', '').upper(),
                    'contract_ticker': contract.get('ticker', ''),
                    'cfi': contract.get('cfi'),
                    'contract_type': contract.get('contract_type', '').lower(),
                    'exercise_style': contract.get('exercise_style', '').lower(),
                    'expiration_date': contract.get('expiration_date'),
                    'primary_exchange': contract.get('primary_exchange'),
                    'shares_per_contract': contract.get('shares_per_contract', 100),
                    'strike_price': contract.get('strike_price'),
                    'underlying_ticker': contract.get('underlying_ticker', '').upper()
                }
                
                # Validate required fields
                required_fields = ['symbol', 'contract_ticker', 'contract_type', 'expiration_date', 'strike_price']
                if all(normalized_contract.get(field) for field in required_fields):
                    # Validate contract type
                    if normalized_contract['contract_type'] in ['call', 'put']:
                        # Validate expiration date format
                        try:
                            datetime.strptime(normalized_contract['expiration_date'], '%Y-%m-%d')
                            normalized.append(normalized_contract)
                        except ValueError:
                            logger.warning(f"Invalid expiration date format: {contract}")
                    else:
                        logger.warning(f"Invalid contract type: {contract}")
                else:
                    logger.warning(f"Missing required fields: {contract}")
                    
            except Exception as e:
                logger.warning(f"Error normalizing contract {contract}: {e}")
        
        return normalized
    
    def bulk_upsert_contracts(self, contracts: List[Dict]) -> Dict[str, any]:
        """Bulk upsert option contracts using COPY method."""
        if not contracts:
            return {'success': True, 'rows_affected': 0}
        
        # Deduplicate contracts by (symbol, contract_ticker) to avoid ON CONFLICT issues
        unique_contracts = {}
        for contract in contracts:
            key = (contract.get('symbol'), contract.get('contract_ticker'))
            unique_contracts[key] = contract
        
        contracts = list(unique_contracts.values())
        logger.info(f"Bulk upserting {len(contracts)} unique option contracts...")
        
        # Create CSV data in memory
        csv_buffer = StringIO()
        
        # Note: Don't write headers for COPY operation
        # headers = [
        #     'symbol', 'contract_ticker', 'cfi', 'contract_type', 'exercise_style',
        #     'expiration_date', 'primary_exchange', 'shares_per_contract', 
        #     'strike_price', 'underlying_ticker'
        # ]
        # csv_buffer.write('\t'.join(headers) + '\n')
        
        # Write data rows
        headers = [
            'symbol', 'contract_ticker', 'cfi', 'contract_type', 'exercise_style',
            'expiration_date', 'primary_exchange', 'shares_per_contract', 
            'strike_price', 'underlying_ticker'
        ]
        
        for contract in contracts:
            row = []
            for header in headers:
                value = contract.get(header, '')
                if value is None:
                    value = ''
                row.append(str(value))
            csv_buffer.write('\t'.join(row) + '\n')
        
        csv_buffer.seek(0)
        
        # Perform bulk upsert
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                # Create temporary table
                cur.execute("""
                    CREATE TEMP TABLE temp_option_contracts (LIKE option_contracts INCLUDING DEFAULTS)
                """)
                
                # COPY data into temp table
                cur.copy_from(
                    csv_buffer,
                    'temp_option_contracts',
                    columns=headers,
                    sep='\t'
                )
                
                # Upsert from temp table to main table
                cur.execute("""
                    INSERT INTO option_contracts (
                        symbol, contract_ticker, cfi, contract_type, exercise_style,
                        expiration_date, primary_exchange, shares_per_contract,
                        strike_price, underlying_ticker, updated_at
                    )
                    SELECT 
                        symbol, contract_ticker, cfi, contract_type, exercise_style,
                        expiration_date::date, primary_exchange, shares_per_contract::integer,
                        strike_price::decimal, underlying_ticker, CURRENT_TIMESTAMP
                    FROM temp_option_contracts
                    ON CONFLICT (symbol, contract_ticker) 
                    DO UPDATE SET
                        cfi = EXCLUDED.cfi,
                        contract_type = EXCLUDED.contract_type,
                        exercise_style = EXCLUDED.exercise_style,
                        expiration_date = EXCLUDED.expiration_date,
                        primary_exchange = EXCLUDED.primary_exchange,
                        shares_per_contract = EXCLUDED.shares_per_contract,
                        strike_price = EXCLUDED.strike_price,
                        underlying_ticker = EXCLUDED.underlying_ticker,
                        updated_at = CURRENT_TIMESTAMP
                """)
                
                rows_affected = cur.rowcount
                # Clean up temp table
                cur.execute("DROP TABLE IF EXISTS temp_option_contracts")
                
                conn.commit()
                
                logger.info(f"Successfully upserted {rows_affected} option contracts")
                return {'success': True, 'rows_affected': rows_affected}
                
        except Exception as e:
            logger.error(f"Bulk upsert failed: {e}")
            conn.rollback()
            return {'success': False, 'error': str(e)}
        finally:
            conn.close()
    
    def scrape_all_symbols(self, symbol_limit: Optional[int] = None) -> Dict[str, any]:
        """Scrape option contracts for all active symbols."""
        start_time = datetime.now()
        logger.info("Starting option contracts scraping for all symbols...")
        
        # Get active symbols
        symbols = self.get_active_symbols()
        if symbol_limit:
            symbols = symbols[:symbol_limit]
            logger.info(f"Limited to first {symbol_limit} symbols")
        
        total_contracts = 0
        successful_symbols = 0
        failed_symbols = []
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"Processing symbol {i}/{len(symbols)}: {symbol}")
            
            try:
                # Fetch contracts for this symbol
                contracts = self.fetch_contracts_for_symbol(symbol)
                
                if contracts:
                    # Normalize data
                    normalized_contracts = self.normalize_contract_data(contracts)
                    
                    if normalized_contracts:
                        # Bulk upsert
                        result = self.bulk_upsert_contracts(normalized_contracts)
                        
                        if result['success']:
                            total_contracts += result['rows_affected']
                            successful_symbols += 1
                            logger.info(f"✓ {symbol}: {result['rows_affected']} contracts processed")
                        else:
                            failed_symbols.append((symbol, result['error']))
                            logger.error(f"✗ {symbol}: {result['error']}")
                    else:
                        logger.warning(f"⚠ {symbol}: No valid contracts after normalization")
                else:
                    logger.info(f"- {symbol}: No contracts found")
                    
            except Exception as e:
                failed_symbols.append((symbol, str(e)))
                logger.error(f"✗ {symbol}: Unexpected error: {e}")
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Summary
        logger.info("="*60)
        logger.info("OPTION CONTRACTS SCRAPING SUMMARY")
        logger.info("="*60)
        logger.info(f"Duration: {duration}")
        logger.info(f"Symbols processed: {len(symbols)}")
        logger.info(f"Successful: {successful_symbols}")
        logger.info(f"Failed: {len(failed_symbols)}")
        logger.info(f"Total contracts: {total_contracts}")
        logger.info(f"API calls made: {self.api_calls_made:,}")
        if self.api_calls_made > 0:
            logger.info(f"Efficiency: {self.total_contracts_fetched / self.api_calls_made:.1f} contracts per API call")
        
        if failed_symbols:
            logger.info("Failed symbols:")
            for symbol, error in failed_symbols:
                logger.info(f"  {symbol}: {error}")
        
        return {
            'success': len(failed_symbols) == 0,
            'duration': duration.total_seconds(),
            'symbols_processed': len(symbols),
            'successful_symbols': successful_symbols,
            'failed_symbols': failed_symbols,
            'total_contracts': total_contracts,
            'api_calls_made': self.api_calls_made,
            'total_contracts_fetched': self.total_contracts_fetched,
            'avg_contracts_per_symbol': total_contracts / successful_symbols if successful_symbols > 0 else 0,
            'avg_api_calls_per_symbol': self.api_calls_made / len(symbols) if len(symbols) > 0 else 0,
            'contracts_per_api_call': self.total_contracts_fetched / self.api_calls_made if self.api_calls_made > 0 else 0
        }

    def scrape_incremental_smart(self, symbol_limit: Optional[int] = None) -> Dict[str, any]:
        """Smart incremental scraping - only process symbols with new contracts in daily_option_snapshot."""
        start_time = datetime.now()
        logger.info("Starting SMART INCREMENTAL option contracts scraping...")
        
        # Get symbols that have new contracts in daily trading
        symbols = self.get_symbols_with_new_contracts()
        if symbol_limit:
            symbols = symbols[:symbol_limit]
            logger.info(f"Limited to first {symbol_limit} symbols")
        
        if not symbols:
            logger.info("No symbols have new contracts - all up to date!")
            return {
                'success': True,
                'symbols_processed': 0,
                'successful_symbols': 0,
                'failed_symbols': [],
                'total_contracts': 0,
                'duration': 0,
                'api_calls_made': 0,
                'incremental': True,
                'smart_incremental': True
            }
        
        logger.info(f"Processing {len(symbols)} symbols with new contracts")
        
        # Process symbols
        total_contracts = 0
        successful_symbols = 0
        failed_symbols = []
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"Processing symbol {i}/{len(symbols)}: {symbol}")
            
            try:
                # Fetch contracts for this symbol
                contracts = self.fetch_contracts_for_symbol(symbol)
                
                if contracts:
                    # Bulk upsert contracts
                    result = self.bulk_upsert_contracts(contracts)
                    if result.get('success'):
                        total_contracts += len(contracts)
                        successful_symbols += 1
                        logger.info(f"✓ {symbol}: {len(contracts)} contracts processed")
                    else:
                        failed_symbols.append((symbol, result.get('error', 'Bulk upsert failed')))
                        logger.error(f"✗ {symbol}: {result.get('error', 'Bulk upsert failed')}")
                else:
                    # Still count as successful - just no contracts found
                    successful_symbols += 1
                    logger.info(f"- {symbol}: No contracts found")
                    
            except Exception as e:
                failed_symbols.append((symbol, str(e)))
                logger.error(f"✗ {symbol}: {e}")
        
        # Summary
        duration = datetime.now() - start_time
        logger.info("="*60)
        logger.info("SMART INCREMENTAL OPTION CONTRACTS SCRAPING SUMMARY")
        logger.info("="*60)
        logger.info(f"Duration: {duration}")
        logger.info(f"Symbols processed: {len(symbols)}")
        logger.info(f"Successful: {successful_symbols}")
        logger.info(f"Failed: {len(failed_symbols)}")
        logger.info(f"Total contracts: {total_contracts}")
        logger.info(f"API calls made: {self.api_calls_made:,}")
        if self.api_calls_made > 0:
            logger.info(f"Efficiency: {self.total_contracts_fetched / self.api_calls_made:.1f} contracts per API call")
        
        if failed_symbols:
            logger.info("Failed symbols:")
            for symbol, error in failed_symbols:
                logger.info(f"  {symbol}: {error}")
        
        return {
            'success': len(failed_symbols) == 0,
            'duration': duration.total_seconds(),
            'symbols_processed': len(symbols),
            'successful_symbols': successful_symbols,
            'failed_symbols': failed_symbols,
            'total_contracts': total_contracts,
            'api_calls_made': self.api_calls_made,
            'total_contracts_fetched': self.total_contracts_fetched,
            'avg_contracts_per_symbol': total_contracts / successful_symbols if successful_symbols > 0 else 0,
            'avg_api_calls_per_symbol': self.api_calls_made / len(symbols) if len(symbols) > 0 else 0,
            'contracts_per_api_call': self.total_contracts_fetched / self.api_calls_made if self.api_calls_made > 0 else 0,
            'incremental': True,
            'smart_incremental': True
        }


def main():
    """Main entry point for standalone execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape option contracts from Polygon.io')
    parser.add_argument('--symbol-limit', type=int, help='Limit number of symbols to process')
    parser.add_argument('--symbol', type=str, help='Process single symbol only')
    
    args = parser.parse_args()
    
    scraper = PolygonOptionContractsScraper()
    
    if args.symbol:
        # Single symbol mode
        contracts = scraper.fetch_contracts_for_symbol(args.symbol)
        normalized = scraper.normalize_contract_data(contracts)
        result = scraper.bulk_upsert_contracts(normalized)
        print(f"Result: {result}")
    else:
        # All symbols mode
        result = scraper.scrape_all_symbols(symbol_limit=args.symbol_limit)
        print(f"Final result: {result}")


if __name__ == '__main__':
    main()
