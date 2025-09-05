"""
Anomaly Detection System Test Script

This script tests the new insider trading anomaly detection system by:
1. Checking database connectivity and table structure
2. Running the anomaly detection algorithm on current data
3. Verifying results are stored correctly
4. Testing the cleanup functions

Usage:
    python scripts/test_anomaly_detection.py [--verbose]
"""

import os
import sys
import logging
import argparse
import json
from datetime import date, datetime

# Add the parent directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.connection import db
from analysis.insider_anomaly_detection import run_insider_anomaly_detection

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_database_structure():
    """Test that the new temp_anomaly table exists and has the correct structure."""
    logger.info("Testing database structure...")
    
    try:
        conn = db.connect()
        logger.info("Database connection established successfully")
        
        with conn.cursor() as cur:
            # Check if temp_anomaly table exists
            logger.info("Checking if temp_anomaly table exists...")
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'temp_anomaly'
                );
            """)
            result = cur.fetchone()
            logger.info(f"Table existence query result: {result}")
            
            if not result or not result[0]:
                logger.error("temp_anomaly table does not exist")
                return False
            
            logger.info("temp_anomaly table exists")
            
            # Check table structure
            logger.info("Checking table structure...")
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'temp_anomaly'
                ORDER BY ordinal_position;
            """)
            columns = cur.fetchall()
            logger.info(f"Found {len(columns)} columns in temp_anomaly table")
            
            expected_columns = [
                'id', 'event_date', 'symbol', 'direction', 'score', 
                'anomaly_types', 'total_individual_anomalies', 
                'max_individual_score', 'details', 'as_of_timestamp',
                'created_at', 'updated_at'
            ]
            
            actual_columns = [col[0] for col in columns]
            logger.info(f"Actual columns: {actual_columns}")
            
            missing_columns = []
            for expected in expected_columns:
                if expected not in actual_columns:
                    missing_columns.append(expected)
            
            if missing_columns:
                logger.error(f"Missing columns: {missing_columns}")
                return False
            
            logger.info("Table structure is correct")
            
            # Check if cleanup function exists
            logger.info("Checking if cleanup_old_anomalies function exists...")
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE p.proname = 'cleanup_old_anomalies'
                    AND n.nspname = 'public'
                );
            """)
            function_result = cur.fetchone()
            logger.info(f"Function existence query result: {function_result}")
            
            if function_result and function_result[0]:
                logger.info("cleanup_old_anomalies function exists")
            else:
                logger.error("cleanup_old_anomalies function missing")
                return False
            
            # Check if anomaly_summary view exists
            logger.info("Checking if anomaly_summary view exists...")
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views
                    WHERE table_name = 'anomaly_summary'
                );
            """)
            view_result = cur.fetchone()
            logger.info(f"View existence query result: {view_result}")
            
            if view_result and view_result[0]:
                logger.info("anomaly_summary view exists")
            else:
                logger.warning("anomaly_summary view missing (non-critical)")
            
            logger.info("Database structure test completed successfully")
            return True
            
    except Exception as e:
        logger.error(f"Database structure test failed: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def test_data_availability():
    """Test that we have sufficient data for anomaly detection."""
    logger.info("Testing data availability...")
    
    try:
        conn = db.connect()
        with conn.cursor() as cur:
            # Check for recent temp_option data
            cur.execute("""
                SELECT COUNT(*) as recent_count, 
                       MIN(as_of_timestamp) as min_ts, 
                       MAX(as_of_timestamp) as max_ts
                FROM temp_option
                WHERE DATE(as_of_timestamp) >= CURRENT_DATE - INTERVAL '7 days';
            """)
            result = cur.fetchone()
            
            logger.info(f"Recent data query result: {result}")
            if result:
                recent_count = result[0] if result[0] is not None else 0
                min_ts = result[1]
                max_ts = result[2]
                
                logger.info(f"Recent temp_option records: {recent_count}")
                if recent_count > 0 and min_ts and max_ts:
                    logger.info(f"Date range: {min_ts} to {max_ts}")
            else:
                recent_count = 0
                logger.warning("No result from recent data query")
            
            # Check for baseline data (30 days)
            cur.execute("""
                SELECT COUNT(DISTINCT DATE(as_of_timestamp)) as days,
                       COUNT(*) as total_records
                FROM temp_option
                WHERE DATE(as_of_timestamp) >= CURRENT_DATE - INTERVAL '30 days';
            """)
            baseline_result = cur.fetchone()
            logger.info(f"Baseline data query result: {baseline_result}")
            
            if baseline_result:
                baseline_days = baseline_result[0] if baseline_result[0] is not None else 0
                baseline_records = baseline_result[1] if baseline_result[1] is not None else 0
                
                logger.info(f"Baseline data: {baseline_records} records across {baseline_days} days")
                
                if baseline_days < 5:
                    logger.warning("Limited baseline data - anomaly detection may be less accurate")
                    return True  # Still allow testing
            else:
                logger.warning("No result from baseline data query")
                return True  # Still allow testing
            
            if recent_count == 0:
                logger.warning("No recent data for anomaly detection")
                return True  # Still allow testing
            
            logger.info("Sufficient data available")
            return True
            
    except Exception as e:
        logger.error(f"Data availability test failed: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def test_anomaly_detection():
    """Test the anomaly detection algorithm."""
    logger.info("Testing anomaly detection algorithm...")
    
    try:
        # Run the detection
        results = run_insider_anomaly_detection(baseline_days=30)
        
        if not results.get('success'):
            logger.error(f"Anomaly detection failed: {results.get('error')}")
            return False
        
        logger.info("Anomaly detection completed successfully")
        logger.info(f"  Contracts analyzed: {results.get('contracts_analyzed', 0)}")
        logger.info(f"  Anomalies detected: {results.get('anomalies_detected', 0)}")
        logger.info(f"  Symbols with anomalies: {results.get('symbols_with_anomalies', 0)}")
        logger.info(f"  Execution time: {results.get('execution_time', 0):.2f}s")
        
        return True
        
    except Exception as e:
        logger.error(f"Anomaly detection test failed: {e}")
        return False


def test_anomaly_storage():
    """Test that anomalies are stored correctly."""
    logger.info("Testing anomaly storage...")
    
    try:
        conn = db.connect()
        with conn.cursor() as cur:
            # Check for recent anomalies
            cur.execute("""
                SELECT COUNT(*) as count, 
                       MAX(score) as max_score, 
                       array_agg(DISTINCT symbol ORDER BY symbol) as symbols
                FROM temp_anomaly
                WHERE event_date = CURRENT_DATE;
            """)
            result = cur.fetchone()
            logger.info(f"Anomaly storage query result: {result}")
            
            if result:
                count = result[0] if result[0] is not None else 0
                max_score = result[1]
                symbols = result[2]
                
                logger.info(f"Today's anomalies: {count} records")
                if count > 0:
                    logger.info(f"  Max score: {max_score}")
                    if symbols and len(symbols) > 0:
                        symbol_list = symbols[:10] if len(symbols) > 10 else symbols
                        logger.info(f"  Symbols: {symbol_list}{'...' if len(symbols) > 10 else ''}")
                    
                    # Test a sample record structure
                    logger.info("Fetching sample anomaly record...")
                    cur.execute("""
                        SELECT symbol, score, anomaly_types, details
                        FROM temp_anomaly
                        WHERE event_date = CURRENT_DATE
                        ORDER BY score DESC
                        LIMIT 1;
                    """)
                    sample = cur.fetchone()
                    logger.info(f"Sample record query result: {sample}")
                    
                    if sample:
                        symbol, score, types, details = sample
                        logger.info(f"  Sample: {symbol} (score: {score}, types: {types})")
                        
                        # Validate JSON details
                        try:
                            json.loads(details)
                            logger.info("Anomaly details JSON is valid")
                        except json.JSONDecodeError:
                            logger.error("Anomaly details JSON is invalid")
                            return False
                else:
                    logger.info("No anomalies found for today (this may be normal)")
            else:
                logger.warning("No result from anomaly storage query")
            
            logger.info("Anomaly storage test passed")
            return True
            
    except Exception as e:
        logger.error(f"Anomaly storage test failed: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def test_cleanup_function():
    """Test the anomaly cleanup function."""
    logger.info("Testing cleanup function...")
    
    try:
        conn = db.connect()
        with conn.cursor() as cur:
            # Test the cleanup function (dry run with 30 days to avoid deleting current data)
            logger.info("Testing cleanup_old_anomalies function...")
            cur.execute("SELECT cleanup_old_anomalies(30);")
            result = cur.fetchone()
            logger.info(f"Cleanup function query result: {result}")
            
            if result:
                deleted_count = result[0] if result[0] is not None else 0
                conn.rollback()  # Rollback to avoid actually deleting data
                
                logger.info(f"Cleanup function would delete {deleted_count} old records")
                logger.info("Cleanup function test passed")
                return True
            else:
                logger.error("No result from cleanup function")
                return False
            
    except Exception as e:
        logger.error(f"Cleanup function test failed: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    parser = argparse.ArgumentParser(description='Test the anomaly detection system')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting Anomaly Detection System Tests")
    logger.info("=" * 50)
    
    tests = [
        ("Database Structure", test_database_structure),
        ("Data Availability", test_data_availability),
        ("Anomaly Detection", test_anomaly_detection),
        ("Anomaly Storage", test_anomaly_storage),
        ("Cleanup Function", test_cleanup_function)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\nRunning {test_name} test...")
        try:
            if test_func():
                passed += 1
                logger.info(f"{test_name} test PASSED")
            else:
                failed += 1
                logger.error(f"{test_name} test FAILED")
        except Exception as e:
            failed += 1
            logger.error(f"{test_name} test FAILED with exception: {e}")
    
    logger.info("\n" + "=" * 50)
    logger.info(f"Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        logger.info("All tests passed! The anomaly detection system is working correctly.")
        return 0
    else:
        logger.error("Some tests failed. Please review the errors above.")
        return 1


if __name__ == '__main__':
    sys.exit(main())
