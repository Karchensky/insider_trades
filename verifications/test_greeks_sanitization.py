"""
Test Greek Values Sanitization
Verifies that extreme/invalid greek values are properly filtered to prevent database overflow
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def sanitize_greek(value, name, max_abs_value):
    """
    Sanitize option greek values to prevent database overflow.
    Returns None if value is invalid or exceeds reasonable bounds.
    
    Args:
        value: The greek value from API
        name: Name of the greek (for logging)
        max_abs_value: Maximum absolute value considered valid
    """
    if value is None:
        return None
    try:
        val = float(value)
        # Check for NaN, Inf, or values exceeding reasonable bounds
        if not (-max_abs_value <= val <= max_abs_value):
            return None
        return val
    except (ValueError, TypeError, OverflowError):
        return None


def test_greeks_sanitization():
    """Test that problematic values from the GitHub Actions error are properly handled"""
    
    print("Testing Greek Values Sanitization")
    print("=" * 80)
    
    # Actual problematic values from the error log
    test_cases = [
        # (value, greek_name, max_bound, expected_result)
        ("-79324197870.17915", "vega", 10000.0, None, "79 billion vega - should be NULL"),
        ("2.482848081087278e+96", "delta", 10.0, None, "e+96 delta - astronomical, should be NULL"),
        ("9.591787197172369e+53", "delta", 10.0, None, "e+53 delta - astronomical, should be NULL"),
        ("-819579127093.8115", "vega", 10000.0, None, "819 billion vega - should be NULL"),
        ("1827245908966.899", "gamma", 1000.0, None, "1.8 trillion gamma - should be NULL"),
        ("10616327355.56986", "vega", 10000.0, None, "10 billion vega - should be NULL"),
        ("22290840770.19817", "gamma", 1000.0, None, "22 billion gamma - should be NULL"),
        ("12237183624041.338", "gamma", 1000.0, None, "12 trillion gamma - should be NULL"),
        ("597969045205.6749", "delta", 10.0, None, "597 billion delta - should be NULL"),
        
        # Valid edge cases that should pass
        ("0.5", "delta", 10.0, 0.5, "Normal delta value"),
        ("-0.75", "delta", 10.0, -0.75, "Normal negative delta"),
        ("0.05", "gamma", 1000.0, 0.05, "Normal gamma value"),
        ("50.0", "vega", 10000.0, 50.0, "Normal vega value"),
        ("-5.0", "theta", 1000.0, -5.0, "Normal theta value"),
        ("9.99", "delta", 10.0, 9.99, "Edge case: just under max delta"),
        ("999.9", "gamma", 1000.0, 999.9, "Edge case: just under max gamma"),
        ("9999.9", "vega", 10000.0, 9999.9, "Edge case: just under max vega"),
        
        # Values that should be filtered
        ("10.01", "delta", 10.0, None, "Just over max delta"),
        ("1000.01", "gamma", 1000.0, None, "Just over max gamma"),
        ("-10.01", "delta", 10.0, None, "Just under min delta"),
        (None, "delta", 10.0, None, "NULL input"),
        ("NaN", "delta", 10.0, None, "NaN string"),
        ("inf", "delta", 10.0, None, "Infinity string"),
    ]
    
    passed = 0
    failed = 0
    
    for value, name, max_val, expected, description in test_cases:
        result = sanitize_greek(value, name, max_val)
        
        if result == expected:
            status = "PASS"
            passed += 1
        else:
            status = "FAIL"
            failed += 1
            
        print(f"{status}: {description}")
        print(f"   Input: {value} | Expected: {expected} | Got: {result}")
        if status == "FAIL":
            print(f"   ERROR: Mismatch!")
        print()
    
    print("=" * 80)
    print(f"Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("\nSUCCESS: All tests passed!")
        return True
    else:
        print(f"\nFAILURE: {failed} tests failed")
        return False


def test_database_decimal_limits():
    """Verify our sanitization bounds fit within DECIMAL(18,8) constraints"""
    print("\n" + "=" * 80)
    print("Testing Database DECIMAL(18,8) Constraints")
    print("=" * 80)
    
    # DECIMAL(18,8) can store: -9999999999.99999999 to 9999999999.99999999
    # That's 10 digits before decimal, 8 after
    decimal_max = 9999999999.99999999
    
    max_bounds = {
        'delta': 10.0,
        'gamma': 1000.0,
        'theta': 1000.0,
        'vega': 10000.0
    }
    
    all_valid = True
    for name, max_val in max_bounds.items():
        fits = max_val <= decimal_max
        status = "PASS" if fits else "FAIL"
        print(f"{status}: {name} max={max_val:,.2f} | DB max={decimal_max:,.2f} | Fits: {fits}")
        if not fits:
            all_valid = False
    
    print("=" * 80)
    if all_valid:
        print("SUCCESS: All sanitization bounds fit within DECIMAL(18,8)")
        return True
    else:
        print("FAILURE: Some bounds exceed database limits")
        return False


if __name__ == '__main__':
    test1 = test_greeks_sanitization()
    test2 = test_database_decimal_limits()
    
    if test1 and test2:
        print("\n" + "=" * 80)
        print("ALL TESTS PASSED - Fix is working correctly!")
        print("=" * 80)
        sys.exit(0)
    else:
        print("\n" + "=" * 80)
        print("SOME TESTS FAILED - Review output above")
        print("=" * 80)
        sys.exit(1)

