# Option Greeks Numeric Overflow Fix

## Problem Summary

The intraday scheduled job was failing in GitHub Actions with database numeric overflow errors when loading option snapshots. The errors occurred during COPY operations into temp tables.

### Error Examples
```
numeric field overflow
DETAIL: A field with precision 18, scale 8 must round to an absolute value less than 10^10.
CONTEXT: COPY temp_opt_snapshot_..., line 22010, column greeks_vega: "-79324197870.17915"
CONTEXT: COPY temp_opt_snapshot_..., line 9709, column greeks_delta: "2.482848081087278e+96"
```

## Root Cause

Polygon API occasionally returns corrupted/invalid values for option greeks:
- **Delta** normally ranges from -1 to 1, but API returned `2.482848081087278e+96`
- **Gamma** normally ranges from 0 to ~0.1, but API returned `12237183624041.338` (12 trillion)
- **Vega** normally ranges from 0 to ~100, but API returned `-819579127093.8115` (819 billion)
- **Theta** normally ranges from -10 to 0, but received similar extreme values

The database columns are defined as `DECIMAL(18,8)`:
- Maximum value: ~10,000,000,000 (10 billion)
- The corrupt values from the API exceeded this limit

## Solution Implementation

Added data validation/sanitization in `database/core/bulk_operations.py` before database insertion.

### Changes Made

1. **Added sanitize_greek() function** (lines 226-249)
   - Validates greek values are within reasonable bounds
   - Returns None (NULL) for invalid/extreme values
   - Handles NaN, Infinity, and overflow cases
   - Tracks count of sanitized values

2. **Applied sanitization before COPY** (lines 286-295)
   - Delta: max ±10.0 (normal: -1 to 1, with buffer for edge cases)
   - Gamma: max 1000.0 (normal: 0 to ~0.1, with buffer for deep ITM/OTM)
   - Theta: max ±1000.0 (normal: -10 to 0, with buffer for extremes)
   - Vega: max 10000.0 (normal: 0 to ~100, with buffer for high-IV)

3. **Added logging** (lines 315-316)
   - Warns when invalid greeks are sanitized
   - Reports count for data quality monitoring

### Validation Bounds Rationale

The chosen bounds are:
- **Conservative enough** to catch clearly corrupt data (e+96, billions, etc.)
- **Generous enough** to accommodate legitimate edge cases
- **Well within** DECIMAL(18,8) database limits
- **Aligned with** financial theory for option greeks

## Testing

Created comprehensive test suite in `verifications/test_greeks_sanitization.py`:

### Test Results
- **23/23 tests passed**
- All problematic values from error log correctly filtered to NULL
- Valid edge cases correctly preserved
- All sanitization bounds fit within database DECIMAL(18,8) constraints

### Tested Scenarios
1. All 9 specific error values from GitHub Actions log
2. Normal/typical greek values
3. Edge cases at boundary limits
4. Invalid inputs (NULL, NaN, Infinity)
5. Database constraint validation

## Expected Behavior

After this fix:
1. Intraday job will no longer crash on corrupt greek values
2. Invalid greeks will be stored as NULL in the database
3. Valid greeks will be preserved unchanged
4. Warnings will be logged when data is sanitized for monitoring
5. Job will continue processing remaining data instead of failing

## Monitoring

Watch for these log messages to track data quality:
```
WARNING - Sanitized X invalid greek values (set to NULL) out of Y total greeks
```

If sanitization count is consistently high, may indicate:
- Data quality issues at Polygon API
- Need to report issue to Polygon support
- Possible need to adjust bounds (if legitimate values are being filtered)

## Future Considerations

1. **API Data Quality**: If sanitization becomes frequent, consider reporting to Polygon
2. **Analytics Impact**: Queries using greeks should handle NULL values appropriately
3. **Bounds Adjustment**: If legitimate extreme values are filtered, bounds can be increased (within DB limits)
4. **Alternative Storage**: For scientific notation values, could consider storing as TEXT or FLOAT instead

## Files Modified

- `database/core/bulk_operations.py` - Added sanitization logic
- `verifications/test_greeks_sanitization.py` - Test suite (new file)
- `verifications/GREEKS_OVERFLOW_FIX_SUMMARY.md` - This documentation (new file)

## Rollback Plan

If needed, revert changes to `database/core/bulk_operations.py`:
```python
# Replace lines 292-295 with original:
f"{f(greeks.get('delta'))}\t{f(greeks.get('gamma'))}\t{f(greeks.get('theta'))}\t{f(greeks.get('vega'))}\t"
```

However, this will restore the original error condition. Better approach would be to adjust sanitization bounds if they're too restrictive.

