# Daily Option Contracts Update Strategy

## Overview

With the rate limit optimization (20 requests/second), we now have a highly efficient system that can check ALL symbols daily in just ~11 minutes. This simple approach eliminates complexity while ensuring complete coverage.

## Performance Results

### **Daily Full Run**
- **Rate**: 350 contracts/second
- **Time per symbol**: 0.67 seconds average
- **API efficiency**: 234.8 contracts per API call
- **Total time**: ~11 minutes for 1,000 symbols
- **Use case**: Daily maintenance (simple and complete)

## Recommended Daily Run Strategy

### **Smart Incremental Approach (Recommended)**
```bash
# Only process symbols with new contracts in daily trading data
python scripts/populate_option_contracts.py --smart-incremental

# Estimated time: 2-5 minutes for 200-600 symbols with new contracts
# Only processes symbols that actually need updates
```

### **Full Run (Initial Setup or Weekly)**
```bash
# Check all active symbols - for initial population or weekly refresh
python scripts/populate_option_contracts.py

# With symbol limit for testing
python scripts/populate_option_contracts.py --symbol-limit 1000

# Estimated time: ~11 minutes for 1,000 symbols
# Expected contracts: ~300K-500K total
```

## Integration with Daily Schedule

### **Add to `daily_schedule.py`**

```python
# After daily_option_snapshot loading
logger.info("[option_contracts] Step 4/4: updating contract metadata...")
try:
    from scrapers.polygon_option_contracts_scraper import PolygonOptionContractsScraper
    
    contracts_scraper = PolygonOptionContractsScraper()
    
    # Use smart incremental - only process symbols with new contracts
    contracts_results = contracts_scraper.scrape_incremental_smart()
    
    if contracts_results.get('success'):
        logger.info(f"[option_contracts] ✓ {contracts_results['total_contracts']} contracts updated for {contracts_results['successful_symbols']} symbols")
        logger.info(f"[option_contracts] ✓ {contracts_results['api_calls_made']} API calls in {contracts_results['duration']:.1f}s")
    else:
        logger.warning(f"[option_contracts] ⚠ Partial success: {contracts_results['successful_symbols']}/{contracts_results['symbols_processed']} symbols")
        
except Exception as contracts_error:
    logger.error(f"[option_contracts] Failed to update contract metadata: {contracts_error}")
    # Continue with pipeline even if contracts fail
```

## Smart Incremental Approach

### **Only Process Symbols with New Contracts:**

- Compare `daily_option_snapshot` vs existing `option_contracts`
- Only scrape symbols that have contracts in trading data but not in our database
- Skip symbols where all contracts already exist
- Extremely efficient - typically 200-600 symbols vs 11,000+

## Performance Optimizations Implemented

### **1. Rate Limit Optimization**
- **Before**: 5 requests/minute (12 second intervals)
- **After**: 20 requests/second (0.05 second intervals)
- **Result**: 240x faster API calls

### **2. Deduplication**
- Removes duplicate contracts within each API response
- Prevents `ON CONFLICT` database errors
- Ensures clean upsert operations

### **3. Bulk Operations**
- PostgreSQL COPY for efficient data loading
- Temp table strategy with proper cleanup
- ON CONFLICT DO UPDATE for seamless upserts

### **4. Complete Daily Coverage**
- Check all active symbols every day
- Ensure no contracts are missed
- Simple, predictable runtime

## Expected Daily Performance

### **Daily Full Run:**
- **Symbols processed**: 1,000-11,000 (all active symbols)
- **Duration**: 11 minutes for 1,000 symbols
- **API calls**: 1,000-11,000 (1 per symbol)
- **Contracts updated**: 300K-500K total
- **Database operations**: Efficient bulk upserts

## Monitoring and Maintenance

### **Key Metrics to Track:**
- Daily symbols processed vs. total available
- API calls per day vs. rate limits
- Contract update success rate
- Database growth rate
- Performance trends (contracts/second)

### **Alerts to Set:**
- Failed symbol processing > 5%
- Daily runtime > 15 minutes
- API rate limit approaching
- Database disk space < 20%

### **Weekly Review:**
- Check symbols with consistently no contracts
- Review symbols with frequent updates
- Optimize symbol selection criteria
- Monitor API usage patterns

## Conclusion

The optimized system provides:
- **240x faster** than original rate limits
- **Simple daily full coverage** - check all symbols every day
- **Robust error handling** and deduplication
- **Production-ready integration** with existing pipeline
- **Predictable performance** and comprehensive tracking

Daily runs complete in **~11 minutes** while maintaining complete, up-to-date contract metadata for all actively traded options.
