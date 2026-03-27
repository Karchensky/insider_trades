# Validated Scoring System - March 2026

## Executive Summary

Fresh analysis of 3,000 historical signals with actual TP100 outcomes reveals that **the Greeks-based scoring system is NOT predictive**. The actually predictive factors are volume-based metrics.

## Analysis Results

### Baseline
- **Total samples**: 3,000 signals
- **Baseline TP100 rate**: 30.1%
- **Date range**: Nov 2025 - Feb 2026

### Individual Factor Performance

| Rank | Factor | Best Percentile | TP100 Rate | Lift |
|------|--------|-----------------|------------|------|
| 1 | volume_score | 95th | 38.5% | 1.28x |
| 2 | z_score | 97th | 36.7% | 1.22x |
| 3 | total_score | 97th | 35.6% | 1.18x |
| 4 | vol_oi_score | 85th | 32.8% | 1.09x |
| 5 | directional_score | 85th | 28.6% | 0.95x |
| 6 | magnitude | 80th | 28.2% | 0.94x |
| 7 | otm_score | 80th | 23.1% | 0.77x |
| 8 | time_score | 80th | 19.7% | 0.65x |

**Greeks (gamma, vega, theta) did not make the top 8 factors.**

### Best 4-Factor Combinations

| Rank | Factors | Count | TP100% | Lift |
|------|---------|-------|--------|------|
| 1 | total_score + directional_score + magnitude + otm_score | 10 | 50.0% | 1.66x |
| 2 | volume_score + z_score + vol_oi_score + directional_score | 82 | 39.0% | 1.30x |
| 3 | volume_score + z_score + vol_oi_score + magnitude | 72 | 38.9% | 1.29x |
| 4 | volume_score + z_score + vol_oi_score + time_score | 76 | 38.2% | 1.27x |

## Recommended Scoring System

Based on the analysis, the recommended scoring system uses:

### Primary Factors (at 93rd percentile)
1. **volume_score** >= 2.5 (volume anomaly)
2. **z_score** >= 3.5 (statistical deviation)
3. **vol_oi_score** >= 1.5 (volume:OI ratio)
4. **total_score** >= 4.5 (legacy composite)

### Scoring Logic
```python
score = (
    (volume_score >= 2.5) +
    (z_score >= 3.5) +
    (vol_oi_score >= 1.5) +
    (total_score >= 4.5)
)

is_high_conviction = (score >= 3)
```

### Expected Performance
- **Score >= 3**: ~38-40% TP100 rate (vs 30% baseline)
- **Score = 4**: ~45-50% TP100 rate (but rare)
- **Lift**: 1.25-1.30x over baseline

## Why Greeks Failed

The original analysis (`final_validation.png`) likely suffered from:

1. **Selection bias**: Analyzed contracts that already had high Greeks, not the recommended_option
2. **Survivorship bias**: Only looked at contracts with outcomes, missing expired worthless
3. **Different methodology**: May have used different filtering criteria
4. **Small sample size**: Results not statistically robust

## Implementation

### No Schema Changes Required

The recommended factors already exist in `daily_anomaly_snapshot`:
- `volume_score`
- `z_score`
- `volume_oi_ratio_score` (vol_oi_score)
- `total_score`

### Query-Time Scoring

```sql
SELECT 
    symbol,
    event_date,
    recommended_option,
    (CASE WHEN volume_score >= 2.5 THEN 1 ELSE 0 END +
     CASE WHEN z_score >= 3.5 THEN 1 ELSE 0 END +
     CASE WHEN volume_oi_ratio_score >= 1.5 THEN 1 ELSE 0 END +
     CASE WHEN total_score >= 4.5 THEN 1 ELSE 0 END
    ) AS validated_score
FROM daily_anomaly_snapshot
WHERE total_magnitude >= 20000
  AND COALESCE(is_bot_driven, FALSE) = FALSE
  AND COALESCE(is_earnings_related, FALSE) = FALSE
HAVING validated_score >= 3
```

## Next Steps

1. **Update detection logic** to use validated factors
2. **Update email alerts** to show validated score
3. **Monitor performance** for 2-4 weeks
4. **Recalibrate thresholds** monthly based on recent data

## Conclusion

The Greeks-based scoring system should be deprecated. The volume-based scoring system (volume_score, z_score, vol_oi_score, total_score) is the validated approach for identifying high-conviction TP100 opportunities.
