# Insider Trading Anomaly Detection System

## Overview

This system analyzes options trading patterns to identify potential insider trading activity by detecting unusual patterns that deviate significantly from historical baselines. The system processes real-time intraday data from `temp_option` and `temp_stock` tables, comparing it against historical baselines derived from `daily_option_snapshot` and `daily_stock_snapshot` tables.

## Architecture

### Data Sources
- **Current Session Data**: `temp_option` and `temp_stock` tables (intraday data for current trading session)
- **Historical Baseline Data**: `daily_option_snapshot` and `daily_stock_snapshot` tables (30+ days of historical data)
- **Underlying Price Priority**: Always derived from `temp_stock` (day_close, day_vwap) rather than temp_option.underlying_price

### Processing Flow
1. **Baseline Calculation**: Analyze 30 days of historical snapshot data to establish normal trading patterns
2. **Current Data Analysis**: Process today's intraday option contracts with valid underlying prices
3. **Anomaly Detection**: Run multiple detection algorithms on symbol-grouped data
4. **Scoring & Aggregation**: Combine individual anomaly scores into symbol-level composite scores
5. **Storage**: Store symbol-level anomalies in `temp_anomaly` table

## Anomaly Detection Algorithms

### 1. Volume Concentration Detection (`_detect_volume_concentration`)

**Purpose**: Identifies symbols where trading volume is concentrated in a small number of option contracts, suggesting coordinated activity.

**Logic**:
- Groups contracts by symbol
- Calculates total volume and identifies high-volume contracts (>100 volume)
- Computes volume concentration ratio: `(high_volume_contracts_volume / total_volume)`
- Calculates volume factor: `min(3.0, total_volume / 10000)` (caps at 3x for very high volume)

**Scoring Formula**:
```
concentration_score = volume_concentration_ratio * volume_factor * high_volume_contract_count
```

**Threshold**: Score > 2.0

**What It Detects**: 
- Large institutional trades concentrated in specific strikes
- Coordinated buying across limited contracts
- Unusual volume spikes in particular options

**Example**: If AAPL has 1M total volume but 800K is concentrated in just 3 contracts, this indicates potential insider coordination.

### 2. Strike Coordination Detection (`_detect_strike_coordination`)

**Purpose**: Identifies symbols where multiple strike prices are being traded with significant volume simultaneously, indicating strategic positioning.

**Logic**:
- Identifies contracts with volume > 100 across different strikes
- Requires at least 3 different strikes with high volume
- Calculates strike spread (max_strike - min_strike)
- Measures volume consistency across strikes

**Scoring Formula**:
```
volume_consistency = 1.0 - (max_volume - min_volume) / total_volume
coordination_score = (strike_count * volume_consistency * min(5.0, total_volume/1000)) / max(1, strike_spread/10)
```

**Threshold**: Score > 2.5

**What It Detects**:
- Hedging strategies across multiple strikes
- Spread trading indicating directional bets
- Coordinated positioning at strategic price levels

**Example**: Trading high volume in TSLA $200, $210, and $220 calls simultaneously suggests knowledge of expected price movement.

### 3. Directional Bias Detection (`_detect_directional_bias`)

**Purpose**: Identifies symbols showing strong preference for calls vs puts (or vice versa), indicating directional conviction.

**Logic**:
- Separates contracts by type (call vs put)
- Calculates volume and contract count for each type
- Computes directional bias ratios

**Scoring Formula**:
```
call_put_volume_ratio = call_volume / (put_volume + 1)
call_put_count_ratio = call_count / (put_count + 1)
bias_score = max(call_put_volume_ratio, 1/call_put_volume_ratio) * sqrt(total_volume/1000)
```

**Threshold**: Score > 3.0

**What It Detects**:
- Strong bullish sentiment (heavy call buying)
- Strong bearish sentiment (heavy put buying)
- Directional bets suggesting knowledge of upcoming events

**Example**: NVDA showing 10:1 call-to-put volume ratio before earnings suggests bullish insider knowledge.

### 4. Expiration Clustering Detection (`_detect_expiration_clustering`)

**Purpose**: Identifies symbols where trading is concentrated in specific expiration dates, especially short-term expirations.

**Logic**:
- Groups contracts by expiration date
- Identifies "short-term" expirations (≤ 21 days)
- Calculates concentration in short-term vs long-term options
- Specifically tracks out-of-the-money (OTM) call volume

**Scoring Formula**:
```
short_term_ratio = short_term_volume / total_volume
time_pressure_factor = (21 - avg_days_to_expiration) / 21
otm_call_factor = otm_call_volume / total_call_volume
clustering_score = short_term_ratio * time_pressure_factor * volume_factor * (1 + otm_call_factor)
```

**Threshold**: Score > 2.5

**What It Detects**:
- Event-driven trading (earnings, announcements)
- Time-sensitive insider information
- Options about to expire with unusual activity

**Example**: Heavy trading in SPY options expiring in 2 days suggests knowledge of imminent market-moving event.

### 5. Volatility Pattern Detection (`_detect_volatility_patterns`)

**Purpose**: Identifies symbols where current implied volatility significantly deviates from historical patterns, suggesting market anticipation of events.

**Logic**:
- Compares current average IV against historical baseline
- Calculates z-score: `(current_iv - baseline_avg) / baseline_stddev`
- Considers volume-weighted significance

**Scoring Formula**:
```
iv_z_score = (current_avg_iv - baseline_avg_iv) / max(baseline_stddev_iv, 0.01)
volume_weight = min(2.0, total_volume / 5000)
volatility_score = abs(iv_z_score) * volume_weight
```

**Threshold**: Score > 2.0

**What It Detects**:
- Market anticipation of volatility events
- Unusual IV spikes suggesting insider knowledge
- Options being priced for expected price movements

**Example**: AMZN options showing 3 standard deviations higher IV than normal suggests market expects significant news.

### 6. Contract Anomaly Detection (`_detect_contract_anomalies`)

**Purpose**: Identifies symbols with unusually high ratios of high-volume to normal-volume contracts.

**Logic**:
- Categorizes contracts as "high volume" (>100) vs "normal volume"
- Calculates ratio of high-volume contracts to total contracts
- Considers absolute volume levels

**Scoring Formula**:
```
high_volume_ratio = high_volume_contracts / total_contracts
volume_factor = min(3.0, total_volume / 10000)
anomaly_score = high_volume_ratio * volume_factor * high_volume_contracts
```

**Threshold**: Score > 1.5

**What It Detects**:
- Unusual concentration of activity in specific contracts
- Large block trades standing out from normal flow
- Institutional activity vs retail trading patterns

**Example**: GOOGL having 20 high-volume contracts out of 100 total (vs normal 2-3) suggests institutional positioning.

### 7. Out-of-the-Money Call Pattern Detection (`_detect_otm_call_patterns`)

**Purpose**: Specifically targets the classic insider trading pattern of heavy OTM call buying with short expirations.

**Detailed Logic**:
1. **Contract Filtering**: Focuses exclusively on call options where underlying_price < strike_price (out-of-the-money)
2. **Time Constraint**: Filters for expirations ≤ 21 days (3 weeks or less)
3. **Volume Threshold**: Requires minimum 50 total OTM volume and minimum 2 contracts
4. **Moneyness Calculation**: Measures how far out-of-the-money each option is
5. **Pattern Recognition**: Identifies coordinated OTM call buying across multiple strikes

**Scoring Formula**:
```
otm_ratio = otm_call_volume / total_symbol_volume
time_pressure_factor = (21 - avg_days_to_expiration) / 21
volume_factor = min(3.0, otm_call_volume / 100)
moneyness_factor = min(2.0, avg_moneyness / 5.0)
insider_score = otm_ratio * time_pressure_factor * volume_factor * moneyness_factor * contract_count
```

**Threshold**: Score > 2.0

**What It Detects**:
- Classic insider trading patterns (buying cheap, high-leverage options before announcements)
- Event-driven speculation (earnings, FDA approvals, merger announcements)
- High-conviction directional bets with asymmetric risk/reward
- "Lottery ticket" options that pay off big if stock moves significantly

**Real-World Example**: Heavy buying of TSLA $300 calls expiring in 5 days when stock is at $250 suggests knowledge of upcoming positive catalyst like earnings beat or product announcement.

### 8. Greeks-Based Anomaly Detection (`_detect_greeks_anomalies`)

**Purpose**: Utilizes option Greeks (delta, gamma, theta, vega) to identify sophisticated trading strategies that suggest insider knowledge.

**Detailed Logic**:
1. **Delta Concentration**: Identifies heavy volume in high-delta calls (>0.7), suggesting strong directional conviction
2. **Gamma Risk Assessment**: Detects concentration in high-gamma options (>0.1), indicating volatility plays
3. **Theta Decay Analysis**: Finds heavy trading in high-theta options with short expirations (≤14 days), suggesting time-sensitive information
4. **Volume Weighting**: All metrics are volume-weighted to focus on significant positions

**Scoring Formula**:
```
greeks_score = 0
if delta_concentration > 0.6: greeks_score += delta_concentration * 3
if gamma_concentration > 0.4: greeks_score += gamma_concentration * 2  
if theta_concentration > 0.5: greeks_score += theta_concentration * 4
greeks_score *= min(2.0, total_volume / 1000)  # Volume factor
```

**Threshold**: Score > 2.0

**What It Detects**:
- **High Delta Concentration**: Strong directional bets (bullish/bearish conviction)
- **High Gamma Concentration**: Volatility plays expecting large price swings
- **High Theta Risk**: Time-sensitive trades expecting near-term catalysts
- **Sophisticated Strategies**: Professional/institutional trading patterns

**Real-World Example**: Heavy volume in NVDA high-delta calls with high gamma before earnings suggests traders expect significant upward price movement with high volatility.

## Scoring System

### Individual Anomaly Scores
Each detection algorithm produces a score typically ranging from 0-10, with higher scores indicating stronger anomalies.

### Symbol-Level Composite Scoring
```python
# Collect all individual anomaly scores for a symbol
individual_scores = [anomaly['score'] for anomaly in symbol_anomalies]

# Calculate composite score (weighted average with diminishing returns)
composite_score = sum(score * (0.8 ** i) for i, score in enumerate(sorted(individual_scores, reverse=True)))

# Maximum individual score
max_individual_score = max(individual_scores)
```

### Risk Level Classification
- **CRITICAL**: Score ≥ 7.0 (Immediate attention required)
- **HIGH**: Score ≥ 5.0 (Strong anomaly signal)
- **MEDIUM**: Score ≥ 3.0 (Moderate anomaly)
- **LOW**: Score < 3.0 (Minor anomaly)

## Integration & Usage

### Running Detection
```python
from analysis.insider_anomaly_detection import run_insider_anomaly_detection

results = run_insider_anomaly_detection(baseline_days=30)
```

### Querying Results
```sql
SELECT symbol, score, anomaly_types, details 
FROM temp_anomaly 
WHERE event_date = CURRENT_DATE 
ORDER BY score DESC;
```
