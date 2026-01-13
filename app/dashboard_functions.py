#!/usr/bin/env python3
"""
Dashboard support functions for Streamlit app
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from typing import Dict, List, Any
from datetime import date, datetime, timedelta
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db

def get_ordered_anomaly_symbols(anomalies_df: pd.DataFrame) -> List[str]:
    """Get anomaly symbols ordered by date descending, score descending."""
    if anomalies_df.empty:
        return []
    
    # Helper function for safe numeric conversion
    def safe_numeric(value, default=0, as_int=False):
        if value is None or value == '' or str(value).lower() in ['none', 'null']:
            return default
        try:
            if as_int:
                return int(float(value))
            else:
                return float(value)
        except (ValueError, TypeError):
            return default
    
    # Create a list of tuples for sorting: (date, score, symbol)
    symbol_data = []
    for _, row in anomalies_df.iterrows():
        try:
            # Extract date
            date_key = None
            if pd.notna(row.get('event_date')):
                date_key = pd.to_datetime(row['event_date']).date()
            elif pd.notna(row.get('as_of_timestamp')):
                date_key = pd.to_datetime(row['as_of_timestamp']).date()
            
            if date_key:
                total_score = safe_numeric(row.get('total_score', 0))
                symbol = str(row.get('symbol', ''))
                
                symbol_data.append((date_key, -total_score, symbol))  # Negative score for descending order
        except Exception as e:
            continue
    
    # Sort by: date descending, score descending
    symbol_data.sort(key=lambda x: (-x[0].toordinal(), x[1]))
    
    # Extract symbols in order
    return [item[2] for item in symbol_data]

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_current_anomalies() -> pd.DataFrame:
    """Get current high-conviction anomalies from daily_anomaly_snapshot table."""
    conn = db.connect()
    cur = None
    try:
        # Use cursor with RealDictCursor for reliable data access
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # First check if we have any data
        cur.execute("SELECT COUNT(*) as count FROM daily_anomaly_snapshot WHERE total_score >= 7.5 AND total_magnitude >= 20000")
        count = cur.fetchone()['count']
        
        if count == 0:
            return pd.DataFrame()
            
        query = """
            SELECT 
                symbol,
                total_score,
                volume_score,
                volume_oi_ratio_score,
                otm_score,
                directional_score,
                time_score,
                call_volume,
                put_volume,
                total_volume,
                call_baseline_avg,
                put_baseline_avg,
                call_multiplier,
                put_multiplier,
                direction,
                pattern_description,
                z_score,
                otm_call_percentage,
                short_term_percentage,
                call_put_ratio,
                as_of_timestamp,
                event_date,
                open_interest,
                call_magnitude,
                put_magnitude,
                total_magnitude,
                COALESCE(high_conviction_score, 0) as high_conviction_score,
                COALESCE(is_high_conviction, false) as is_high_conviction,
                recommended_option
            FROM daily_anomaly_snapshot
            WHERE total_score >= 7.5 AND total_magnitude >= 20000
            ORDER BY is_high_conviction DESC, total_score DESC, as_of_timestamp DESC
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        # Convert cursor results to DataFrame manually (more reliable than pandas read_sql_query)
        data = []
        for row in rows:
            data.append(dict(row))
        
        df = pd.DataFrame(data)
        return df
        
    except Exception as e:
        st.error(f"Error fetching anomalies: {e}")
        return pd.DataFrame()
    finally:
        if cur:
            cur.close()
        conn.close()

@st.cache_data(ttl=180)  # Cache for 3 minutes
def get_symbol_history(symbol: str, days: int = 30) -> Dict[str, pd.DataFrame]:
    """Get historical data for a specific symbol."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get stock price history
        stock_query = """
            WITH history_daily AS (
            SELECT date, open, high, low, close, trading_volume as volume
            FROM daily_stock_snapshot
            WHERE symbol = %s
              AND date >= CURRENT_DATE - INTERVAL %s
            ORDER BY date ASC )

            , latest_temp_stock AS (
            SELECT DISTINCT ON (symbol) 
                date(as_of_timestamp) as date, day_open as open, day_high as high, day_low as low, day_close as close, day_volume as volume
            FROM temp_stock
            WHERE symbol = %s
            ORDER BY symbol, as_of_timestamp DESC )

            select * from history_daily
            UNION ALL
            select * from latest_temp_stock
        """

        cur.execute(stock_query, (symbol, f'{days} days', symbol))
        stock_rows = cur.fetchall()
        stock_df = pd.DataFrame([dict(row) for row in stock_rows]) if stock_rows else pd.DataFrame()
        
        # Get options activity history
        options_query = """
            WITH history_daily AS ( 
            SELECT 
                dos.date,
                oc.contract_type,
                SUM(dos.volume) as total_volume,
                COUNT(*) as contract_count,
                AVG(dos.implied_volatility) as avg_iv
            FROM daily_option_snapshot dos
            INNER JOIN option_contracts oc on dos.contract_ticker = oc.contract_ticker
            WHERE oc.symbol = %s
              AND dos.date >= CURRENT_DATE - INTERVAL %s
            GROUP BY dos.date, oc.contract_type
            ORDER BY dos.date ASC, oc.contract_type )

            , latest_temp_option AS (
                -- Get the most recent option data for each contract
                SELECT DISTINCT ON (symbol, contract_ticker) 
                    date(as_of_timestamp) as date, symbol, contract_ticker, session_volume, contract_type, implied_volatility
                FROM temp_option
                WHERE symbol = %s
                ORDER BY symbol, contract_ticker, as_of_timestamp DESC  )

            select * from history_daily
            UNION ALL
          select date,
                contract_type,
                SUM(session_volume) as total_volume,
                COUNT(*) as contract_count,
                AVG(implied_volatility) as avg_iv
          from latest_temp_option
          group by date, contract_type
        """
        cur.execute(options_query, (symbol, f'{days} days', symbol))
        options_rows = cur.fetchall()
        options_df = pd.DataFrame([dict(row) for row in options_rows]) if options_rows else pd.DataFrame()
        
        # Get today's intraday activity
        intraday_query = """
            SELECT DISTINCT ON (o.symbol, o.contract_ticker)  
                o.contract_ticker,
                oc.contract_type,
                oc.strike_price,
                oc.expiration_date,
                o.open_interest,
                o.session_volume,
                o.session_close,
                o.implied_volatility,
                o.greeks_delta,
                oc.shares_per_contract,
                COALESCE(s.day_close, s.day_vwap) as underlying_price,
                o.as_of_timestamp
            FROM temp_option o
            INNER JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
            LEFT JOIN temp_stock s ON o.symbol = s.symbol
            WHERE o.symbol = %s
              AND o.session_volume > 0
            ORDER BY o.symbol, o.contract_ticker, o.as_of_timestamp DESC, o.session_volume DESC
        """
        cur.execute(intraday_query, (symbol,))
        intraday_rows = cur.fetchall()
        intraday_df = pd.DataFrame([dict(row) for row in intraday_rows]) if intraday_rows else pd.DataFrame()
        
        return {
            'stock': stock_df,
            'options': options_df,
            'intraday': intraday_df
        }
    except Exception as e:
        st.error(f"Error fetching symbol history: {e}")
        return {'stock': pd.DataFrame(), 'options': pd.DataFrame(), 'intraday': pd.DataFrame()}
    finally:
        conn.close()

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_anomaly_timeline(days: int = 7) -> pd.DataFrame:
    """Get timeline of anomalies over the past N days."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            SELECT 
                event_date,
                COUNT(*) as anomaly_count,
                AVG(total_score) as avg_score,
                MAX(total_score) as max_score,
                ARRAY_AGG(symbol ORDER BY total_score DESC) as symbols
            FROM daily_anomaly_snapshot
            WHERE event_date >= CURRENT_DATE - INTERVAL %s
              AND total_score >= 7.5 AND total_magnitude >= 20000
            GROUP BY event_date
            ORDER BY event_date DESC
        """
        
        cur.execute(query, (f'{days} days',))
        rows = cur.fetchall()
        
        if not rows:
            return pd.DataFrame()
        
        # Convert cursor results to DataFrame
        data = []
        for row in rows:
            data.append(dict(row))
        
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Error fetching anomaly timeline: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def create_anomaly_summary_table(anomalies_df: pd.DataFrame) -> None:
    """Create the main anomaly summary table."""
    if anomalies_df.empty:
        st.info("No high-conviction anomalies detected in the past 7 days.")
        return
    
    st.subheader("High-Conviction Insider Trading Alerts")
    
    # Sort anomalies by score descending (all anomalies now meet magnitude threshold)
    sorted_anomalies_df = anomalies_df.sort_values('total_score', ascending=False)
    
    # Helper function for safe numeric conversion
    def safe_numeric(value, default=0, as_int=False):
        if value is None or value == '' or str(value).lower() in ['none', 'null']:
            return default
        try:
            if as_int:
                return int(float(value))
            else:
                return float(value)
        except (ValueError, TypeError):
            return default
    
    # Process high conviction anomalies
    if not sorted_anomalies_df.empty:
        st.write("**High Conviction Anomalies (Score ≥ 7.5, Magnitude ≥ $20K)**")
        display_data = []
        for _, row in sorted_anomalies_df.iterrows():
            try:
                call_volume = safe_numeric(row.get('call_volume', 0), as_int=True)
                put_volume = safe_numeric(row.get('put_volume', 0), as_int=True)
                total_volume = safe_numeric(row.get('total_volume', call_volume + put_volume), as_int=True)
                call_baseline = safe_numeric(row.get('call_baseline_avg', 1), default=1)
                put_baseline = safe_numeric(row.get('put_baseline_avg', 1), default=1)
                call_multiplier = safe_numeric(row.get('call_multiplier', 0))
                put_multiplier = safe_numeric(row.get('put_multiplier', 0))
                
                # Calculate indicators
                call_percentage = (call_volume / total_volume * 100) if total_volume > 0 else 0
                otm_score = safe_numeric(row.get('otm_score', 0))
                
                # Determine insider pattern and appropriate multiplier
                if call_percentage >= 80:
                    pattern = "Bullish insider activity"
                    volume_text = f"{call_multiplier:.1f}x normal call volume"
                elif call_percentage <= 20:
                    pattern = "Strong bearish insider activity"
                    volume_text = f"{put_multiplier:.1f}x normal put volume"
                else:
                    pattern = "Mixed directional positioning"
                    volume_text = f"{call_multiplier:.1f}x call, {put_multiplier:.1f}x put"
                
                # Format key indicators using new data structure
                z_score = safe_numeric(row.get('z_score', 0))
                total_score = safe_numeric(row.get('total_score', 0))
                volume_oi_ratio_score = safe_numeric(row.get('volume_oi_ratio_score', 0))
                short_term_percentage = safe_numeric(row.get('short_term_percentage', 0))
                
                # Get additional data for detailed indicators
                call_open_interest = safe_numeric(row.get('call_open_interest', 0))
                put_open_interest = safe_numeric(row.get('put_open_interest', 0))
                volume_score = safe_numeric(row.get('volume_score', 0))
                directional_score = safe_numeric(row.get('directional_score', 0))
                time_score = safe_numeric(row.get('time_score', 0))
                
                # Calculate magnitude data
                call_magnitude = safe_numeric(row.get('call_magnitude', 0))
                put_magnitude = safe_numeric(row.get('put_magnitude', 0))
                total_magnitude = call_magnitude + put_magnitude
                
                key_indicators = f"""• Volume Score: {volume_score:.1f}/3.0 ({volume_text})
• Volume:OI Score: {volume_oi_ratio_score:.1f}/2.0 (Call: {call_volume:,} vs {call_open_interest:,} OI)
• OTM Score: {otm_score:.1f}/2.0 (Out-of-money concentration)
• Direction Score: {directional_score:.1f}/1.0 ({call_percentage:.0f}% calls vs {100-call_percentage:.0f}% puts)
• Time Score: {time_score:.1f}/2.0 (Near-term expiration focus)
• Magnitude: ${total_magnitude:,.0f} total (Call: ${call_magnitude:,.0f}, Put: ${put_magnitude:,.0f})"""
                
                # Handle timestamp safely
                timestamp_str = 'N/A'
                if pd.notna(row.get('as_of_timestamp')):
                    try:
                        timestamp_str = row['as_of_timestamp'].strftime('%H:%M:%S')
                    except (AttributeError, ValueError):
                        timestamp_str = str(row['as_of_timestamp'])
                
                # Get magnitude data
                call_magnitude = safe_numeric(row.get('call_magnitude', 0))
                put_magnitude = safe_numeric(row.get('put_magnitude', 0))
                total_magnitude = call_magnitude + put_magnitude
                
                display_data.append({
                    'Symbol': str(row.get('symbol', 'Unknown')),
                    'Score': f"{total_score:.1f}/10",
                    'Volume': f"{total_volume:,}",
                    'Magnitude': f"${total_magnitude:,.0f}",
                    'Open Interest': f"{int(safe_numeric(row.get('open_interest', 0))):,}",
                    'Key Indicators': key_indicators,
                    'Insider Pattern': pattern,
                    'Timestamp': timestamp_str
                })
                
            except Exception as e:
                st.warning(f"Error processing row for symbol {row.get('symbol', 'Unknown')}: {e}")
                continue
        
        # Create DataFrame for display
        display_df = pd.DataFrame(display_data)
        
        # Style the table
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
    
    # Process low volume anomalies

def create_anomaly_summary_by_date(anomalies_df: pd.DataFrame) -> None:
    """Create anomaly summary tables grouped by date, ordered by date descending."""
    if anomalies_df.empty:
        st.info("No high-conviction anomalies detected in the past 7 days.")
        return
    
    st.subheader("High-Conviction Insider Trading Alerts")
    
    # Helper function for safe numeric conversion
    def safe_numeric(value, default=0, as_int=False):
        if value is None or value == '' or str(value).lower() in ['none', 'null']:
            return default
        try:
            if as_int:
                return int(float(value))
            else:
                return float(value)
        except (ValueError, TypeError):
            return default
    
    # Group anomalies by date
    anomalies_by_date = {}
    for _, row in anomalies_df.iterrows():
        try:
            # Extract date from event_date or as_of_timestamp
            date_key = None
            if pd.notna(row.get('event_date')):
                date_key = pd.to_datetime(row['event_date']).date()
            elif pd.notna(row.get('as_of_timestamp')):
                date_key = pd.to_datetime(row['as_of_timestamp']).date()
            
            if date_key:
                if date_key not in anomalies_by_date:
                    anomalies_by_date[date_key] = []
                anomalies_by_date[date_key].append(row)
        except Exception as e:
            st.warning(f"Error processing date for symbol {row.get('symbol', 'Unknown')}: {e}")
            continue
    
    if not anomalies_by_date:
        st.info("No valid date information found in anomaly data.")
        return
    
    # Sort dates in descending order (most recent first)
    sorted_dates = sorted(anomalies_by_date.keys(), reverse=True)
    
    # Create a table for each date
    for date in sorted_dates:
        date_anomalies = anomalies_by_date[date]
        
        # Create subheader for the date
        st.subheader(f"Anomalies for {date.strftime('%Y-%m-%d')}")
        
        # Sort anomalies by score descending (all anomalies now meet magnitude threshold)
        date_anomalies.sort(key=lambda x: safe_numeric(x.get('total_score', 0)), reverse=True)
        
        # Process high conviction anomalies
        if date_anomalies:
            st.write("**High Conviction Anomalies (Score ≥ 7.5, Magnitude ≥ $20K)**")
            display_data = []
            for row in date_anomalies:
                try:
                    call_volume = safe_numeric(row.get('call_volume', 0), as_int=True)
                    put_volume = safe_numeric(row.get('put_volume', 0), as_int=True)
                    total_volume = safe_numeric(row.get('total_volume', call_volume + put_volume), as_int=True)
                    call_baseline = safe_numeric(row.get('call_baseline_avg', 1), default=1)
                    put_baseline = safe_numeric(row.get('put_baseline_avg', 1), default=1)
                    call_multiplier = safe_numeric(row.get('call_multiplier', 0))
                    put_multiplier = safe_numeric(row.get('put_multiplier', 0))
                    
                    # Calculate indicators
                    call_percentage = (call_volume / total_volume * 100) if total_volume > 0 else 0
                    otm_score = safe_numeric(row.get('otm_score', 0))
                    
                    # Determine insider pattern and appropriate multiplier
                    if call_percentage >= 80:
                        pattern = "Strong bullish insider activity"
                        volume_text = f"{call_multiplier:.1f}x normal call volume"
                    elif call_percentage <= 20:
                        pattern = "Strong bearish insider activity"
                        volume_text = f"{put_multiplier:.1f}x normal put volume"
                    else:
                        pattern = "Mixed directional positioning"
                        volume_text = f"{call_multiplier:.1f}x call, {put_multiplier:.1f}x put"
                    
                    # Format key indicators using new data structure
                    z_score = safe_numeric(row.get('z_score', 0))
                    total_score = safe_numeric(row.get('total_score', 0))
                    volume_oi_ratio_score = safe_numeric(row.get('volume_oi_ratio_score', 0))
                    short_term_percentage = safe_numeric(row.get('short_term_percentage', 0))
                    
                    # Get additional data for detailed indicators
                    call_open_interest = safe_numeric(row.get('call_open_interest', 0))
                    put_open_interest = safe_numeric(row.get('put_open_interest', 0))
                    volume_score = safe_numeric(row.get('volume_score', 0))
                    directional_score = safe_numeric(row.get('directional_score', 0))
                    time_score = safe_numeric(row.get('time_score', 0))
                    
                    # Calculate magnitude data
                    call_magnitude = safe_numeric(row.get('call_magnitude', 0))
                    put_magnitude = safe_numeric(row.get('put_magnitude', 0))
                    total_magnitude = call_magnitude + put_magnitude
                    
                    key_indicators = f"""• Volume Score: {volume_score:.1f}/3.0 ({volume_text})
• Volume:OI Score: {volume_oi_ratio_score:.1f}/2.0 (Call: {call_volume:,} vs {call_open_interest:,} OI)
• OTM Score: {otm_score:.1f}/2.0 (Out-of-money concentration)
• Direction Score: {directional_score:.1f}/1.0 ({call_percentage:.0f}% calls vs {100-call_percentage:.0f}% puts)
• Time Score: {time_score:.1f}/2.0 (Near-term expiration focus)
• Magnitude: ${total_magnitude:,.0f} total (Call: ${call_magnitude:,.0f}, Put: ${put_magnitude:,.0f})"""
                    
                    # Handle timestamp safely
                    timestamp_str = 'N/A'
                    if pd.notna(row.get('as_of_timestamp')):
                        try:
                            timestamp_str = row['as_of_timestamp'].strftime('%H:%M:%S')
                        except (AttributeError, ValueError):
                            timestamp_str = str(row['as_of_timestamp'])
                    
                    # Get magnitude data
                    call_magnitude = safe_numeric(row.get('call_magnitude', 0))
                    put_magnitude = safe_numeric(row.get('put_magnitude', 0))
                    total_magnitude = call_magnitude + put_magnitude
                    
                    display_data.append({
                        'Symbol': str(row.get('symbol', 'Unknown')),
                        'Score': f"{total_score:.1f}/10",
                        'Volume': f"{total_volume:,}",
                        'Magnitude': f"${total_magnitude:,.0f}",
                        'Open Interest': f"{int(safe_numeric(row.get('open_interest', 0))):,}",
                        'Key Indicators': key_indicators,
                        'Timestamp': timestamp_str
                    })
                    
                except Exception as e:
                    st.warning(f"Error processing row for symbol {row.get('symbol', 'Unknown')}: {e}")
                    continue
            
            # Create DataFrame for display
            display_df = pd.DataFrame(display_data)
            
            # Style the table with column configuration
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Key Indicators": st.column_config.TextColumn(
                        "Key Indicators",
                        width="large",
                        help="Detailed trading indicators and metrics"
                    ),
                    "Symbol": st.column_config.TextColumn(
                        "Symbol",
                        width="small"
                    ),
                    "Score": st.column_config.TextColumn(
                        "Score",
                        width="small"
                    ),
                    "Volume": st.column_config.TextColumn(
                        "Volume",
                        width="small"
                    ),
                    "Open Interest": st.column_config.TextColumn(
                        "Open Interest",
                        width="small"
                    ),
                    "Insider Pattern": st.column_config.TextColumn(
                        "Insider Pattern",
                        width="small"
                    ),
                    "Timestamp": st.column_config.TextColumn(
                        "Timestamp",
                        width="small"
                    )
                }
            )
        
        # Process low volume anomalies
        
        # Add some spacing between date groups
        st.markdown("---")

def create_symbol_analysis(symbol: str, anomaly_data: Dict, selected_date: date = None) -> None:
    """Create detailed analysis for a specific symbol."""
    st.header(f"Deep Dive Analysis: {symbol}")
    
    # Get historical data
    history = get_symbol_history(symbol)
    
    if history['stock'].empty:
        st.warning(f"No historical data available for {symbol}")
        return
    
    # Score breakdown
    details = anomaly_data.get('details', {})
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Volume Score",
            f"{details.get('volume_score', 0):.1f}/3.0",
            help="Statistical Z-score analysis vs 30-day baseline"
        )
    
    with col2:
        st.metric(
            "Volume:OI Ratio Score", 
            f"{details.get('volume_oi_ratio_score', 0):.1f}/2.0",
            help="Volume to open interest ratio anomaly vs historical baseline"
        )
    
    with col3:
        st.metric(
            "OTM Call Score", 
            f"{details.get('otm_score', 0):.1f}/2.0",
            help="Out-of-money call concentration (classic insider pattern)"
        )
    
    with col4:
        st.metric(
            "Directional Score",
            f"{details.get('directional_score', 0):.1f}/1.0", 
            help="Strong call/put preference indicating conviction"
        )
    
    with col5:
        st.metric(
            "Time Pressure Score",
            f"{details.get('time_score', 0):.1f}/2.0",
            help="Near-term expiration clustering"
        )
    
    # Trading activity metrics
    st.subheader("Current Trading Activity")
    
    call_volume = details.get('call_volume', 0)
    put_volume = details.get('put_volume', 0)
    call_baseline = details.get('call_baseline_avg', 1)
    put_baseline = details.get('put_baseline_avg', 1)
    current_open_interest = details.get('current_open_interest', 0)
    open_interest_multiplier = details.get('open_interest_multiplier', 0)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Call Volume",
            f"{call_volume:,}",
            delta=f"{((call_volume/call_baseline - 1) * 100) if call_baseline > 0 else 0:.0f}% vs baseline"
        )
    
    with col2:
        st.metric(
            "Put Volume", 
            f"{put_volume:,}",
            delta=f"{((put_volume/put_baseline - 1) * 100) if put_baseline > 0 else 0:.0f}% vs baseline"
        )
    
    with col3:
        call_ratio = call_volume / (call_volume + put_volume) if (call_volume + put_volume) > 0 else 0
        st.metric(
            "Call/Put Ratio",
            f"{call_ratio:.1%}",
            help="Percentage of total volume in calls vs puts"
        )
    
    with col4:
        st.metric(
            "Total Volume",
            f"{call_volume + put_volume:,}",
            help="Total option contracts traded today"
        )
    
    with col5:
        st.metric(
            "Open Interest",
            f"{current_open_interest:,}",
            delta=f"{open_interest_multiplier:.1f}x vs prior day",
            help=f"Current open interest: {current_open_interest:,}"
        )
    
    # Charts
    create_combined_price_volume_chart(history['stock'], symbol)
    create_options_activity_chart(history['options'], symbol)

def create_price_chart(stock_df: pd.DataFrame, symbol: str) -> None:
    """Create stock price chart with volume."""
    if stock_df.empty:
        return
    
    st.subheader(f"{symbol} Stock Price & Volume (30 Days)")
    
    # Create subplot with secondary y-axis
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.7, 0.3],
        subplot_titles=[f'{symbol} Price', 'Volume']
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=stock_df['date'],
            open=stock_df['open'],
            high=stock_df['high'],
            low=stock_df['low'],
            close=stock_df['close'],
            name='Price'
        ),
        row=1, col=1
    )
    
    # Volume bars
    fig.add_trace(
        go.Bar(
            x=stock_df['date'],
            y=stock_df['volume'],
            name='Volume',
            marker_color='rgba(158,202,225,0.8)'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=500,
        showlegend=False,
        xaxis_rangeslider_visible=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_options_activity_chart(options_df: pd.DataFrame, symbol: str) -> None:
    """Create options activity timeline chart."""
    if options_df.empty:
        return
    
    st.subheader(f"{symbol} Options Activity Timeline")
    
    # Validate data before pivoting
    required_columns = ['date', 'contract_type', 'total_volume']
    if not all(col in options_df.columns for col in required_columns):
        st.error(f"Missing required columns for chart. Available: {list(options_df.columns)}")
        return
    
    # Check for string data that looks like column headers
    if not options_df.empty:
        sample_volume = str(options_df.iloc[0]['total_volume'])
        if 'total_volume' in sample_volume.lower():
            st.error("Data contains column headers instead of actual values. Chart cannot be displayed.")
            return
    
    try:
        # Pivot the data for easier plotting
        pivot_df = options_df.pivot_table(
            index='date', 
            columns='contract_type', 
            values='total_volume', 
            fill_value=0,
            aggfunc='sum'  # Explicit aggregation function
        ).reset_index()
    except Exception as e:
        st.error(f"Error creating pivot table: {e}")
        return
    
    fig = go.Figure()
    
    if 'call' in pivot_df.columns:
        fig.add_trace(go.Scatter(
            x=pivot_df['date'],
            y=pivot_df['call'],
            mode='lines+markers',
            name='Call Volume',
            line=dict(color='green', width=2)
        ))
    
    if 'put' in pivot_df.columns:
        fig.add_trace(go.Scatter(
            x=pivot_df['date'],
            y=pivot_df['put'],
            mode='lines+markers',
            name='Put Volume',
            line=dict(color='red', width=2)
        ))
    
    fig.update_layout(
        title=f'{symbol} Daily Options Volume',
        xaxis_title='Date',
        yaxis_title='Volume',
        height=400,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)

def get_performance_matrix_data() -> pd.DataFrame:
    """Get performance matrix data showing price movements after anomalies."""
    conn = db.connect()
    cur = None
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = """
            WITH base AS (
                SELECT DISTINCT ON (a.symbol) 
                    a.symbol, 
                    a.total_score, 
                    a.event_date, 
                    a.total_volume, 
                    b.weighted_average_price as weighted_average_price, 
                    a.direction
                FROM daily_anomaly_snapshot a
                INNER JOIN daily_stock_snapshot b
                    ON a.symbol = b.symbol
                    AND a.event_date = b.date
                WHERE a.total_score >= 7.5 
                AND a.total_magnitude >= 20000
                ORDER BY a.symbol, a.event_date
            )
            SELECT 
                a.symbol,
                a.total_score,
                a.direction,
                b.date - a.event_date as day_number,
                b.close as close_price,
                a.weighted_average_price as starting_price
            FROM base a
            LEFT JOIN daily_stock_snapshot b 
                ON a.symbol = b.symbol
                AND b.date >= a.event_date
                AND b.date <= a.event_date + INTERVAL '30 days'
            WHERE b.date IS NOT NULL
            ORDER BY a.symbol, day_number
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        df = pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
        return df
        
    except Exception as e:
        st.error(f"Error fetching performance matrix data: {e}")
        return pd.DataFrame()
    finally:
        if cur:
            cur.close()
        conn.close()

def create_performance_matrix() -> None:
    """Create performance matrix showing price movements after anomalies."""
    st.subheader("Performance Matrix")
    st.write("Price movements after anomaly detection (30 days)")
    
    # Get performance data
    perf_df = get_performance_matrix_data()
    
    if perf_df.empty:
        st.info("No performance data available")
        return
    
    # Calculate price movement percentage
    perf_df['price_movement'] = ((perf_df['close_price'] - perf_df['starting_price']) / perf_df['starting_price']) * 100
    
    # Create pivot table
    pivot_df = perf_df.pivot_table(
        index=['symbol', 'direction'],
        columns='day_number',
        values='price_movement',
        aggfunc='first'
    ).fillna(0)
    
    # Round scores to nearest 0.5 for grouping
    score_mapping = {}
    for symbol in perf_df['symbol'].unique():
        symbol_data = perf_df[perf_df['symbol'] == symbol]
        if not symbol_data.empty:
            score = symbol_data['total_score'].iloc[0]
            score_mapping[symbol] = round(score * 2) / 2
    
    pivot_df['score_group'] = pivot_df.index.get_level_values(0).map(score_mapping)
    
    # Reset index to make it easier to work with
    pivot_df = pivot_df.reset_index()
    
    # Create score groups for filtering
    score_mapping = {}
    for symbol in perf_df['symbol'].unique():
        symbol_data = perf_df[perf_df['symbol'] == symbol]
        if not symbol_data.empty:
            score = symbol_data['total_score'].iloc[0]
            score_mapping[symbol] = round(score * 2) / 2
    
    perf_df['score_group'] = perf_df['symbol'].map(score_mapping)
    
    # Always use symbol as row dimension
    selected_row = 'symbol'
    
    # Always show all filters
    col1, col2 = st.columns(2)
    
    with col1:
        # Symbol filter
        available_symbols = sorted(perf_df['symbol'].unique())
        selected_symbols = st.multiselect("Filter by Symbol", available_symbols, default=available_symbols)
        
        # Direction filter
        available_directions = sorted(perf_df['direction'].unique())
        selected_directions = st.multiselect("Filter by Direction", available_directions, default=available_directions)
    
    with col2:
        # Score Group filter
        available_score_groups = sorted(perf_df['score_group'].unique())
        selected_score_groups = st.multiselect("Filter by Score Group", available_score_groups, default=available_score_groups)
    
    # Apply all filters
    perf_df = perf_df[
        (perf_df['symbol'].isin(selected_symbols)) &
        (perf_df['direction'].isin(selected_directions)) &
        (perf_df['score_group'].isin(selected_score_groups))
    ]
    
    # Create final pivot table (always use symbol as index)
    final_pivot = perf_df.pivot_table(
        index='symbol',
        columns='day_number',
        values='price_movement',
        aggfunc='first'
    ).fillna(0)
    
    # Format column names
    final_pivot.columns = [f"Day {int(col)}" for col in final_pivot.columns]
    
    # Clean the data first - replace extreme values with 0
    final_pivot_clean = final_pivot.copy()
    final_pivot_clean = final_pivot_clean.replace([np.inf, -np.inf], 0)
    final_pivot_clean = final_pivot_clean.where(abs(final_pivot_clean) < 1e6, 0)
    
    # Format the display to show percentages
    styled_df = final_pivot_clean.style.format(lambda x: f"{x:.2f}%" if pd.notna(x) else "0.00%")
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        height=min(600, 200 + len(final_pivot) * 30)
    )
    
    # Add time series chart
    create_performance_timeseries_chart(perf_df, 'symbol')

def create_performance_timeseries_chart(perf_df: pd.DataFrame, selected_row: str) -> None:
    """Create time series chart showing performance over time."""
    if perf_df.empty:
        return
    
    st.subheader("Performance Time Series")
    
    # Prepare data for plotting
    if selected_row == 'symbol':
        # Group by symbol and day_number
        plot_data = perf_df.groupby(['symbol', 'day_number'])['price_movement'].mean().reset_index()
        
        fig = go.Figure()
        for symbol in plot_data['symbol'].unique():
            symbol_data = plot_data[plot_data['symbol'] == symbol]
            fig.add_trace(go.Scatter(
                x=symbol_data['day_number'],
                y=symbol_data['price_movement'],
                mode='lines+markers',
                name=symbol,
                line=dict(width=2),
                hovertemplate='%{fullData.name}: %{y:.2f}%<extra></extra>'
            ))
    elif selected_row == 'direction':
        # Group by direction and day_number
        plot_data = perf_df.groupby(['direction', 'day_number'])['price_movement'].mean().reset_index()
        
        fig = go.Figure()
        for direction in plot_data['direction'].unique():
            direction_data = plot_data[plot_data['direction'] == direction]
            fig.add_trace(go.Scatter(
                x=direction_data['day_number'],
                y=direction_data['price_movement'],
                mode='lines+markers',
                name=direction,
                line=dict(width=3),
                hovertemplate='%{fullData.name}: %{y:.2f}%<extra></extra>'
            ))
    else:  # score_group
        # Group by score_group and day_number
        score_mapping = {}
        for symbol in perf_df['symbol'].unique():
            symbol_data = perf_df[perf_df['symbol'] == symbol]
            if not symbol_data.empty:
                score = symbol_data['total_score'].iloc[0]
                score_mapping[symbol] = round(score * 2) / 2
        
        perf_df['score_group'] = perf_df['symbol'].map(score_mapping)
        plot_data = perf_df.groupby(['score_group', 'day_number'])['price_movement'].mean().reset_index()
        
        fig = go.Figure()
        for score_group in sorted(plot_data['score_group'].unique()):
            score_data = plot_data[plot_data['score_group'] == score_group]
            fig.add_trace(go.Scatter(
                x=score_data['day_number'],
                y=score_data['price_movement'],
                mode='lines+markers',
                name=f"Score {score_group}",
                line=dict(width=3),
                hovertemplate='%{fullData.name}: %{y:.2f}%<extra></extra>'
            ))
    
    # Update layout
    fig.update_layout(
        title=f"Performance Over Time - {selected_row.title()}",
        xaxis_title="Days After Anomaly",
        yaxis_title="Price Movement (%)",
        height=500,
        hovermode='x unified',
        showlegend=True
    )
    
    # Add horizontal line at 0%
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    
    st.plotly_chart(fig, use_container_width=True)

def create_anomaly_timeline_chart(timeline_df: pd.DataFrame) -> None:
    """Create anomaly detection timeline chart."""
    if timeline_df.empty:
        return
    
    st.subheader("Anomaly Detection Timeline")
    
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=['Daily Anomaly Count', 'Average Score']
    )
    
    # Anomaly count
    fig.add_trace(
        go.Bar(
            x=timeline_df['event_date'],
            y=timeline_df['anomaly_count'],
            name='Anomaly Count',
            marker_color='rgba(255, 99, 71, 0.8)'
        ),
        row=1, col=1
    )
    
    # Average score
    fig.add_trace(
        go.Scatter(
            x=timeline_df['event_date'],
            y=timeline_df['avg_score'],
            mode='lines+markers',
            name='Avg Score',
            line=dict(color='blue', width=2)
        ),
        row=2, col=1
    )
    
    fig.update_layout(height=400, showlegend=False)
    fig.update_yaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(title_text="Score", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)

@st.cache_data(ttl=120)  # Cache for 2 minutes
def get_options_heatmap_data(symbol: str, target_date: date = None) -> pd.DataFrame:
    """Get options data for heatmap visualization by strike and expiration."""
    conn = db.connect()
    cur = None
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if target_date is None:
            target_date = date.today()
        
        # Get options data for the specified date
        # First try daily_option_snapshot for historical data
        query = """
            SELECT 
                oc.strike_price,
                oc.expiration_date,
                oc.contract_type,
                dos.volume,
                dos.open_interest,
                dos.close_price as option_price,
                dos.implied_volatility,
                s.close as underlying_price
            FROM daily_option_snapshot dos
            INNER JOIN option_contracts oc ON dos.contract_ticker = oc.contract_ticker
            LEFT JOIN daily_stock_snapshot s ON dos.symbol = s.symbol AND dos.date = s.date
            WHERE dos.symbol = %s
              AND dos.date = %s
              AND dos.volume > 0
            ORDER BY oc.expiration_date, oc.strike_price
        """
        
        cur.execute(query, (symbol, target_date))
        rows = cur.fetchall()
        
        # If no historical data found, try temp tables for today's data
        if not rows and target_date == date.today():
            query = """
                WITH latest_temp_option AS (
                    SELECT DISTINCT ON (symbol, contract_ticker)
                        symbol, contract_ticker, session_volume, open_interest, 
                        session_close, implied_volatility, as_of_timestamp
                    FROM temp_option
                    WHERE symbol = %s AND session_volume > 0
                    ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                ),
                latest_temp_stock AS (
                    SELECT DISTINCT ON (symbol)
                        symbol, day_close, day_vwap
                    FROM temp_stock
                    WHERE symbol = %s
                    ORDER BY symbol, as_of_timestamp DESC
                )
                SELECT 
                    oc.strike_price,
                    oc.expiration_date,
                    oc.contract_type,
                    o.session_volume as volume,
                    o.open_interest,
                    o.session_close as option_price,
                    o.implied_volatility,
                    COALESCE(s.day_close, s.day_vwap) as underlying_price
                FROM latest_temp_option o
                INNER JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
                LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                ORDER BY oc.expiration_date, oc.strike_price
            """
            cur.execute(query, (symbol, symbol))
            rows = cur.fetchall()
        
        df = pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
        return df
        
    except Exception as e:
        st.error(f"Error fetching heatmap data: {e}")
        return pd.DataFrame()
    finally:
        if cur:
            cur.close()
        conn.close()

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_available_symbols() -> List[str]:
    """Get list of all available symbols for search."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get symbols from multiple tables
        query = """
            SELECT DISTINCT symbol FROM (
                SELECT symbol FROM daily_stock_snapshot
            ) symbols
            ORDER BY symbol
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        return [row['symbol'] for row in rows]
        
    except Exception as e:
        st.error(f"Error fetching available symbols: {e}")
        return []
    finally:
        conn.close()

def create_options_heatmaps(symbol: str, target_date: date = None) -> None:
    """Create 2x2 grid of options heatmaps: call/put volume and open interest by strike/expiration."""
    st.subheader(f"Options Heatmaps for {symbol}")
    
    if target_date:
        st.write(f"Data for: {target_date}")
    
    # Get options data
    options_data = get_options_heatmap_data(symbol, target_date)
    
    if options_data.empty:
        st.info(f"No options data available for {symbol}" + (f" on {target_date}" if target_date else ""))
        return
    
    # Get underlying price for reference line
    underlying_price = float(options_data['underlying_price'].iloc[0]) if not options_data.empty and options_data['underlying_price'].iloc[0] is not None else None
    
    # Create 2x2 subplot grid
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("Call Volume", "Put Volume", "Call Open Interest", "Put Open Interest"),
        specs=[[{"type": "heatmap"}, {"type": "heatmap"}],
               [{"type": "heatmap"}, {"type": "heatmap"}]]
    )
    
    # Separate call and put data
    calls_data = options_data[options_data['contract_type'] == 'call']
    puts_data = options_data[options_data['contract_type'] == 'put']
    
    # Create heatmaps for each quadrant
    heatmaps = [
        (calls_data, 'volume', 'Call Volume', 1, 1),
        (puts_data, 'volume', 'Put Volume', 1, 2),
        (calls_data, 'open_interest', 'Call Open Interest', 2, 1),
        (puts_data, 'open_interest', 'Put Open Interest', 2, 2)
    ]
    
    for data, value_col, title, row, col in heatmaps:
        if not data.empty and value_col in data.columns:
            # Create pivot table for heatmap
            pivot_data = data.pivot_table(
                index='expiration_date',
                columns='strike_price', 
                values=value_col,
                fill_value=0,
                aggfunc='sum'
            )
            
            if not pivot_data.empty:
                # Create heatmap
                fig.add_trace(
                    go.Heatmap(
                        z=pivot_data.values,
                        x=[str(x) for x in pivot_data.columns],
                        y=[str(y) for y in pivot_data.index],
                        colorscale='bupu',
                        showscale=(col == 2),  # Only show colorbar on right side
                        name=title
                    ),
                    row=row, col=col
                )
    
    # Update layout
    fig.update_layout(
        height=1400,
        title_text=f"Options Activity Heatmaps - {symbol}",
        showlegend=False
    )
    
    # Update axes labels
    fig.update_xaxes(title_text="Strike Price")
    fig.update_yaxes(title_text="Expiration Date")
    
    st.plotly_chart(fig, use_container_width=True)

@st.cache_data(ttl=120)  # Cache for 2 minutes
def get_contract_details(symbol: str, target_date: date = None) -> pd.DataFrame:
    """Get detailed contract information for a symbol, ordered by volume."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if target_date is None:
            target_date = date.today()
        
        # Get contract details for the specified date
        # First try daily_option_snapshot for historical data
        query = """
            SELECT 
                oc.contract_ticker,
                oc.contract_type,
                oc.strike_price,
                oc.expiration_date,
                oc.shares_per_contract,
                dos.volume,
                dos.open_interest,
                dos.close_price,
                dos.implied_volatility,
                dos.greeks_delta,
                s.close as underlying_price,
                dos.date
            FROM daily_option_snapshot dos
            INNER JOIN option_contracts oc ON dos.contract_ticker = oc.contract_ticker
            LEFT JOIN daily_stock_snapshot s ON dos.symbol = s.symbol AND dos.date = s.date
            WHERE oc.symbol = %s
              AND dos.date = %s
              AND dos.volume > 0
            ORDER BY dos.volume DESC
        """
        
        cur.execute(query, (symbol, target_date))
        rows = cur.fetchall()
        
        # If no historical data found, try temp tables for today's data
        if not rows and target_date == date.today():
            query = """
                WITH latest_temp_option AS (
                    SELECT DISTINCT ON (symbol, contract_ticker)
                        symbol, contract_ticker, session_volume, open_interest, 
                        session_close, implied_volatility, greeks_delta, as_of_timestamp
                    FROM temp_option
                    WHERE symbol = %s AND session_volume > 0
                    ORDER BY symbol, contract_ticker, as_of_timestamp DESC
                ),
                latest_temp_stock AS (
                    SELECT DISTINCT ON (symbol)
                        symbol, day_close, day_vwap
                    FROM temp_stock
                    WHERE symbol = %s
                    ORDER BY symbol, as_of_timestamp DESC
                )
                SELECT 
                    oc.contract_ticker,
                    oc.contract_type,
                    oc.strike_price,
                    oc.expiration_date,
                    oc.shares_per_contract,
                    o.session_volume as volume,
                    o.open_interest,
                    o.session_close as close_price,
                    o.implied_volatility,
                    o.greeks_delta,
                    COALESCE(s.day_close, s.day_vwap) as underlying_price,
                    date(o.as_of_timestamp) as date
                FROM latest_temp_option o
                INNER JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
                LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                ORDER BY o.session_volume DESC
            """
            cur.execute(query, (symbol, symbol))
            rows = cur.fetchall()
        
        df = pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
        
        # Add calculated fields
        if not df.empty:
            df['days_to_expiry'] = (pd.to_datetime(df['expiration_date']) - pd.Timestamp.now()).dt.days
            df['moneyness'] = df.apply(lambda row: 
                'ITM' if (row['contract_type'] == 'call' and row['strike_price'] < row['underlying_price']) or 
                         (row['contract_type'] == 'put' and row['strike_price'] > row['underlying_price'])
                else 'OTM', axis=1)
            df['volume_oi_ratio'] = df['volume'] / df['open_interest'].replace(0, 1)  # Avoid division by zero
            
            # Add volume magnitude calculation
            df['volume_magnitude'] = df['volume'] * df['close_price'] * df['shares_per_contract']
        
        return df
        
    except Exception as e:
        st.error(f"Error fetching contract details: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_consolidated_symbol_data(symbol: str, target_date: date = None) -> Dict[str, Any]:
    """Get all data needed for symbol analysis in a single optimized query."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if target_date is None:
            target_date = date.today()
        
        # Consolidated query to get all symbol data at once
        query = """
            WITH latest_temp_stock AS (
                SELECT DISTINCT ON (symbol)
                    symbol, day_open, day_high, day_low, day_close, day_volume, day_vwap, as_of_timestamp
                FROM temp_stock
                WHERE symbol = %s
                ORDER BY symbol, as_of_timestamp DESC
            ),
            latest_temp_option AS (
                SELECT DISTINCT ON (symbol, contract_ticker)
                    symbol, contract_ticker, session_volume, open_interest, 
                    session_close, implied_volatility, greeks_delta, as_of_timestamp
                FROM temp_option
                WHERE symbol = %s AND session_volume > 0
                ORDER BY symbol, contract_ticker, as_of_timestamp DESC
            ),
            daily_stock_data AS (
                SELECT date, open, high, low, close, trading_volume as volume
                FROM daily_stock_snapshot
                WHERE symbol = %s AND date >= CURRENT_DATE - INTERVAL '30 days'
                ORDER BY date ASC
            ),
            daily_option_data AS (
                SELECT 
                    dos.date,
                    oc.contract_type,
                    SUM(dos.volume) as total_volume,
                    COUNT(*) as contract_count,
                    AVG(dos.implied_volatility) as avg_iv
                FROM daily_option_snapshot dos
                INNER JOIN option_contracts oc ON dos.contract_ticker = oc.contract_ticker
                WHERE oc.symbol = %s AND dos.date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY dos.date, oc.contract_type
                ORDER BY dos.date ASC, oc.contract_type
            ),
            heatmap_data AS (
                SELECT 
                    oc.strike_price,
                    oc.expiration_date,
                    oc.contract_type,
                    dos.volume,
                    dos.open_interest,
                    dos.close_price as option_price,
                    dos.implied_volatility,
                    s.close as underlying_price
                FROM daily_option_snapshot dos
                INNER JOIN option_contracts oc ON dos.contract_ticker = oc.contract_ticker
                LEFT JOIN daily_stock_snapshot s ON oc.symbol = s.symbol AND dos.date = s.date
                WHERE oc.symbol = %s AND dos.date = %s AND dos.volume > 0
                
                UNION ALL
                
                SELECT 
                    oc.strike_price,
                    oc.expiration_date,
                    oc.contract_type,
                    o.session_volume as volume,
                    o.open_interest,
                    o.session_close as option_price,
                    o.implied_volatility,
                    COALESCE(s.day_close, s.day_vwap) as underlying_price
                FROM latest_temp_option o
                INNER JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
                LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                WHERE o.symbol = %s AND %s = CURRENT_DATE
            ),
            contract_details AS (
                SELECT 
                    oc.contract_ticker,
                    oc.contract_type,
                    oc.strike_price,
                    oc.expiration_date,
                    dos.volume,
                    dos.open_interest,
                    dos.close_price,
                    dos.implied_volatility,
                    dos.greeks_delta,
                    s.close as underlying_price,
                    dos.date
                FROM daily_option_snapshot dos
                INNER JOIN option_contracts oc ON dos.contract_ticker = oc.contract_ticker
                LEFT JOIN daily_stock_snapshot s ON oc.symbol = s.symbol AND dos.date = s.date
                WHERE oc.symbol = %s AND dos.date = %s AND dos.volume > 0
                
                UNION ALL
                
                SELECT 
                    oc.contract_ticker,
                    oc.contract_type,
                    oc.strike_price,
                    oc.expiration_date,
                    o.session_volume as volume,
                    o.open_interest,
                    o.session_close as close_price,
                    o.implied_volatility,
                    o.greeks_delta,
                    COALESCE(s.day_close, s.day_vwap) as underlying_price,
                    date(o.as_of_timestamp) as date
                FROM latest_temp_option o
                INNER JOIN option_contracts oc ON o.contract_ticker = oc.contract_ticker
                LEFT JOIN latest_temp_stock s ON o.symbol = s.symbol
                WHERE o.symbol = %s AND %s = CURRENT_DATE
            )
            SELECT 
                'stock_history' as data_type,
                json_agg(daily_stock_data.*) as data
            FROM daily_stock_data
            
            UNION ALL
            
            SELECT 
                'options_history' as data_type,
                json_agg(daily_option_data.*) as data
            FROM daily_option_data
            
            UNION ALL
            
            SELECT 
                'heatmap_data' as data_type,
                json_agg(heatmap_data.*) as data
            FROM heatmap_data
            
            UNION ALL
            
            SELECT 
                'contract_details' as data_type,
                json_agg(contract_details.*) as data
            FROM contract_details
            
            UNION ALL
            
            SELECT 
                'latest_stock' as data_type,
                json_agg(latest_temp_stock.*) as data
            FROM latest_temp_stock
            
            UNION ALL
            
            SELECT 
                'latest_options' as data_type,
                json_agg(latest_temp_option.*) as data
            FROM latest_temp_option
        """
        
        cur.execute(query, (symbol, symbol, symbol, symbol, symbol, target_date, symbol, target_date, symbol, target_date, symbol, target_date))
        rows = cur.fetchall()
        
        # Process results
        result = {
            'stock_history': pd.DataFrame(),
            'options_history': pd.DataFrame(),
            'heatmap_data': pd.DataFrame(),
            'contract_details': pd.DataFrame(),
            'latest_stock': pd.DataFrame(),
            'latest_options': pd.DataFrame()
        }
        
        for row in rows:
            data_type = row['data_type']
            data_json = row['data']
            if data_json:
                result[data_type] = pd.DataFrame(data_json)
        
        return result
        
    except Exception as e:
        st.error(f"Error fetching consolidated symbol data: {e}")
        return {
            'stock_history': pd.DataFrame(),
            'options_history': pd.DataFrame(),
            'heatmap_data': pd.DataFrame(),
            'contract_details': pd.DataFrame(),
            'latest_stock': pd.DataFrame(),
            'latest_options': pd.DataFrame()
        }
    finally:
        conn.close()

@st.cache_data(ttl=180)  # Cache for 3 minutes
def get_symbol_anomaly_data(symbol: str, target_date: date = None) -> Dict[str, Any]:
    """Get anomaly data for a symbol even if below 7.5 threshold."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        if target_date is None:
            target_date = date.today()
            
        query = """
            SELECT 
                symbol,
                total_score,
                volume_score,
                volume_oi_ratio_score,
                otm_score,
                directional_score,
                time_score,
                call_volume,
                put_volume,
                total_volume,
                call_baseline_avg,
                put_baseline_avg,
                call_multiplier,
                put_multiplier,
                direction,
                pattern_description,
                z_score,
                otm_call_percentage,
                short_term_percentage,
                call_put_ratio,
                as_of_timestamp,
                event_date,
                open_interest,
                call_magnitude,
                put_magnitude,
                total_magnitude
            FROM daily_anomaly_snapshot
            WHERE symbol = %s AND event_date = %s
            ORDER BY as_of_timestamp DESC
            LIMIT 1
        """
        
        cur.execute(query, (symbol, target_date))
        row = cur.fetchone()
        
        if not row:
            return None
        
        # Build anomaly data from table structure
        anomaly_data = {
            'composite_score': float(row['total_score']),
            'details': {
                'volume_score': float(row.get('volume_score', 0)),
                'volume_oi_ratio_score': float(row.get('volume_oi_ratio_score', 0)),
                'otm_score': float(row.get('otm_score', 0)),
                'directional_score': float(row.get('directional_score', 0)),
                'time_score': float(row.get('time_score', 0)),
                'call_volume': int(row.get('call_volume', 0)),
                'put_volume': int(row.get('put_volume', 0)),
                'total_volume': int(row.get('total_volume', 0)),
                'call_baseline_avg': float(row.get('call_baseline_avg', 0)),
                'put_baseline_avg': float(row.get('put_baseline_avg', 0)),
                'call_multiplier': float(row.get('call_multiplier', 0)),
                'current_open_interest': int(row.get('open_interest', 0)),
                'pattern_description': row.get('pattern_description', 'Unusual trading pattern'),
                'z_score': float(row.get('z_score', 0))
            }
        }
        return anomaly_data
        
    except Exception as e:
        st.error(f"Error fetching symbol anomaly data: {e}")
        return None
    finally:
        conn.close()

def create_combined_price_volume_chart(stock_df: pd.DataFrame, symbol: str) -> None:
    """Create combined stock price and volume chart."""
    if stock_df.empty:
        return
    
    st.subheader(f"{symbol} Stock Price & Volume (30 Days)")
    
    # Create subplot with secondary y-axis
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        row_heights=[0.7, 0.3],
        subplot_titles=[f'{symbol} Price', 'Volume']
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=stock_df['date'],
            open=stock_df['open'],
            high=stock_df['high'],
            low=stock_df['low'],
            close=stock_df['close'],
            name='Price'
        ),
        row=1, col=1
    )
    
    # Volume bars
    fig.add_trace(
        go.Bar(
            x=stock_df['date'],
            y=stock_df['volume'],
            name='Volume',
            marker_color='rgba(158,202,225,0.8)'
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=500,
        showlegend=False,
        xaxis_rangeslider_visible=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

def create_basic_symbol_analysis(symbol: str, selected_date: date = None) -> None:
    """Create basic analysis for a symbol without anomaly data."""
    st.header(f"Analysis: {symbol}")
    
    # Get historical data
    history = get_symbol_history(symbol)
    
    if history['stock'].empty:
        st.warning(f"No historical data available for {symbol}")
        return
    
    # Get anomaly data for this symbol (even if below 7.5 threshold)
    anomaly_data = get_symbol_anomaly_data(symbol, selected_date)
    
    if anomaly_data:
        # Show anomaly scores even if below 7.5
        st.subheader("Anomaly Analysis")
        details = anomaly_data.get('details', {})
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Volume Score",
                f"{details.get('volume_score', 0):.1f}/3.0",
                help="Statistical Z-score analysis vs 30-day baseline"
            )
        
        with col2:
            st.metric(
                "Volume:OI Ratio Score", 
                f"{details.get('volume_oi_ratio_score', 0):.1f}/2.0",
                help="Volume to open interest ratio anomaly vs historical baseline"
            )
        
        with col3:
            st.metric(
                "OTM Call Score", 
                f"{details.get('otm_score', 0):.1f}/2.0",
                help="Out-of-money call concentration (classic insider pattern)"
            )
        
        with col4:
            st.metric(
                "Directional Score",
                f"{details.get('directional_score', 0):.1f}/1.0", 
                help="Strong call/put preference indicating conviction"
            )
        
        with col5:
            st.metric(
                "Time Pressure Score",
                f"{details.get('time_score', 0):.1f}/2.0",
                help="Near-term expiration clustering"
            )
        
        # Trading activity metrics
        st.subheader("Current Trading Activity")
        
        call_volume = details.get('call_volume', 0)
        put_volume = details.get('put_volume', 0)
        call_baseline = details.get('call_baseline_avg', 1)
        put_baseline = details.get('put_baseline_avg', 1)
        current_open_interest = details.get('current_open_interest', 0)
        open_interest_multiplier = details.get('open_interest_multiplier', 0)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Call Volume",
                f"{call_volume:,}",
                delta=f"{((call_volume/call_baseline - 1) * 100) if call_baseline > 0 else 0:.0f}% vs baseline"
            )
        
        with col2:
            st.metric(
                "Put Volume", 
                f"{put_volume:,}",
                delta=f"{((put_volume/put_baseline - 1) * 100) if put_baseline > 0 else 0:.0f}% vs baseline"
            )
        
        with col3:
            call_ratio = call_volume / (call_volume + put_volume) if (call_volume + put_volume) > 0 else 0
            st.metric(
                "Call/Put Ratio",
                f"{call_ratio:.1%}",
                help="Percentage of total volume in calls vs puts"
            )
        
        with col4:
            st.metric(
                "Total Volume",
                f"{call_volume + put_volume:,}",
                help="Total option contracts traded today"
            )
        
        with col5:
            st.metric(
                "Open Interest",
                f"{current_open_interest:,}",
                delta=f"{open_interest_multiplier:.1f}x vs prior day",
                help=f"Current open interest: {current_open_interest:,}"
            )
    else:
        # Show basic metrics if no anomaly data
        st.subheader("Current Trading Activity")
        
        # Get latest stock data
        latest_stock = history['stock'].iloc[-1] if not history['stock'].empty else None
        latest_options = history['intraday'] if not history['intraday'].empty else pd.DataFrame()
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            if latest_stock is not None:
                st.metric("Current Price", f"${latest_stock['close']:.2f}")
            else:
                st.metric("Current Price", "N/A")
        
        with col2:
            if latest_stock is not None:
                st.metric("Volume", f"{latest_stock['volume']:,}")
            else:
                st.metric("Volume", "N/A")
        
        with col3:
            if not latest_options.empty:
                call_volume = latest_options[latest_options['contract_type'] == 'call']['session_volume'].sum()
                put_volume = latest_options[latest_options['contract_type'] == 'put']['session_volume'].sum()
                total_volume = call_volume + put_volume
                st.metric("Options Volume", f"{total_volume:,}")
            else:
                st.metric("Options Volume", "N/A")
        
        with col4:
            if not latest_options.empty:
                call_volume = latest_options[latest_options['contract_type'] == 'call']['session_volume'].sum()
                put_volume = latest_options[latest_options['contract_type'] == 'put']['session_volume'].sum()
                call_put_ratio = call_volume / put_volume if put_volume > 0 else float('inf')
                st.metric("Call/Put Ratio", f"{call_put_ratio:.2f}")
            else:
                st.metric("Call/Put Ratio", "N/A")
        
        with col5:
            if not latest_options.empty:
                total_oi = latest_options['open_interest'].sum()
                st.metric("Open Interest", f"{total_oi:,}")
            else:
                st.metric("Open Interest", "N/A")
    
    # Show combined price and volume chart
    create_combined_price_volume_chart(history['stock'], symbol)
    create_options_activity_chart(history['options'], symbol)

def create_contracts_table(symbol: str, target_date: date = None) -> None:
    """Create a detailed contracts table for the selected symbol."""
    st.subheader(f"Contract Details for {symbol}")
    
    if target_date:
        st.write(f"Data for: {target_date}")
    
    # Get contract data
    contracts_df = get_contract_details(symbol, target_date)
    
    if contracts_df.empty:
        st.info(f"No contract data available for {symbol}" + (f" on {target_date}" if target_date else ""))
        return
    
    # Prepare display columns
    display_df = contracts_df.copy()
    
    # Convert numeric columns to proper types for sorting
    display_df['volume'] = pd.to_numeric(display_df['volume'], errors='coerce').fillna(0).astype(int)
    display_df['open_interest'] = pd.to_numeric(display_df['open_interest'], errors='coerce').fillna(0).astype(int)
    
    # Format columns for display
    display_df['Strike'] = display_df['strike_price'].apply(lambda x: f"${x:.2f}")
    display_df['Expiration'] = pd.to_datetime(display_df['expiration_date']).dt.strftime('%Y-%m-%d')
    display_df['Days to Exp'] = display_df['days_to_expiry']
    display_df['Volume'] = display_df['volume']  # Keep as numeric for sorting
    display_df['Open Interest'] = display_df['open_interest']  # Keep as numeric for sorting
    display_df['Price'] = display_df['close_price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
    display_df['Volume Magnitude'] = display_df['volume_magnitude'].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "N/A")
    display_df['Underlying'] = display_df['underlying_price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
    display_df['IV'] = display_df['implied_volatility'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
    display_df['Delta'] = display_df['greeks_delta'].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "N/A")
    display_df['V/OI Ratio'] = display_df['volume_oi_ratio'].apply(lambda x: f"{x:.2f}")
    
    # Select and reorder columns for display
    display_columns = [
        'contract_ticker', 'contract_type', 'Strike', 'Expiration', 'Days to Exp',
        'moneyness', 'Volume', 'Open Interest', 'Price', 'Volume Magnitude', 'Underlying', 'IV', 'Delta', 'V/OI Ratio'
    ]
    
    display_df = display_df[display_columns]
    
    # Rename columns for better display
    display_df.columns = [
        'Contract', 'Type', 'Strike', 'Expiration', 'Days to Exp',
        'Moneyness', 'Volume', 'Open Interest', 'Price', 'Volume Magnitude', 'Underlying', 'IV', 'Delta', 'V/OI Ratio'
    ]
    
    # Display the table
    st.dataframe(
        display_df,
        use_container_width=True,
        height=400
    )
    
    # Add summary statistics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Contracts", len(contracts_df))
    
    with col2:
        total_volume = contracts_df['volume'].sum()
        st.metric("Total Volume", f"{total_volume:,}")
    
    with col3:
        total_oi = contracts_df['open_interest'].sum()
        st.metric("Total Open Interest", f"{total_oi:,}")
    
    with col4:
        call_volume = contracts_df[contracts_df['contract_type'] == 'call']['volume'].sum()
        put_volume = contracts_df[contracts_df['contract_type'] == 'put']['volume'].sum()
        call_put_ratio = call_volume / put_volume if put_volume > 0 else float('inf')
        st.metric("Call/Put Ratio", f"{call_put_ratio:.2f}")


# =============================================================================
# HIGH CONVICTION GREEKS FUNCTIONS
# =============================================================================

# Thresholds from validated analysis (93rd percentile)
HIGH_CONVICTION_THRESHOLDS = {
    'theta': 0.1624,
    'gamma': 0.4683,
    'vega': 0.1326,
    'otm_score': 1.4
}


@st.cache_data(ttl=300)
def get_recommended_option_details(contract_ticker: str) -> Dict:
    """Get Greeks and details for a recommended option."""
    if not contract_ticker:
        return {}
    
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Try temp_option first (current data), then daily_option_snapshot
        cur.execute("""
            SELECT 
                contract_ticker,
                session_close as close_price,
                session_volume as volume,
                implied_volatility,
                greeks_delta,
                greeks_gamma,
                greeks_theta,
                greeks_vega,
                open_interest
            FROM temp_option
            WHERE contract_ticker = %s
            ORDER BY as_of_timestamp DESC
            LIMIT 1
        """, (contract_ticker,))
        
        row = cur.fetchone()
        
        if not row:
            # Fallback to daily snapshot
            cur.execute("""
                SELECT 
                    contract_ticker,
                    close_price,
                    volume,
                    implied_volatility,
                    greeks_delta,
                    greeks_gamma,
                    greeks_theta,
                    greeks_vega,
                    open_interest
                FROM daily_option_snapshot
                WHERE contract_ticker = %s
                ORDER BY date DESC
                LIMIT 1
            """, (contract_ticker,))
            row = cur.fetchone()
        
        return dict(row) if row else {}
        
    except Exception as e:
        return {}
    finally:
        conn.close()


def create_greeks_display(anomaly_data: Dict) -> None:
    """Display Greeks information for high conviction alerts."""
    
    hc_score = anomaly_data.get('high_conviction_score', 0)
    is_hc = anomaly_data.get('is_high_conviction', False)
    recommended_option = anomaly_data.get('recommended_option', '')
    otm_score = anomaly_data.get('otm_score', 0)
    
    if not is_hc and hc_score < 2:
        return
    
    st.subheader("High Conviction Analysis")
    
    # Get option details
    option_details = get_recommended_option_details(recommended_option) if recommended_option else {}
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        status = "HIGH CONVICTION" if is_hc else "Moderate Signal"
        st.markdown(f"""
        ### Greeks Score: **{hc_score}/4**
        **{status}**
        
        **Recommended Option:**  
        `{recommended_option if recommended_option else 'N/A'}`
        """)
        
        if option_details:
            price = option_details.get('close_price', 0)
            volume = option_details.get('volume', 0)
            st.markdown(f"""
            - **Price**: ${float(price):.2f}
            - **Volume**: {int(volume):,}
            """)
    
    with col2:
        # Greeks breakdown with threshold comparison
        theta = abs(float(option_details.get('greeks_theta', 0) or 0))
        gamma = float(option_details.get('greeks_gamma', 0) or 0)
        vega = float(option_details.get('greeks_vega', 0) or 0)
        
        theta_pass = theta >= HIGH_CONVICTION_THRESHOLDS['theta']
        gamma_pass = gamma >= HIGH_CONVICTION_THRESHOLDS['gamma']
        vega_pass = vega >= HIGH_CONVICTION_THRESHOLDS['vega']
        otm_pass = otm_score >= HIGH_CONVICTION_THRESHOLDS['otm_score']
        
        st.markdown("**Factor Breakdown (93rd percentile thresholds):**")
        
        factors_data = [
            ("Theta", theta, HIGH_CONVICTION_THRESHOLDS['theta'], theta_pass),
            ("Gamma", gamma, HIGH_CONVICTION_THRESHOLDS['gamma'], gamma_pass),
            ("Vega", vega, HIGH_CONVICTION_THRESHOLDS['vega'], vega_pass),
            ("OTM Score", otm_score, HIGH_CONVICTION_THRESHOLDS['otm_score'], otm_pass),
        ]
        
        for name, value, threshold, passed in factors_data:
            status_str = "[PASS]" if passed else "[FAIL]"
            st.markdown(f"{status_str} **{name}**: {value:.4f} (threshold: {threshold})")
    
    # Additional Greeks info
    if option_details:
        st.markdown("---")
        cols = st.columns(5)
        
        with cols[0]:
            delta = option_details.get('greeks_delta', 0)
            st.metric("Delta", f"{float(delta or 0):.4f}")
        
        with cols[1]:
            st.metric("Gamma", f"{gamma:.4f}", 
                     delta="PASS" if gamma_pass else "FAIL",
                     delta_color="normal" if gamma_pass else "inverse")
        
        with cols[2]:
            st.metric("Theta", f"{theta:.4f}",
                     delta="PASS" if theta_pass else "FAIL",
                     delta_color="normal" if theta_pass else "inverse")
        
        with cols[3]:
            st.metric("Vega", f"{vega:.4f}",
                     delta="PASS" if vega_pass else "FAIL",
                     delta_color="normal" if vega_pass else "inverse")
        
        with cols[4]:
            iv = option_details.get('implied_volatility', 0)
            st.metric("IV", f"{float(iv or 0):.1%}")


# =============================================================================
# PERFORMANCE ANALYSIS PAGE
# =============================================================================

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_performance_data() -> pd.DataFrame:
    """Get historical anomaly data with option performance for analysis."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get anomalies with their recommended options and subsequent performance
        cur.execute("""
            WITH base AS (
                SELECT 
                    a.symbol, a.event_date, a.direction,
                    a.total_score, a.otm_score, a.total_magnitude,
                    COALESCE(a.high_conviction_score, 0) as high_conviction_score,
                    COALESCE(a.is_high_conviction, false) as is_high_conviction,
                    a.recommended_option,
                    COALESCE(a.is_bot_driven, false) as is_bot_driven,
                    COALESCE(a.is_earnings_related, false) as is_earnings_related,
                    o.close_price as entry_price,
                    o.greeks_theta, o.greeks_gamma, o.greeks_vega, o.implied_volatility
                FROM daily_anomaly_snapshot a
                LEFT JOIN daily_option_snapshot o 
                    ON a.recommended_option = o.contract_ticker 
                    AND a.event_date = o.date
                WHERE a.total_magnitude >= 20000
                  AND a.event_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND a.event_date <= CURRENT_DATE - INTERVAL '10 days'
            ),
            price_agg AS (
                SELECT 
                    b.recommended_option,
                    b.event_date,
                    MAX(o_future.close_price) as max_price
                FROM base b
                INNER JOIN daily_option_snapshot o_future
                    ON b.recommended_option = o_future.contract_ticker
                    AND o_future.date > b.event_date
                    AND o_future.date <= b.event_date + INTERVAL '10 days'
                WHERE b.recommended_option IS NOT NULL
                GROUP BY b.recommended_option, b.event_date
            )
            SELECT 
                b.*,
                p.max_price
            FROM base b
            LEFT JOIN price_agg p 
                ON b.recommended_option = p.recommended_option 
                AND b.event_date = p.event_date
            ORDER BY b.event_date DESC
        """)
        
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        
        # Calculate performance metrics
        df['entry_price'] = pd.to_numeric(df['entry_price'], errors='coerce')
        df['max_price'] = pd.to_numeric(df['max_price'], errors='coerce')
        
        # Calculate max ROI (if we sold at the peak)
        df['max_roi'] = ((df['max_price'] - df['entry_price']) / df['entry_price'] * 100).fillna(0)
        
        # Did it hit +100% take profit?
        df['hit_100_tp'] = df['max_price'] >= (df['entry_price'] * 2.0)
        
        # Did it hit +50% take profit?
        df['hit_50_tp'] = df['max_price'] >= (df['entry_price'] * 1.5)
        
        return df
        
    except Exception as e:
        st.error(f"Error fetching performance data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


def create_performance_analysis_page() -> None:
    """Create the performance analysis page."""
    st.header("Algorithm Performance Analysis")
    
    st.markdown("""
    Analysis of historical high conviction alerts and their option performance.
    Based on +100% take profit strategy (sell when option doubles, otherwise hold to expiration).
    """)
    
    # Get performance data
    df = get_performance_data()
    
    if df.empty:
        st.warning("No performance data available. Need at least 10 days of historical data.")
        return
    
    # Filter out bot-driven and earnings-related
    df_clean = df[
        (df['is_bot_driven'] == False) & 
        (df['is_earnings_related'] == False)
    ].copy()
    
    st.markdown(f"**Data Range**: {df_clean['event_date'].min()} to {df_clean['event_date'].max()}")
    st.markdown(f"**Total Alerts**: {len(df_clean)} (after filtering bot/earnings)")
    
    # Summary metrics
    st.subheader("Overall Performance Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # All alerts
    total = len(df_clean)
    hit_100 = df_clean['hit_100_tp'].sum()
    hit_50 = df_clean['hit_50_tp'].sum()
    
    with col1:
        st.metric("Total Alerts", total)
    
    with col2:
        rate_100 = hit_100 / total * 100 if total > 0 else 0
        st.metric("+100% Hit Rate", f"{rate_100:.1f}%", 
                 delta=f"{hit_100} hits")
    
    with col3:
        rate_50 = hit_50 / total * 100 if total > 0 else 0
        st.metric("+50% Hit Rate", f"{rate_50:.1f}%",
                 delta=f"{hit_50} hits")
    
    with col4:
        avg_roi = df_clean['max_roi'].mean()
        st.metric("Avg Max ROI", f"{avg_roi:.1f}%")
    
    # High Conviction vs All
    st.markdown("---")
    st.subheader("High Conviction (Score >= 3) vs All Alerts")
    
    df_hc = df_clean[df_clean['high_conviction_score'] >= 3]
    df_not_hc = df_clean[df_clean['high_conviction_score'] < 3]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### High Conviction Alerts")
        hc_total = len(df_hc)
        hc_hit_100 = df_hc['hit_100_tp'].sum()
        hc_rate = hc_hit_100 / hc_total * 100 if hc_total > 0 else 0
        
        st.metric("Alerts", hc_total)
        st.metric("+100% Hit Rate", f"{hc_rate:.1f}%", delta=f"{hc_hit_100} hits")
        st.metric("Avg Max ROI", f"{df_hc['max_roi'].mean():.1f}%" if hc_total > 0 else "N/A")
    
    with col2:
        st.markdown("### Other Alerts (Score < 3)")
        other_total = len(df_not_hc)
        other_hit_100 = df_not_hc['hit_100_tp'].sum()
        other_rate = other_hit_100 / other_total * 100 if other_total > 0 else 0
        
        st.metric("Alerts", other_total)
        st.metric("+100% Hit Rate", f"{other_rate:.1f}%", delta=f"{other_hit_100} hits")
        st.metric("Avg Max ROI", f"{df_not_hc['max_roi'].mean():.1f}%" if other_total > 0 else "N/A")
    
    # Lift calculation
    if other_rate > 0 and hc_rate > 0:
        lift = hc_rate / other_rate
        st.markdown(f"**Lift**: High conviction alerts are **{lift:.2f}x** more likely to hit +100%")
    
    # Score breakdown
    st.markdown("---")
    st.subheader("Performance by Greeks Score")
    
    score_data = []
    for score in range(5):
        subset = df_clean[df_clean['high_conviction_score'] == score]
        if len(subset) > 0:
            score_data.append({
                'Score': score,
                'Alerts': len(subset),
                '+100% Hits': subset['hit_100_tp'].sum(),
                'Hit Rate': f"{subset['hit_100_tp'].sum() / len(subset) * 100:.1f}%",
                'Avg Max ROI': f"{subset['max_roi'].mean():.1f}%"
            })
    
    if score_data:
        score_df = pd.DataFrame(score_data)
        st.dataframe(score_df, use_container_width=True, hide_index=True)
    
    # Visualization
    st.markdown("---")
    st.subheader("Hit Rate by Score")
    
    if score_data:
        import plotly.express as px
        
        viz_data = []
        for score in range(5):
            subset = df_clean[df_clean['high_conviction_score'] == score]
            if len(subset) > 0:
                viz_data.append({
                    'Score': str(score),
                    'Hit Rate': subset['hit_100_tp'].sum() / len(subset) * 100,
                    'Count': len(subset)
                })
        
        if viz_data:
            viz_df = pd.DataFrame(viz_data)
            
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=viz_df['Score'],
                y=viz_df['Hit Rate'],
                text=viz_df['Count'].apply(lambda x: f'n={x}'),
                textposition='outside',
                marker_color=['#ff6b6b' if int(s) < 3 else '#51cf66' for s in viz_df['Score']]
            ))
            
            fig.add_hline(y=50, line_dash="dash", line_color="green", 
                         annotation_text="Break-even (50%)")
            
            fig.update_layout(
                title="Hit Rate by Greeks Score",
                xaxis_title="Greeks Score",
                yaxis_title="+100% Hit Rate (%)",
                yaxis_range=[0, 100]
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    # Recent high conviction alerts
    st.markdown("---")
    st.subheader("Recent High Conviction Alerts")
    
    recent_hc = df_hc.head(20)[['event_date', 'symbol', 'direction', 'high_conviction_score', 
                                'entry_price', 'max_price', 'max_roi', 'hit_100_tp']].copy()
    
    if not recent_hc.empty:
        recent_hc['entry_price'] = recent_hc['entry_price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
        recent_hc['max_price'] = recent_hc['max_price'].apply(lambda x: f"${x:.2f}" if pd.notna(x) else "N/A")
        recent_hc['max_roi'] = recent_hc['max_roi'].apply(lambda x: f"{x:.1f}%")
        recent_hc['hit_100_tp'] = recent_hc['hit_100_tp'].apply(lambda x: "YES" if x else "NO")
        recent_hc['direction'] = recent_hc['direction'].apply(
            lambda x: "BULLISH" if x == 'call_heavy' else "BEARISH" if x == 'put_heavy' else x
        )
        
        recent_hc.columns = ['Date', 'Symbol', 'Direction', 'Score', 'Entry', 'Max Price', 'Max ROI', 'Hit +100%']
        st.dataframe(recent_hc, use_container_width=True, hide_index=True)
    else:
        st.info("No high conviction alerts in the analysis period.")


# =============================================================================
# GREEKS-BASED SYMBOL ANALYSIS
# =============================================================================

@st.cache_data(ttl=300)
def get_high_conviction_anomalies() -> pd.DataFrame:
    """Get only high conviction anomalies."""
    conn = db.connect()
    try:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        cur.execute("""
            SELECT 
                symbol, event_date, direction,
                total_score, otm_score, total_magnitude,
                high_conviction_score, is_high_conviction, recommended_option
            FROM daily_anomaly_snapshot
            WHERE is_high_conviction = TRUE
              AND total_magnitude >= 20000
            ORDER BY event_date DESC, high_conviction_score DESC
        """)
        
        rows = cur.fetchall()
        return pd.DataFrame([dict(row) for row in rows]) if rows else pd.DataFrame()
        
    except Exception as e:
        return pd.DataFrame()
    finally:
        conn.close()


def create_greeks_symbol_analysis(symbol: str, anomaly_data: Dict, selected_date: date = None) -> None:
    """Create Greeks-focused analysis for a specific symbol."""
    st.header(f"Greeks Analysis: {symbol}")
    
    # Get historical data
    history = get_symbol_history(symbol)
    
    if history['stock'].empty:
        st.warning(f"No historical data available for {symbol}")
        return
    
    details = anomaly_data.get('details', {})
    
    # High conviction header info
    hc_score = details.get('high_conviction_score', 0)
    is_hc = details.get('is_high_conviction', False)
    recommended_option = details.get('recommended_option', '')
    direction = details.get('direction', '')
    
    # Direction and score
    dir_display = "BULLISH" if direction == 'call_heavy' else "BEARISH" if direction == 'put_heavy' else direction
    status = "HIGH CONVICTION" if is_hc else "Below Threshold"
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Greeks Score", f"{hc_score}/4")
    with col2:
        st.metric("Status", status)
    with col3:
        st.metric("Direction", dir_display)
    with col4:
        magnitude = details.get('total_magnitude', 0)
        st.metric("Magnitude", f"${magnitude:,.0f}")
    
    # Recommended option section
    st.markdown("---")
    st.subheader("Recommended Option")
    
    if recommended_option:
        option_details = get_recommended_option_details(recommended_option)
        
        if option_details:
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Contract", recommended_option)
            with col2:
                price = float(option_details.get('close_price', 0) or 0)
                st.metric("Price", f"${price:.2f}")
            with col3:
                volume = int(option_details.get('volume', 0) or 0)
                st.metric("Volume", f"{volume:,}")
            with col4:
                iv = float(option_details.get('implied_volatility', 0) or 0)
                st.metric("IV", f"{iv:.1%}")
            with col5:
                oi = int(option_details.get('open_interest', 0) or 0)
                st.metric("Open Interest", f"{oi:,}")
            
            # Greeks breakdown
            st.markdown("---")
            st.subheader("Greeks Factor Analysis")
            
            theta = abs(float(option_details.get('greeks_theta', 0) or 0))
            gamma = float(option_details.get('greeks_gamma', 0) or 0)
            vega = float(option_details.get('greeks_vega', 0) or 0)
            delta = float(option_details.get('greeks_delta', 0) or 0)
            otm_score = details.get('otm_score', 0)
            
            theta_pass = theta >= HIGH_CONVICTION_THRESHOLDS['theta']
            gamma_pass = gamma >= HIGH_CONVICTION_THRESHOLDS['gamma']
            vega_pass = vega >= HIGH_CONVICTION_THRESHOLDS['vega']
            otm_pass = otm_score >= HIGH_CONVICTION_THRESHOLDS['otm_score']
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Delta", f"{delta:.4f}")
            
            with col2:
                st.metric("Theta", f"{theta:.4f}",
                         delta="PASS" if theta_pass else "FAIL",
                         delta_color="normal" if theta_pass else "inverse")
            
            with col3:
                st.metric("Gamma", f"{gamma:.4f}",
                         delta="PASS" if gamma_pass else "FAIL",
                         delta_color="normal" if gamma_pass else "inverse")
            
            with col4:
                st.metric("Vega", f"{vega:.4f}",
                         delta="PASS" if vega_pass else "FAIL",
                         delta_color="normal" if vega_pass else "inverse")
            
            with col5:
                st.metric("OTM Score", f"{otm_score:.2f}",
                         delta="PASS" if otm_pass else "FAIL",
                         delta_color="normal" if otm_pass else "inverse")
            
            # Thresholds reference
            st.markdown("""
            **Thresholds (93rd percentile):** 
            Theta >= 0.1624 | Gamma >= 0.4683 | Vega >= 0.1326 | OTM Score >= 1.4
            """)
        else:
            st.warning(f"Could not fetch details for {recommended_option}")
    else:
        st.info("No recommended option available")
    
    # Price chart
    st.markdown("---")
    create_combined_price_volume_chart(history['stock'], symbol)
