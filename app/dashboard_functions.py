#!/usr/bin/env python3
"""
Dashboard support functions for Streamlit app
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from typing import Dict, List, Any

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db

def get_current_anomalies() -> pd.DataFrame:
    """Get current high-conviction anomalies from temp_anomaly table."""
    conn = db.connect()
    try:
        query = """
            SELECT 
                symbol,
                score,
                anomaly_types,
                details,
                as_of_timestamp,
                event_date
            FROM temp_anomaly
            WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'
              AND score >= 7.0
            ORDER BY score DESC, as_of_timestamp DESC
        """
        
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        st.error(f"Error fetching anomalies: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def get_symbol_history(symbol: str, days: int = 30) -> Dict[str, pd.DataFrame]:
    """Get historical data for a specific symbol."""
    conn = db.connect()
    try:
        # Get stock price history
        stock_query = """
            SELECT date, open, high, low, close, volume
            FROM daily_stock_snapshot
            WHERE symbol = %s
              AND date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date ASC
        """
        stock_df = pd.read_sql_query(stock_query, conn, params=[symbol, days])
        
        # Get options activity history
        options_query = """
            SELECT 
                dos.date,
                oc.contract_type,
                SUM(dos.volume) as total_volume,
                COUNT(*) as contract_count,
                AVG(dos.implied_volatility) as avg_iv
            FROM daily_option_snapshot dos
            INNER JOIN option_contracts oc ON dos.symbol = oc.symbol AND dos.contract_ticker = oc.contract_ticker
            WHERE dos.symbol = %s
              AND dos.date >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY dos.date, oc.contract_type
            ORDER BY dos.date ASC, oc.contract_type
        """
        options_df = pd.read_sql_query(options_query, conn, params=[symbol, days])
        
        # Get today's intraday activity
        intraday_query = """
            SELECT 
                o.contract_ticker,
                oc.contract_type,
                oc.strike_price,
                oc.expiration_date,
                o.session_volume,
                o.session_close,
                o.implied_volatility,
                o.greeks_delta,
                COALESCE(s.day_close, s.day_vwap) as underlying_price,
                o.as_of_timestamp
            FROM temp_option o
            INNER JOIN option_contracts oc ON o.symbol = oc.symbol AND o.contract_ticker = oc.contract_ticker
            LEFT JOIN temp_stock s ON o.symbol = s.symbol
            WHERE o.symbol = %s
              AND o.session_volume > 0
            ORDER BY o.session_volume DESC
        """
        intraday_df = pd.read_sql_query(intraday_query, conn, params=[symbol])
        
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

def get_anomaly_timeline(days: int = 7) -> pd.DataFrame:
    """Get timeline of anomalies over the past N days."""
    conn = db.connect()
    try:
        query = """
            SELECT 
                event_date,
                COUNT(*) as anomaly_count,
                AVG(score) as avg_score,
                MAX(score) as max_score,
                ARRAY_AGG(symbol ORDER BY score DESC) as symbols
            FROM temp_anomaly
            WHERE event_date >= CURRENT_DATE - INTERVAL '%s days'
              AND score >= 7.0
            GROUP BY event_date
            ORDER BY event_date DESC
        """
        
        df = pd.read_sql_query(query, conn, params=[days])
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
    
    # Process the data for display
    display_data = []
    for _, row in anomalies_df.iterrows():
        details = row['details'] if isinstance(row['details'], dict) else {}
        
        # Extract key metrics
        call_volume = details.get('call_volume', 0)
        put_volume = details.get('put_volume', 0)
        total_volume = call_volume + put_volume
        call_baseline = details.get('call_baseline_avg', 1)
        
        # Calculate indicators
        call_multiplier = call_volume / call_baseline if call_baseline > 0 else 0
        call_percentage = (call_volume / total_volume * 100) if total_volume > 0 else 0
        otm_score = details.get('otm_call_score', 0)
        
        # Determine pattern
        if call_percentage >= 80:
            pattern = "Strong bullish insider activity"
        elif call_percentage <= 20:
            pattern = "Strong bearish insider activity"
        else:
            pattern = "Mixed directional positioning"
        
        # Format key indicators
        key_indicators = f"""• {call_multiplier:.1f}x normal call volume
• {call_percentage:.0f}% calls vs {100-call_percentage:.0f}% puts
• OTM Score: {otm_score:.1f}/3.0"""
        
        display_data.append({
            'Symbol': row['symbol'],
            'Score': f"{row['score']:.1f}/10",
            'Key Indicators': key_indicators,
            'Insider Pattern': pattern,
            'Timestamp': row['as_of_timestamp'].strftime('%H:%M:%S') if pd.notna(row['as_of_timestamp']) else 'N/A'
        })
    
    # Create DataFrame for display
    display_df = pd.DataFrame(display_data)
    
    # Style the table
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

def create_symbol_analysis(symbol: str, anomaly_data: Dict) -> None:
    """Create detailed analysis for a specific symbol."""
    st.header(f"Deep Dive Analysis: {symbol}")
    
    # Get historical data
    history = get_symbol_history(symbol)
    
    if history['stock'].empty:
        st.warning(f"No historical data available for {symbol}")
        return
    
    # Score breakdown
    details = anomaly_data.get('details', {})
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Volume Score",
            f"{details.get('volume_score', 0):.1f}/3.0",
            help="Statistical Z-score analysis vs 30-day baseline"
        )
    
    with col2:
        st.metric(
            "OTM Call Score", 
            f"{details.get('otm_call_score', 0):.1f}/3.0",
            help="Out-of-money call concentration (classic insider pattern)"
        )
    
    with col3:
        st.metric(
            "Directional Score",
            f"{details.get('directional_score', 0):.1f}/2.0", 
            help="Strong call/put preference indicating conviction"
        )
    
    with col4:
        st.metric(
            "Time Pressure Score",
            f"{details.get('time_pressure_score', 0):.1f}/2.0",
            help="Near-term expiration clustering"
        )
    
    # Trading activity metrics
    st.subheader("Current Trading Activity")
    
    call_volume = details.get('call_volume', 0)
    put_volume = details.get('put_volume', 0)
    call_baseline = details.get('call_baseline_avg', 1)
    put_baseline = details.get('put_baseline_avg', 1)
    
    col1, col2, col3, col4 = st.columns(4)
    
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
    
    # Charts
    create_price_chart(history['stock'], symbol)
    create_options_activity_chart(history['options'], symbol)
    create_intraday_contracts_table(history['intraday'], symbol)

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
    
    # Pivot the data for easier plotting
    pivot_df = options_df.pivot_table(
        index='date', 
        columns='contract_type', 
        values='total_volume', 
        fill_value=0
    ).reset_index()
    
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

def create_intraday_contracts_table(intraday_df: pd.DataFrame, symbol: str) -> None:
    """Create table of today's most active contracts."""
    if intraday_df.empty:
        return
    
    st.subheader(f"{symbol} Today's Most Active Contracts")
    
    # Process data for display
    display_df = intraday_df.copy()
    display_df['Days to Exp'] = (pd.to_datetime(display_df['expiration_date']) - pd.Timestamp.now()).dt.days
    display_df['Moneyness'] = ((display_df['strike_price'] / display_df['underlying_price'] - 1) * 100).round(1)
    display_df['IV'] = (display_df['implied_volatility'] * 100).round(1)
    
    # Select and rename columns for display
    display_columns = {
        'contract_ticker': 'Contract',
        'contract_type': 'Type',
        'strike_price': 'Strike',
        'session_volume': 'Volume',
        'session_close': 'Price',
        'IV': 'IV %',
        'Days to Exp': 'DTE',
        'Moneyness': 'Moneyness %',
        'greeks_delta': 'Delta'
    }
    
    display_df = display_df[list(display_columns.keys())].rename(columns=display_columns)
    display_df = display_df.head(20)  # Top 20 contracts
    
    # Format numeric columns
    display_df['Strike'] = display_df['Strike'].round(2)
    display_df['Price'] = display_df['Price'].round(2)
    display_df['Delta'] = display_df['Delta'].round(3)
    
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )

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
