#!/usr/bin/env python3
"""
Insider Trading Detection Dashboard

Comprehensive Streamlit application for analyzing and investigating
insider trading anomalies detected by the system.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Check for required environment variables
try:
    from database.core.connection import db
    from dashboard_functions import (
        get_current_anomalies, get_symbol_history,
        create_anomaly_summary_table, create_anomaly_summary_by_date, create_symbol_analysis,
        get_available_symbols, create_options_heatmaps, create_contracts_table, create_basic_symbol_analysis, get_symbol_anomaly_data,
        create_performance_matrix, get_ordered_anomaly_symbols,
        create_greeks_display, create_performance_analysis_page,
        get_high_conviction_anomalies, create_greeks_symbol_analysis
    )
    DATABASE_AVAILABLE = True
except Exception as e:
    st.error(f"Database connection error: {e}")
    DATABASE_AVAILABLE = False

# Page configuration
st.set_page_config(
    page_title="Insider Trading Detection Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main Streamlit application."""
    st.title("Insider Trading Detection Dashboard")
    
    # Check if database is available
    if not DATABASE_AVAILABLE:
        st.error("""
        **Database Connection Error**
        
        The application cannot connect to the database. This could be due to:
        
        1. **Missing Environment Variables**: The `SUPABASE_DB_URL` environment variable is not set
        2. **Network Issues**: Unable to reach the database server
        3. **Authentication Problems**: Invalid database credentials
        
        **For Streamlit.io Deployment:**
        - Make sure to set the `SUPABASE_DB_URL` secret in your Streamlit.io app settings
        - Go to your Streamlit.io app -> Settings -> Secrets and add your database URL
        
        **For Local Development:**
        - Ensure your `.env` file contains the correct `SUPABASE_DB_URL`
        - Verify your database credentials are correct
        """)
        return
    
    # Sidebar Configuration
    st.sidebar.header("Navigation")
    
    # Page selection - 3 main sections
    page = st.sidebar.radio(
        "Select View",
        ["High Conviction", "Legacy", "Performance Overview"],
        index=0
    )
    
    # Get current anomalies and available symbols (cached for performance)
    if 'anomalies_df' not in st.session_state:
        st.session_state.anomalies_df = get_current_anomalies()
    if 'available_symbols' not in st.session_state:
        st.session_state.available_symbols = get_available_symbols()
    
    anomalies_df = st.session_state.anomalies_df
    available_symbols = st.session_state.available_symbols
    
    # Refresh button in sidebar
    if st.sidebar.button("Refresh Data"):
        if 'anomalies_df' in st.session_state:
            del st.session_state.anomalies_df
        if 'available_symbols' in st.session_state:
            del st.session_state.available_symbols
        st.rerun()
    
    # Route to appropriate page
    if page == "Performance Overview":
        create_performance_analysis_page()
    elif page == "High Conviction":
        render_greeks_based_page(anomalies_df, available_symbols)
    else:  # Legacy
        render_legacy_page(anomalies_df, available_symbols)


def render_greeks_based_page(anomalies_df: pd.DataFrame, available_symbols: list):
    """Render the high conviction interface using two-tier scoring."""
    st.markdown("## High Conviction Alerts")
    st.markdown("""
    **Tier 1 — Event scoring** (symbol-level): Volume anomaly (`>= 2.0`), Z-score (`>= 3.0`), Vol:OI ratio (`>= 1.2`), Magnitude (`>= $50K`).
    Alert fires when 3+ of 4 factors exceed thresholds. Filters: not bot-driven (<5% intraday move), not earnings-related.

    **Tier 2 — Contract selection**: Highest-volume tradeable contract ($0.05-$5.00, vol > 50, direction-aligned).

    **Enrichment**: Alerts include signal context — symbol novelty, recent news, SEC insider filings — to aid manual review.
    """)
    
    # Get high conviction alerts directly (not filtered by legacy score)
    hc_df = get_high_conviction_anomalies()
    
    # Symbol selection for Greeks-based view
    st.sidebar.markdown("---")
    st.sidebar.subheader("Symbol Selection")
    
    # Get high conviction symbols ordered by date/score
    if not hc_df.empty:
        hc_symbols = get_ordered_anomaly_symbols(hc_df)
    else:
        hc_symbols = []
    
    all_symbols = ['Overview'] + hc_symbols
    
    # Check for existing selection
    selected_symbol = st.session_state.get('greeks_selected_symbol', 'Overview')
    if selected_symbol not in all_symbols:
        selected_symbol = 'Overview'
    
    selected_symbol = st.sidebar.selectbox(
        "Select Symbol",
        all_symbols,
        index=all_symbols.index(selected_symbol) if selected_symbol in all_symbols else 0,
        key="greeks_selected_symbol"
    )
    
    # Date selector for symbol analysis
    selected_date = None
    if selected_symbol and selected_symbol != 'Overview':
        selected_date = st.sidebar.date_input(
            "Select Date",
            value=date.today(),
            max_value=date.today(),
            key="greeks_date"
        )
        if st.sidebar.button("Back to Overview", key="greeks_back"):
            # Use query params to avoid widget key conflict
            st.query_params.clear()
            st.rerun()
    
    # Main content
    if selected_symbol == 'Overview':
        # Show high conviction alerts overview
        if hc_df.empty:
            st.info("No high conviction alerts detected.")
            st.markdown("""
            High conviction alerts require an Event Score >= 3/4 based on:
            - Volume Score >= 2.0 (volume anomaly)
            - Z-Score >= 3.0 (statistical deviation)
            - Vol:OI Score >= 1.2 (fresh positioning)
            - Magnitude >= $50,000 (institutional scale)
            """)
        else:
            # Display alerts table
            st.subheader(f"Active High Conviction Alerts ({len(hc_df)})")
            
            display_df = hc_df[['event_date', 'symbol', 'direction', 'high_conviction_score', 
                               'recommended_option', 'otm_score', 'total_magnitude']].copy()
            display_df.columns = ['Date', 'Symbol', 'Direction', 'Event Score',
                                 'Recommended Option', 'OTM Score', 'Magnitude ($)']
            display_df['Magnitude ($)'] = display_df['Magnitude ($)'].apply(lambda x: f"${x:,.0f}")
            display_df['Direction'] = display_df['Direction'].apply(
                lambda x: "BULLISH" if x == 'call_heavy' else "BEARISH" if x == 'put_heavy' else x
            )
            display_df['OTM Score'] = display_df['OTM Score'].apply(lambda x: f"{x:.2f}")
            display_df['Event Score'] = display_df['Event Score'].apply(lambda x: f"{x}/4")
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            # Summary stats
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Alerts", len(hc_df))
            with col2:
                bullish = len(hc_df[hc_df['direction'] == 'call_heavy'])
                st.metric("Bullish", bullish)
            with col3:
                bearish = len(hc_df[hc_df['direction'] == 'put_heavy'])
                st.metric("Bearish", bearish)
            with col4:
                avg_score = hc_df['high_conviction_score'].mean()
                st.metric("Avg Event Score", f"{avg_score:.1f}/4")
    else:
        # Show symbol-specific analysis (unified view)
        symbol_anomaly_data = get_symbol_anomaly_data(selected_symbol, selected_date)
        
        if symbol_anomaly_data:
            create_symbol_analysis(selected_symbol, symbol_anomaly_data, selected_date)
        else:
            st.warning(f"No data for {selected_symbol} on {selected_date}")
            create_basic_symbol_analysis(selected_symbol, selected_date)
        
        # Contracts and heatmaps
        st.markdown("---")
        create_contracts_table(selected_symbol, selected_date)
        st.markdown("---")
        create_options_heatmaps(selected_symbol, selected_date)


def render_legacy_page(anomalies_df: pd.DataFrame, available_symbols: list):
    """Render the legacy composite score interface."""
    st.markdown("## Legacy Composite Score Alerts")
    st.markdown("""
    Original scoring system: Volume Anomaly + Volume:OI Ratio + OTM Concentration + Directional Bias + Time Pressure.
    **Alert threshold**: Score >= 7.5/10.0, Magnitude >= $20K
    """)
    
    # Symbol selection
    st.sidebar.markdown("---")
    st.sidebar.subheader("Symbol Selection")
    
    ordered_anomaly_symbols = get_ordered_anomaly_symbols(anomalies_df) if not anomalies_df.empty else []
    other_symbols = sorted([s for s in available_symbols if s not in ordered_anomaly_symbols])
    all_symbols = ['Overview'] + ordered_anomaly_symbols + other_symbols
    
    selected_symbol = st.session_state.get('legacy_selected_symbol', 'Overview')
    if selected_symbol not in all_symbols:
        selected_symbol = 'Overview'
    
    selected_symbol = st.sidebar.selectbox(
        "Select Symbol",
        all_symbols,
        index=all_symbols.index(selected_symbol) if selected_symbol in all_symbols else 0,
        key="legacy_selected_symbol"
    )
    
    selected_date = None
    if selected_symbol and selected_symbol != 'Overview':
        selected_date = st.sidebar.date_input(
            "Select Date",
            value=date.today(),
            max_value=date.today(),
            key="legacy_date"
        )
        if st.sidebar.button("Back to Overview", key="legacy_back"):
            st.session_state.legacy_selected_symbol = 'Overview'
            st.rerun()
    
    # Main content
    if selected_symbol == 'Overview':
        if not anomalies_df.empty:
            create_anomaly_summary_by_date(anomalies_df)
            st.markdown("---")
            create_performance_matrix()
        else:
            st.info("No anomalies detected meeting the threshold criteria.")
    else:
        symbol_anomaly_data = get_symbol_anomaly_data(selected_symbol, selected_date)
        
        if symbol_anomaly_data:
            create_symbol_analysis(selected_symbol, symbol_anomaly_data, selected_date)
        else:
            create_basic_symbol_analysis(selected_symbol, selected_date)
        
        st.markdown("---")
        create_contracts_table(selected_symbol, selected_date)
        st.markdown("---")
        create_options_heatmaps(selected_symbol, selected_date)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    **Detection Method**: Statistical Z-score analysis vs 90-day baseline
    **Legacy alert threshold**: Score >= 7.5/10.0, Magnitude >= $20K
    **Current system**: Use the High Conviction tab (event scoring, 3+/4 factors)
    """)


if __name__ == '__main__':
    main()
