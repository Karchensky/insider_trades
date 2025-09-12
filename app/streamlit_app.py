#!/usr/bin/env python3
"""
Insider Trading Detection Dashboard

Comprehensive Streamlit application for analyzing and investigating high-conviction
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
        get_current_anomalies, get_symbol_history, get_anomaly_timeline,
        create_anomaly_summary_table, create_anomaly_summary_by_date, create_symbol_analysis, create_anomaly_timeline_chart,
        get_available_symbols, create_options_heatmaps, create_contracts_table, create_basic_symbol_analysis
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
    st.markdown("**High-Conviction Statistical Anomaly Analysis**")
    
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
        - Go to your Streamlit.io app → Settings → Secrets and add your database URL
        
        **For Local Development:**
        - Ensure your `.env` file contains the correct `SUPABASE_DB_URL`
        - Verify your database credentials are correct
        """)
        return
    
    # Sidebar Configuration
    st.sidebar.header("Navigation")
    
    # Get current anomalies and available symbols
    anomalies_df = get_current_anomalies()
    available_symbols = get_available_symbols()
    
    # Check for symbol selection from URL parameters or session state
    if 'symbol' in st.query_params:
        selected_symbol = st.query_params['symbol']
    elif 'selected_symbol' in st.session_state:
        selected_symbol = st.session_state.selected_symbol
    else:
        selected_symbol = None
    
    # Unified symbol selection
    st.sidebar.subheader("Symbol Selection")
    
    # Show anomaly symbols first, then others
    anomaly_symbols = anomalies_df['symbol'].unique().tolist() if not anomalies_df.empty else []
    other_symbols = [s for s in available_symbols if s not in anomaly_symbols]
    all_symbols = ['Overview'] + anomaly_symbols + other_symbols[:50]  # Limit to first 50 others for performance
    
    # If we have a selected symbol from URL/session, make sure it's in the list
    if selected_symbol and selected_symbol not in all_symbols:
        all_symbols = [selected_symbol] + all_symbols
    
    selected_symbol = st.sidebar.selectbox(
        "Select Symbol for Analysis",
        all_symbols,
        index=all_symbols.index(selected_symbol) if selected_symbol in all_symbols else 0
    )
    
    # Return to Summary button
    if selected_symbol and selected_symbol != 'Overview':
        if st.sidebar.button("Return to Summary"):
            st.session_state.selected_symbol = 'Overview'
            st.rerun()
    
    # Date selector for analysis
    selected_date = None
    if selected_symbol and selected_symbol != 'Overview':
        st.sidebar.subheader("Analysis Options")
        selected_date = st.sidebar.date_input(
            "Select Date for Analysis",
            value=date.today(),
            max_value=date.today()
        )
    
    if st.sidebar.button("Refresh Data"):
        st.rerun()
    
    # Main content
    if not selected_symbol or selected_symbol == 'Overview':
        # Show anomaly overview
        if not anomalies_df.empty:
            create_anomaly_summary_by_date(anomalies_df)
            timeline_df = get_anomaly_timeline()
            create_anomaly_timeline_chart(timeline_df)
        else:
            st.info("No high-conviction anomalies detected in the past 7 days.")
            st.markdown("""
            ### What this means:
            - Market activity is within normal statistical ranges
            - No unusual insider trading patterns detected
            - System is functioning correctly and monitoring continuously
            
            ### Next steps:
            - Check back during or after market hours
            - Anomalies are detected every 15 minutes during trading hours
            - Email notifications are sent automatically when anomalies are found
            """)
    else:
        # Show symbol analysis
        st.subheader(f"Analysis for {selected_symbol}")
        
        # Check if this symbol has anomalies
        if not anomalies_df.empty and selected_symbol in anomalies_df['symbol'].values:
            symbol_anomaly = anomalies_df[anomalies_df['symbol'] == selected_symbol].iloc[0]
            # Build anomaly data from new table structure
            anomaly_data = {
                'composite_score': float(symbol_anomaly['total_score']),
                'details': {
                    'volume_score': float(symbol_anomaly.get('volume_score', 0)),
                    'open_interest_score': float(symbol_anomaly.get('open_interest_score', 0)),
                    'otm_score': float(symbol_anomaly.get('otm_score', 0)),
                    'directional_score': float(symbol_anomaly.get('directional_score', 0)),
                    'time_score': float(symbol_anomaly.get('time_score', 0)),
                    'call_volume': int(symbol_anomaly.get('call_volume', 0)),
                    'put_volume': int(symbol_anomaly.get('put_volume', 0)),
                    'total_volume': int(symbol_anomaly.get('total_volume', 0)),
                    'call_baseline_avg': float(symbol_anomaly.get('call_baseline_avg', 0)),
                    'put_baseline_avg': float(symbol_anomaly.get('put_baseline_avg', 0)),
                    'call_multiplier': float(symbol_anomaly.get('call_multiplier', 0)),
                    'current_open_interest': int(symbol_anomaly.get('open_interest', 0)),
                    'prior_open_interest': int(symbol_anomaly.get('prior_open_interest', 0)),
                    'open_interest_multiplier': float(symbol_anomaly.get('open_interest_change', 0)),
                    'pattern_description': symbol_anomaly.get('pattern_description', 'Unusual trading pattern'),
                    'z_score': float(symbol_anomaly.get('z_score', 0))
                }
            }
            create_symbol_analysis(selected_symbol, anomaly_data)
        else:
            # Show basic symbol analysis without anomaly data
            create_basic_symbol_analysis(selected_symbol)
        
        # Add contracts table section
        st.markdown("---")
        create_contracts_table(selected_symbol, selected_date)
        
        # Add heatmaps section
        st.markdown("---")
        create_options_heatmaps(selected_symbol, selected_date)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    **System Status**: Active monitoring every 15 minutes during market hours  
    **Alert Threshold**: Score ≥ 7.0/10.0 (high-conviction only)  
    **Detection Method**: Statistical Z-score analysis vs 30-day baseline  
    
    *This system is for informational purposes only. Always conduct proper due diligence.*
    """)

if __name__ == '__main__':
    main()