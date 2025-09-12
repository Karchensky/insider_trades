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
        get_current_anomalies, get_symbol_history,
        create_anomaly_summary_table, create_anomaly_summary_by_date, create_symbol_analysis,
        get_available_symbols, create_options_heatmaps, create_contracts_table, create_basic_symbol_analysis, get_symbol_anomaly_data,
        create_performance_matrix
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
    
    # Get current anomalies and available symbols (cached for performance)
    if 'anomalies_df' not in st.session_state:
        st.session_state.anomalies_df = get_current_anomalies()
    if 'available_symbols' not in st.session_state:
        st.session_state.available_symbols = get_available_symbols()
    
    anomalies_df = st.session_state.anomalies_df
    available_symbols = st.session_state.available_symbols
    
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
    all_symbols = ['Overview'] + anomaly_symbols + other_symbols  # Show all available symbols
    
    # If we have a selected symbol from URL/session, make sure it's in the list
    if selected_symbol and selected_symbol not in all_symbols:
        all_symbols = [selected_symbol] + all_symbols
    
    selected_symbol = st.sidebar.selectbox(
        "Select Symbol for Analysis",
        all_symbols,
        index=all_symbols.index(selected_symbol) if selected_symbol in all_symbols else 0
    )
    
    # Date selector for analysis
    selected_date = None
    if selected_symbol and selected_symbol != 'Overview':
        selected_date = st.sidebar.date_input(
            "Select Date for Analysis",
            value=date.today(),
            max_value=date.today()
        )
    
    # Buttons
    if selected_symbol and selected_symbol != 'Overview':
        if st.sidebar.button("Return to Summary"):
            st.session_state.selected_symbol = 'Overview'
            st.rerun()
    
    if st.sidebar.button("Refresh Data"):
        # Clear cached data
        if 'anomalies_df' in st.session_state:
            del st.session_state.anomalies_df
        if 'available_symbols' in st.session_state:
            del st.session_state.available_symbols
        st.rerun()
    
    # Main content
    if not selected_symbol or selected_symbol == 'Overview':
        # Show anomaly overview
        if not anomalies_df.empty:
            create_anomaly_summary_by_date(anomalies_df)
            
            # Performance matrix
            st.markdown("---")
            create_performance_matrix()
            
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
        # Check if this symbol has anomalies for the selected date
        symbol_anomaly_data = get_symbol_anomaly_data(selected_symbol, selected_date)
        
        if symbol_anomaly_data:
            create_symbol_analysis(selected_symbol, symbol_anomaly_data, selected_date)
        else:
            # Show basic symbol analysis without anomaly data
            create_basic_symbol_analysis(selected_symbol, selected_date)
        
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