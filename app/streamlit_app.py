#!/usr/bin/env python3
"""
Insider Trading Detection Dashboard

Comprehensive Streamlit application for analyzing and investigating high-conviction
insider trading anomalies detected by the system.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.core.connection import db
from dashboard_functions import (
    get_current_anomalies, get_symbol_history, get_anomaly_timeline,
    create_anomaly_summary_table, create_symbol_analysis, create_anomaly_timeline_chart,
    get_available_symbols, create_options_heatmaps
)

# Page configuration
st.set_page_config(
    page_title="Insider Trading Detection Dashboard",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """Main Streamlit application."""
    st.title("Insider Trading Detection Dashboard")
    st.markdown("**High-Conviction Statistical Anomaly Analysis**")
    
    # Sidebar Configuration
    st.sidebar.header("Navigation")
    
    # Mode Selection
    mode = st.sidebar.radio("Mode", ["Anomalies", "Symbol Search"])
    
    if mode == "Anomalies":
        # Get current anomalies
        anomalies_df = get_current_anomalies()
        
        if not anomalies_df.empty:
            symbols = anomalies_df['symbol'].unique().tolist()
            selected_symbol = st.sidebar.selectbox(
                "Select Symbol for Deep Dive",
                ['Overview'] + symbols
            )
            
            # Date selector for heatmaps (when not in Overview mode)
            selected_date = None
            if selected_symbol != 'Overview':
                st.sidebar.subheader("Heatmap Options")
                selected_date = st.sidebar.date_input(
                    "Select Date for Heatmaps",
                    value=date.today(),
                    max_value=date.today()
                )
            
            if st.sidebar.button("Refresh Data"):
                st.rerun()
            
            # Main content
            if selected_symbol == 'Overview':
                create_anomaly_summary_table(anomalies_df)
                timeline_df = get_anomaly_timeline()
                create_anomaly_timeline_chart(timeline_df)
            else:
                symbol_anomaly = anomalies_df[anomalies_df['symbol'] == selected_symbol].iloc[0]
                # Build anomaly data from new table structure
                anomaly_data = {
                    'composite_score': float(symbol_anomaly['total_score']),
                    'details': {
                        'volume_score': float(symbol_anomaly.get('volume_score', 0)),
                        'otm_call_score': float(symbol_anomaly.get('otm_score', 0)),  # Note: function expects 'otm_call_score'
                        'directional_score': float(symbol_anomaly.get('directional_score', 0)),
                        'time_pressure_score': float(symbol_anomaly.get('time_score', 0)),  # Note: function expects 'time_pressure_score'
                        'call_volume': int(symbol_anomaly.get('call_volume', 0)),
                        'put_volume': int(symbol_anomaly.get('put_volume', 0)),
                        'total_volume': int(symbol_anomaly.get('total_volume', 0)),
                        'call_baseline_avg': float(symbol_anomaly.get('call_baseline_avg', 0)),
                        'put_baseline_avg': float(symbol_anomaly.get('put_baseline_avg', 0)),
                        'call_multiplier': float(symbol_anomaly.get('call_multiplier', 0)),
                        'pattern_description': symbol_anomaly.get('pattern_description', 'Unusual trading pattern'),
                        'z_score': float(symbol_anomaly.get('z_score', 0))
                    }
                }
                create_symbol_analysis(selected_symbol, anomaly_data)
                
                # Add heatmaps section
                st.markdown("---")
                create_options_heatmaps(selected_symbol, selected_date)
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
    
    else:  # Symbol Search mode
        st.sidebar.subheader("Symbol Search")
        
        # Get available symbols
        available_symbols = get_available_symbols()
        
        # Symbol search
        search_term = st.sidebar.text_input("Search Symbol", "").upper()
        
        if search_term:
            matching_symbols = [s for s in available_symbols if search_term in s]
            if matching_symbols:
                selected_symbol = st.sidebar.selectbox("Select Symbol", matching_symbols)
                
                # Date selector for heatmaps
                st.sidebar.subheader("Analysis Options")
                selected_date = st.sidebar.date_input(
                    "Select Date for Analysis",
                    value=date.today(),
                    max_value=date.today()
                )
                
                # Display symbol analysis
                st.subheader(f"Analysis for {selected_symbol}")
                
                # Check if this symbol has anomalies
                anomalies_df = get_current_anomalies()
                if not anomalies_df.empty and selected_symbol in anomalies_df['symbol'].values:
                    st.info("This symbol has current anomalies! Check the Anomalies tab for detailed analysis.")
                
                # Show heatmaps
                create_options_heatmaps(selected_symbol, selected_date)
                
                # Show historical data
                history = get_symbol_history(selected_symbol)
                if not history['stock'].empty:
                    st.subheader("Price History")
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=history['stock']['date'],
                        y=history['stock']['close'],
                        mode='lines',
                        name='Close Price'
                    ))
                    fig.update_layout(title=f"{selected_symbol} Price History", xaxis_title="Date", yaxis_title="Price")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"No symbols found matching '{search_term}'")
        else:
            st.info("Enter a symbol to search for analysis")
            st.write(f"Available symbols: {len(available_symbols)} total")
    
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