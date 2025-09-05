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
    create_anomaly_summary_table, create_symbol_analysis, create_anomaly_timeline_chart
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
    
    # Get current anomalies
    anomalies_df = get_current_anomalies()
    
    if not anomalies_df.empty:
        # Sidebar
        st.sidebar.header("Navigation")
        symbols = anomalies_df['symbol'].unique().tolist()
        selected_symbol = st.sidebar.selectbox(
            "Select Symbol for Deep Dive",
            ['Overview'] + symbols
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
                'composite_score': float(symbol_anomaly['score']),
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