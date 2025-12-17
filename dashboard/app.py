"""
Streamlit Dashboard for Natural Disaster Risk Platform

Interactive dashboard for visualizing risk assessments.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api_connectors import USGSConnector, NASAFIRMSConnector, NOAAWeatherConnector
from src.risk_scoring import RiskScorer

# Page configuration
st.set_page_config(
    page_title="Natural Disaster Risk Platform",
    page_icon="🌋",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize connectors
@st.cache_resource
def get_connectors():
    return {
        "usgs": USGSConnector(),
        "nasa": NASAFIRMSConnector(),
        "noaa": NOAAWeatherConnector(),
        "scorer": RiskScorer()
    }

connectors = get_connectors()

# Title and description
st.title(" Natural Disaster Risk Intelligence Platform")
st.markdown("**Real-Time Multi-Hazard Risk Assessment**")

# Sidebar
st.sidebar.header("Configuration")

# Location input
st.sidebar.subheader("📍 Location")
latitude = st.sidebar.number_input("Latitude", value=39.7392, min_value=-90.0, max_value=90.0, format="%.4f")
longitude = st.sidebar.number_input("Longitude", value=-104.9903, min_value=-180.0, max_value=180.0, format="%.4f")
radius_km = st.sidebar.slider("Search Radius (km)", min_value=100, max_value=1000, value=500, step=50)

# Quick location buttons
st.sidebar.subheader(" Quick Locations")
col1, col2 = st.sidebar.columns(2)
if col1.button("Denver, CO"):
    latitude, longitude = 39.7392, -104.9903
if col2.button("Los Angeles, CA"):
    latitude, longitude = 34.0522, -118.2437
if col1.button("Seattle, WA"):
    latitude, longitude = 47.6062, -122.3321
if col2.button("Phoenix, AZ"):
    latitude, longitude = 33.4484, -112.0740

# Analysis button
if st.sidebar.button(" Analyze Risk", type="primary"):
    with st.spinner("Fetching data from government APIs..."):
        
        # Fetch data
        earthquakes = connectors["usgs"].get_earthquakes_near_location(
            latitude, longitude, radius_km, days=365
        )
        
        wildfires = connectors["nasa"].get_wildfires_near_location(
            latitude, longitude, radius_deg=radius_km/111.0, days=30
        )
        
        weather_alerts = connectors["noaa"].get_alerts_near_location(
            latitude, longitude
        )
        
        # Calculate risk scores
        eq_risk = connectors["scorer"].calculate_earthquake_risk(
            earthquakes, latitude, longitude, radius_km=radius_km
        )
        
        fire_risk = connectors["scorer"].calculate_wildfire_risk(
            wildfires, latitude, longitude, radius_km=radius_km
        )
        
        weather_risk = connectors["scorer"].calculate_weather_alert_risk(weather_alerts)
        
        composite = connectors["scorer"].calculate_composite_risk(
            eq_risk, fire_risk, weather_risk
        )
        
        # Store in session state
        st.session_state.composite = composite
        st.session_state.earthquakes = earthquakes
        st.session_state.wildfires = wildfires
        st.session_state.weather_alerts = weather_alerts
        st.session_state.analysis_complete = True

# Main content
if st.session_state.get("analysis_complete", False):
    composite = st.session_state.composite
    
    # Risk Score Cards
    st.subheader(" Risk Assessment Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Composite Score",
            value=f"{composite['composite_score']:.1f}/100",
            delta=f"{composite['risk_level']}"
        )
    
    with col2:
        st.metric(
            label="Earthquake Risk",
            value=f"{composite['earthquake_score']:.1f}/100"
        )
    
    with col3:
        st.metric(
            label="Wildfire Risk",
            value=f"{composite['wildfire_score']:.1f}/100"
        )
    
    with col4:
        st.metric(
            label="Weather Risk",
            value=f"{composite['severe_weather_score']:.1f}/100"
        )
    
    # Risk Level Indicator
    risk_level = composite['risk_level']
    if risk_level == "Extreme":
        st.error(f"⚠️ **{risk_level} Risk** - Immediate attention required")
    elif risk_level == "High":
        st.warning(f"⚠️ **{risk_level} Risk** - Close monitoring recommended")
    elif risk_level == "Moderate":
        st.info(f"ℹ️ **{risk_level} Risk** - Standard precautions advised")
    else:
        st.success(f"✅ **{risk_level} Risk** - Minimal concern")
    
    st.markdown("---")
    
    # Detailed Data
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "🌍 Earthquakes", "🔥 Wildfires", "⛈️ Weather Alerts"])
    
    with tab1:
        st.subheader("Risk Breakdown")
        
        # Create bar chart
        hazards = ["Earthquake", "Wildfire", "Weather", "Flood", "Heat"]
        scores = [
            composite['earthquake_score'],
            composite['wildfire_score'],
            composite['severe_weather_score'],
            composite['flood_score'],
            composite['extreme_heat_score']
        ]
        
        fig = px.bar(
            x=hazards,
            y=scores,
            labels={"x": "Hazard Type", "y": "Risk Score (0-100)"},
            title="Individual Hazard Risk Scores",
            color=scores,
            color_continuous_scale="Reds"
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        
        # Data sources
        st.subheader("Data Sources")
        col1, col2, col3 = st.columns(3)
        col1.metric("Earthquakes Analyzed", len(st.session_state.earthquakes))
        col2.metric("Active Fires Detected", len(st.session_state.wildfires))
        col3.metric("Weather Alerts", len(st.session_state.weather_alerts))
    
    with tab2:
        st.subheader("Earthquake Data")
        earthquakes = st.session_state.earthquakes
        
        if not earthquakes.empty:
            st.dataframe(
                earthquakes[["time", "magnitude", "depth_km", "place"]].head(20),
                use_container_width=True
            )
            
            # Magnitude distribution
            fig = px.histogram(
                earthquakes,
                x="magnitude",
                nbins=20,
                title="Earthquake Magnitude Distribution",
                labels={"magnitude": "Magnitude", "count": "Frequency"}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No earthquakes detected in the specified area and time range.")
    
    with tab3:
        st.subheader("Wildfire Data")
        wildfires = st.session_state.wildfires
        
        if not wildfires.empty:
            st.dataframe(
                wildfires[["acq_datetime", "latitude", "longitude", "fire_radiative_power", "confidence"]].head(20),
                use_container_width=True
            )
            
            # Fire intensity over time
            fig = px.scatter(
                wildfires,
                x="acq_datetime",
                y="fire_radiative_power",
                color="confidence",
                title="Fire Radiative Power Over Time",
                labels={
                    "acq_datetime": "Detection Time",
                    "fire_radiative_power": "Fire Radiative Power (MW)",
                    "confidence": "Confidence"
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No active wildfires detected in the specified area.")
    
    with tab4:
        st.subheader("Weather Alerts")
        alerts = st.session_state.weather_alerts
        
        if not alerts.empty:
            for _, alert in alerts.iterrows():
                severity = alert.get("severity", "Unknown")
                if severity == "Extreme":
                    st.error(f"**{alert['event']}** - {alert['headline']}")
                elif severity == "Severe":
                    st.warning(f"**{alert['event']}** - {alert['headline']}")
                else:
                    st.info(f"**{alert['event']}** - {alert['headline']}")
                
                with st.expander("Details"):
                    st.write(alert.get("description", "No description available"))
        else:
            st.success("No active weather alerts for this location.")

else:
    # Instructions
    st.info("👈 Enter a location in the sidebar and click **Analyze Risk** to begin.")
    
    st.markdown("""
    ### About This Platform
    
    This platform provides real-time natural disaster risk assessment by integrating data from:
    
    - **USGS**: Earthquake monitoring and historical seismicity
    - **NASA FIRMS**: Active wildfire detection from satellite imagery
    - **NOAA**: Weather alerts and severe weather warnings
    
    The composite risk score combines multiple hazard types with configurable weights to provide 
    a comprehensive risk assessment for any location.
    
    ### How to Use
    
    1. Enter latitude and longitude (or use quick location buttons)
    2. Adjust the search radius
    3. Click "Analyze Risk" to fetch data and calculate scores
    4. Review the detailed breakdown in each tab
    """)

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("""
**Natural Disaster Risk Platform**  
Version 1.0.0  
Data Sources: USGS, NASA FIRMS, NOAA
""")