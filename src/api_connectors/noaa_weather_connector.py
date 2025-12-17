"""
NOAA Weather API Connector

Fetches weather data and alerts from the National Weather Service API.
API Documentation: https://www.weather.gov/documentation/services-web-api
"""

import requests
import pandas as pd
from typing import Optional, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NOAAWeatherConnector:
    """Connector for NOAA National Weather Service API"""
    
    BASE_URL = "https://api.weather.gov"
    
    def __init__(self):
        self.session = requests.Session()
        # Required User-Agent header for NOAA API
        self.session.headers.update({
            "User-Agent": "(DisasterRiskPlatform, contact@example.com)"
        })
    
    def get_point_metadata(self, latitude: float, longitude: float) -> Optional[Dict]:
        """
        Get metadata for a specific lat/lon point
        
        Returns zone, county, forecast office, grid coordinates, etc.
        """
        url = f"{self.BASE_URL}/points/{latitude},{longitude}"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching point metadata: {e}")
            return None
    
    def get_forecast(self, latitude: float, longitude: float) -> Optional[Dict]:
        """Get 7-day forecast for a location"""
        
        # First get point metadata to get forecast URL
        point_data = self.get_point_metadata(latitude, longitude)
        
        if not point_data:
            return None
        
        forecast_url = point_data["properties"]["forecast"]
        
        try:
            response = self.session.get(forecast_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching forecast: {e}")
            return None
    
    def get_active_alerts(
        self,
        state: Optional[str] = None,
        zone: Optional[str] = None,
        latitude: Optional[float] = None,
        longitude: Optional[float] = None,
        severity: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get active weather alerts
        
        Args:
            state: Two-letter state code (e.g., "CO")
            zone: Specific zone ID
            latitude: Filter by point
            longitude: Filter by point  
            severity: Filter by severity (Extreme, Severe, Moderate, Minor, Unknown)
            
        Returns:
            DataFrame with alert data
        """
        
        url = f"{self.BASE_URL}/alerts/active"
        params = {}
        
        if state:
            params["area"] = state
        if zone:
            params["zone"] = zone
        if latitude and longitude:
            params["point"] = f"{latitude},{longitude}"
        if severity:
            params["severity"] = severity
        
        try:
            logger.info(f"Fetching active weather alerts (params: {params})")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            features = data.get("features", [])
            
            if not features:
                logger.info("No active alerts")
                return pd.DataFrame()
            
            # Parse into DataFrame
            records = []
            for feature in features:
                props = feature["properties"]
                
                record = {
                    "id": props.get("id"),
                    "event": props.get("event"),
                    "severity": props.get("severity"),
                    "certainty": props.get("certainty"),
                    "urgency": props.get("urgency"),
                    "headline": props.get("headline"),
                    "description": props.get("description"),
                    "instruction": props.get("instruction"),
                    "response": props.get("response"),
                    "onset": pd.to_datetime(props.get("onset")) if props.get("onset") else None,
                    "expires": pd.to_datetime(props.get("expires")) if props.get("expires") else None,
                    "sent": pd.to_datetime(props.get("sent")) if props.get("sent") else None,
                    "status": props.get("status"),
                    "message_type": props.get("messageType"),
                    "category": props.get("category"),
                    "sender": props.get("senderName"),
                    "area_desc": props.get("areaDesc"),
                }
                records.append(record)
            
            df = pd.DataFrame(records)
            logger.info(f"Retrieved {len(df)} active alerts")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching alerts: {e}")
            return pd.DataFrame()
    
    def get_severe_alerts(self, state: Optional[str] = None) -> pd.DataFrame:
        """Get only severe/extreme alerts"""
        df_extreme = self.get_active_alerts(state=state, severity="Extreme")
        df_severe = self.get_active_alerts(state=state, severity="Severe")
        
        if df_extreme.empty and df_severe.empty:
            return pd.DataFrame()
        elif df_extreme.empty:
            return df_severe
        elif df_severe.empty:
            return df_extreme
        else:
            return pd.concat([df_extreme, df_severe], ignore_index=True)
    
    def get_alerts_near_location(
        self,
        latitude: float,
        longitude: float
    ) -> pd.DataFrame:
        """Get all alerts affecting a specific location"""
        return self.get_active_alerts(latitude=latitude, longitude=longitude)


if __name__ == "__main__":
    # Test the connector
    print("\n" + "="*60)
    print("NOAA WEATHER CONNECTOR TEST")
    print("="*60 + "\n")
    
    connector = NOAAWeatherConnector()
    
    # Test 1: Get alerts for Colorado
    print("Test 1: Fetching active weather alerts for Colorado...")
    df_co = connector.get_active_alerts(state="CO")
    
    if not df_co.empty:
        print(f"\n✓ Retrieved {len(df_co)} active alerts in Colorado")
        print("\nActive alerts:")
        print(df_co[["event", "severity", "urgency", "area_desc"]].head())
    else:
        print("✓ No active alerts in Colorado (which is good!)")
    
    # Test 2: Get forecast for Denver
    print("\n" + "-"*60)
    print("Test 2: Fetching forecast for Denver, CO...")
    forecast = connector.get_forecast(39.7392, -104.9903)
    
    if forecast:
        periods = forecast["properties"]["periods"]
        print(f"\n✓ Retrieved {len(periods)} forecast periods")
        print(f"\nNext 3 periods:")
        for period in periods[:3]:
            print(f"  {period['name']}: {period['shortForecast']}, {period['temperature']}°{period['temperatureUnit']}")
    else:
        print("✗ Could not retrieve forecast")
    
    # Test 3: Check severe alerts across multiple states
    print("\n" + "-"*60)
    print("Test 3: Checking for severe/extreme alerts in western states...")
    
    states = ["CA", "CO", "OR", "WA", "MT", "ID", "NV", "AZ", "NM"]
    for state in states:
        df = connector.get_severe_alerts(state=state)
        if not df.empty:
            print(f"  {state}: {len(df)} severe alerts - {', '.join(df['event'].unique())}")
        else:
            print(f"  {state}: No severe alerts")
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)
    