"""
NASA FIRMS Wildfire API Connector

Fetches active fire data from NASA's Fire Information for Resource Management System (FIRMS).
API Documentation: https://firms.modaps.eosdis.nasa.gov/api/area/
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NASAFIRMSConnector:
    """Connector for NASA FIRMS Active Fire API"""
    
    BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
    
    # Common US State Boundaries (west, south, east, north)
    US_STATE_BOUNDS = {
        "California": (-124.5, 32.5, -114.0, 42.0),
        "Colorado": (-109.1, 37.0, -102.0, 41.0),
        "Oregon": (-124.6, 42.0, -116.5, 46.3),
        "Washington": (-124.8, 45.5, -116.9, 49.0),
        "Montana": (-116.1, 44.4, -104.0, 49.0),
        "Idaho": (-117.2, 42.0, -111.0, 49.0),
        "Nevada": (-120.0, 35.0, -114.0, 42.0),
        "Arizona": (-114.8, 31.3, -109.0, 37.0),
        "New Mexico": (-109.1, 31.3, -103.0, 37.0),
        "Texas": (-106.7, 25.8, -93.5, 36.5),
        "Wyoming": (-111.1, 41.0, -104.0, 45.0),
        "Utah": (-114.1, 37.0, -109.0, 42.0),
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize NASA FIRMS connector
        
        Args:
            api_key: NASA FIRMS API key. If None, uses DEMO_KEY (limited to US only)
        """
        self.api_key = api_key or os.getenv("NASA_FIRMS_API_KEY", "DEMO_KEY")
        
        if self.api_key == "DEMO_KEY":
            logger.warning("Using DEMO_KEY - limited to US data only. Get free API key at: https://firms.modaps.eosdis.nasa.gov/api/area/")
        
        self.session = requests.Session()
    
    def get_wildfires(
        self,
        area: tuple,
        source: str = "VIIRS_NOAA20_NRT",
        days: int = 1
    ) -> pd.DataFrame:
        """
        Fetch active fire data for a specified area
        
        Args:
            area: Tuple of (west, south, east, north) coordinates
            source: Data source. Options:
                - "VIIRS_NOAA20_NRT" (recommended, 375m resolution)
                - "MODIS_NRT" (1km resolution)
                - "VIIRS_SNPP_NRT" (375m resolution)
            days: Number of days of data (1-10)
            
        Returns:
            DataFrame with wildfire data
        """
        
        west, south, east, north = area
        
        url = f"{self.BASE_URL}/{self.api_key}/{source}/{west},{south},{east},{north}/{days}"
        
        try:
            logger.info(f"Fetching wildfires for area {area} (last {days} days)")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Check if response is CSV
            content_type = response.headers.get("Content-Type", "")
            if "text/csv" not in content_type and "text/plain" not in content_type:
                logger.error(f"Unexpected response format: {content_type}")
                return pd.DataFrame()
            
            # Check for error messages in response
            if "error" in response.text.lower() or len(response.text) < 50:
                logger.warning(f"API returned error or no data: {response.text[:100]}")
                return pd.DataFrame()
            
            # Read CSV from response
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            if df.empty:
                logger.warning("No wildfire data returned")
                return pd.DataFrame()
            
            # Parse datetime
            df["acq_datetime"] = pd.to_datetime(
                df["acq_date"] + " " + df["acq_time"].astype(str).str.zfill(4), 
                format="%Y-%m-%d %H%M"
            )
            
            # Standardize column names
            column_mapping = {
                "latitude": "latitude",
                "longitude": "longitude",
                "bright_ti4": "brightness_kelvin",
                "bright_ti5": "brightness_ti5_kelvin",
                "scan": "scan_size_km",
                "track": "track_size_km",
                "frp": "fire_radiative_power",
                "confidence": "confidence",
                "daynight": "day_night",
                "type": "fire_type"
            }
            
            # Rename columns that exist
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
            
            logger.info(f"Retrieved {len(df)} active fire detections")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching wildfire data: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error processing wildfire data: {e}")
            return pd.DataFrame()
    
    def get_wildfires_by_state(
        self,
        state_name: str,
        days: int = 7,
        source: str = "VIIRS_NOAA20_NRT"
    ) -> pd.DataFrame:
        """
        Get wildfires for a US state
        
        Args:
            state_name: Name of the state (e.g., "California", "Colorado")
            days: Number of days of data (1-10)
            source: Data source (default: VIIRS_NOAA20_NRT)
            
        Returns:
            DataFrame with wildfire data
        """
        if state_name not in self.US_STATE_BOUNDS:
            logger.error(f"State '{state_name}' not found. Available states: {list(self.US_STATE_BOUNDS.keys())}")
            return pd.DataFrame()
        
        area = self.US_STATE_BOUNDS[state_name]
        return self.get_wildfires(area, source=source, days=days)
    
    def get_wildfires_near_location(
        self,
        latitude: float,
        longitude: float,
        radius_deg: float = 2.0,
        days: int = 7,
        source: str = "VIIRS_NOAA20_NRT"
    ) -> pd.DataFrame:
        """
        Get wildfires near a specific location
        
        Args:
            latitude: Center latitude
            longitude: Center longitude
            radius_deg: Radius in degrees (roughly 111km per degree at equator)
            days: Number of days of data (1-10)
            source: Data source
            
        Returns:
            DataFrame with wildfire data
        """
        area = (
            longitude - radius_deg,  # west
            latitude - radius_deg,   # south
            longitude + radius_deg,  # east
            latitude + radius_deg    # north
        )
        return self.get_wildfires(area, source=source, days=days)


if __name__ == "__main__":
    # Test the connector
    print("\n" + "="*60)
    print("NASA FIRMS WILDFIRE CONNECTOR TEST")
    print("="*60 + "\n")
    
    connector = NASAFIRMSConnector()
    
    # Test 1: Get wildfires in California for the past 7 days
    print("Test 1: Fetching wildfires in California (past 7 days)...")
    df_ca = connector.get_wildfires_by_state("California", days=7)
    
    if not df_ca.empty:
        print(f"\n✓ Retrieved {len(df_ca)} active fire detections in California")
        print(f"\nRecent wildfires:")
        print(df_ca[["acq_datetime", "latitude", "longitude", "brightness_kelvin", "confidence"]].head(10))
        print(f"\nFire Radiative Power range: {df_ca['fire_radiative_power'].min():.1f} - {df_ca['fire_radiative_power'].max():.1f} MW")
        print(f"Average FRP: {df_ca['fire_radiative_power'].mean():.1f} MW")
    else:
        print("✓ No active wildfires in California (or using DEMO_KEY which may have restrictions)")
    
    # Test 2: Get wildfires near Denver, CO
    print("\n" + "-"*60)
    print("Test 2: Fetching wildfires near Denver, CO...")
    df_denver = connector.get_wildfires_near_location(39.7392, -104.9903, radius_deg=3.0, days=30)
    
    if not df_denver.empty:
        print(f"\n✓ Retrieved {len(df_denver)} fire detections near Denver (within ~330km)")
        print(df_denver[["acq_datetime", "latitude", "longitude", "fire_radiative_power"]].head(5))
    else:
        print("✓ No active wildfires near Denver in the past 30 days")
    
    # Test 3: Summary across multiple states
    print("\n" + "-"*60)
    print("Test 3: Active fire summary across western US states...")
    
    western_states = ["California", "Colorado", "Oregon", "Washington", "Montana"]
    for state in western_states:
        df = connector.get_wildfires_by_state(state, days=7)
        print(f"  {state:15s}: {len(df):3d} active fires")
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)
    
    print("\nNote: If using DEMO_KEY, you'll only see US data.")
    print("For global coverage, get a free API key at:")
    print("https://firms.modaps.eosdis.nasa.gov/api/area/")