"""
USGS Earthquake API Connector
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class USGSConnector:
    """Connector for USGS Earthquake API"""
    
    BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    
    def __init__(self):
        self.session = requests.Session()
    
    def get_earthquakes(
        self,
        start_date=None,
        end_date=None,
        min_magnitude=2.5,
        latitude=None,
        longitude=None,
        max_radius_km=500
    ):
        """Fetch earthquake data from USGS API"""
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        params = {
            "format": "geojson",
            "starttime": start_date,
            "endtime": end_date,
            "minmagnitude": min_magnitude,
            "limit": 1000
        }
        
        if latitude and longitude:
            params["latitude"] = latitude
            params["longitude"] = longitude
            params["maxradiuskm"] = max_radius_km
        
        try:
            logger.info(f"Fetching earthquakes from {start_date} to {end_date}")
            response = self.session.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            features = data.get("features", [])
            
            if not features:
                return pd.DataFrame()
            
            records = []
            for feature in features:
                props = feature["properties"]
                coords = feature["geometry"]["coordinates"]
                
                records.append({
                    "id": feature["id"],
                    "time": pd.to_datetime(props["time"], unit="ms"),
                    "latitude": coords[1],
                    "longitude": coords[0],
                    "depth_km": coords[2],
                    "magnitude": props["mag"],
                    "place": props.get("place"),
                    "type": props.get("type")
                })
            
            df = pd.DataFrame(records)
            logger.info(f"Retrieved {len(df)} earthquake records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching earthquake data: {e}")
            return pd.DataFrame()
    
    def get_earthquakes_near_location(
        self,
        latitude: float,
        longitude: float,
        max_radius_km: float = 500,
        days: int = 365
    ) -> pd.DataFrame:
        """
        Get earthquakes near a specific location
        
        Args:
            latitude: Center latitude
            longitude: Center longitude
            max_radius_km: Maximum radius in km from center point
            days: Number of days to look back
            
        Returns:
            DataFrame with earthquake data
        """
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        return self.get_earthquakes(
            start_date=start_date,
            latitude=latitude,
            longitude=longitude,
            max_radius_km=max_radius_km,
            min_magnitude=2.0
        )


if __name__ == "__main__":
    connector = USGSConnector()
    df = connector.get_earthquakes(min_magnitude=4.5)
    
    if not df.empty:
        print(f"\n✓ Retrieved {len(df)} earthquakes")
        print(df[["time", "magnitude", "place"]].head())
    else:
        print("✗ No earthquake data retrieved")