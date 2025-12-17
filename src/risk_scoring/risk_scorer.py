"""
Risk Scoring Module

Calculates composite risk scores for locations based on multiple hazard types.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RiskScorer:
    """Calculate composite risk scores for natural disasters"""
    
    # Default weights for each hazard type (must sum to 1.0)
    DEFAULT_WEIGHTS = {
        "earthquake": 0.25,
        "wildfire": 0.30,
        "severe_weather": 0.20,
        "flood": 0.15,
        "extreme_heat": 0.10
    }
    
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        """
        Initialize risk scorer
        
        Args:
            weights: Custom weights for each hazard type. If None, uses defaults.
        """
        self.weights = weights if weights is not None else self.DEFAULT_WEIGHTS
        
        # Validate weights sum to 1.0
        total_weight = sum(self.weights.values())
        if not np.isclose(total_weight, 1.0):
            logger.warning(f"Weights sum to {total_weight:.2f}, normalizing to 1.0")
            self.weights = {k: v/total_weight for k, v in self.weights.items()}
    
    def calculate_earthquake_risk(
        self,
        earthquake_df: pd.DataFrame,
        latitude: float,
        longitude: float,
        radius_km: float = 500,
        lookback_years: int = 10
    ) -> float:
        """
        Calculate earthquake risk score (0-100) for a location
        
        Based on:
        - Historical earthquake frequency
        - Maximum magnitude
        - Proximity to earthquakes
        
        Returns:
            Risk score 0-100 (higher = more risk)
        """
        
        if earthquake_df.empty:
            return 0.0
        
        # Filter to recent earthquakes
        cutoff_date = datetime.now() - timedelta(days=lookback_years * 365)
        recent_eq = earthquake_df[earthquake_df["time"] >= cutoff_date]
        
        if recent_eq.empty:
            return 0.0
        
        # Calculate distance to each earthquake
        recent_eq = recent_eq.copy()
        recent_eq["distance_km"] = self._haversine_distance(
            latitude, longitude,
            recent_eq["latitude"].values, recent_eq["longitude"].values
        )
        
        # Filter to earthquakes within radius
        nearby_eq = recent_eq[recent_eq["distance_km"] <= radius_km]
        
        if nearby_eq.empty:
            return 0.0
        
        # Calculate risk factors
        num_earthquakes = len(nearby_eq)
        max_magnitude = nearby_eq["magnitude"].max()
        
        # Normalize factors
        frequency_score = min(num_earthquakes / 50.0, 1.0) * 100  # 50+ earthquakes = max
        magnitude_score = min(max_magnitude / 7.0, 1.0) * 100     # Mag 7+ = max
        
        # Weighted combination
        risk_score = 0.6 * frequency_score + 0.4 * magnitude_score
        
        return min(risk_score, 100.0)
    
    def calculate_wildfire_risk(
        self,
        wildfire_df: pd.DataFrame,
        latitude: float,
        longitude: float,
        radius_km: float = 100,
        lookback_days: int = 30
    ) -> float:
        """
        Calculate wildfire risk score (0-100) for a location
        
        Based on:
        - Active fire detections
        - Fire intensity (radiative power)
        - Proximity to fires
        
        Returns:
            Risk score 0-100 (higher = more risk)
        """
        
        if wildfire_df.empty:
            return 0.0
        
        # Filter to recent fires
        cutoff_date = datetime.now() - timedelta(days=lookback_days)
        recent_fires = wildfire_df[wildfire_df["acq_datetime"] >= cutoff_date]
        
        if recent_fires.empty:
            return 0.0
        
        # Calculate distance to each fire
        recent_fires = recent_fires.copy()
        recent_fires["distance_km"] = self._haversine_distance(
            latitude, longitude,
            recent_fires["latitude"].values, recent_fires["longitude"].values
        )
        
        # Filter to fires within radius
        nearby_fires = recent_fires[recent_fires["distance_km"] <= radius_km]
        
        if nearby_fires.empty:
            return 0.0
        
        # Calculate risk factors
        num_fires = len(nearby_fires)
        max_frp = nearby_fires["fire_radiative_power"].max()
        
        # Normalize factors (FRP typically 0-500 MW)
        frequency_score = min(num_fires / 20.0, 1.0) * 100
        intensity_score = min(max_frp / 500.0, 1.0) * 100
        
        # Distance decay (closer fires = higher risk)
        min_distance = nearby_fires["distance_km"].min()
        proximity_score = max(0, 100 - (min_distance / radius_km * 100))
        
        # Weighted combination
        risk_score = 0.4 * frequency_score + 0.3 * intensity_score + 0.3 * proximity_score
        
        return min(risk_score, 100.0)
    
    def calculate_weather_alert_risk(
        self,
        alert_df: pd.DataFrame
    ) -> float:
        """
        Calculate severe weather risk score (0-100) based on active alerts
        
        Returns:
            Risk score 0-100 (higher = more risk)
        """
        
        if alert_df.empty:
            return 0.0
        
        # Severity mapping
        severity_scores = {
            "Extreme": 100,
            "Severe": 75,
            "Moderate": 50,
            "Minor": 25,
            "Unknown": 10
        }
        
        # Get highest severity alert
        max_score = 0
        for severity in alert_df["severity"].unique():
            score = severity_scores.get(severity, 0)
            max_score = max(max_score, score)
        
        return float(max_score)
    
    def calculate_composite_risk(
        self,
        earthquake_score: float,
        wildfire_score: float,
        weather_score: float,
        flood_score: float = 0.0,
        heat_score: float = 0.0
    ) -> Dict[str, float]:
        """
        Calculate composite risk score from individual hazard scores
        
        Returns:
            Dictionary with composite score and individual scores
        """
        
        scores = {
            "earthquake": earthquake_score,
            "wildfire": wildfire_score,
            "severe_weather": weather_score,
            "flood": flood_score,
            "extreme_heat": heat_score
        }
        
        # Calculate weighted composite score
        composite = sum(
            self.weights.get(hazard, 0) * score 
            for hazard, score in scores.items()
        )
        
        # Risk level classification
        if composite >= 75:
            risk_level = "Extreme"
        elif composite >= 50:
            risk_level = "High"
        elif composite >= 25:
            risk_level = "Moderate"
        else:
            risk_level = "Low"
        
        return {
            "composite_score": round(composite, 1),
            "risk_level": risk_level,
            **{f"{k}_score": round(v, 1) for k, v in scores.items()}
        }
    
    @staticmethod
    def _haversine_distance(
        lat1: float, lon1: float,
        lat2: np.ndarray, lon2: np.ndarray
    ) -> np.ndarray:
        """
        Calculate great circle distance between points using Haversine formula
        
        Args:
            lat1, lon1: Single point coordinates
            lat2, lon2: Array of coordinates
            
        Returns:
            Array of distances in kilometers
        """
        R = 6371  # Earth's radius in km
        
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        
        return R * c


if __name__ == "__main__":
    # Test the risk scorer
    print("\n" + "="*60)
    print("RISK SCORING ALGORITHM TEST")
    print("="*60 + "\n")
    
    scorer = RiskScorer()
    
    # Create dummy data for testing
    earthquake_df = pd.DataFrame({
        "time": pd.date_range(end=datetime.now(), periods=10, freq="D"),
        "latitude": np.random.uniform(39.5, 40.0, 10),
        "longitude": np.random.uniform(-105.5, -105.0, 10),
        "magnitude": np.random.uniform(2.5, 5.0, 10)
    })
    
    wildfire_df = pd.DataFrame({
        "acq_datetime": pd.date_range(end=datetime.now(), periods=5, freq="H"),
        "latitude": np.random.uniform(39.5, 40.0, 5),
        "longitude": np.random.uniform(-105.5, -105.0, 5),
        "fire_radiative_power": np.random.uniform(10, 200, 5)
    })
    
    # Test location (Denver)
    lat, lon = 39.7392, -104.9903
    
    print("Calculating risk scores for Denver, CO...")
    
    eq_risk = scorer.calculate_earthquake_risk(earthquake_df, lat, lon)
    fire_risk = scorer.calculate_wildfire_risk(wildfire_df, lat, lon)
    weather_risk = 0.0  # No alerts
    
    composite = scorer.calculate_composite_risk(eq_risk, fire_risk, weather_risk)
    
    print(f"\n✓ Risk Scoring Complete")
    print(f"\nIndividual Hazard Scores:")
    print(f"  Earthquake Risk:    {composite['earthquake_score']:.1f}/100")
    print(f"  Wildfire Risk:      {composite['wildfire_score']:.1f}/100")
    print(f"  Weather Risk:       {composite['severe_weather_score']:.1f}/100")
    print(f"  Flood Risk:         {composite['flood_score']:.1f}/100")
    print(f"  Extreme Heat Risk:  {composite['extreme_heat_score']:.1f}/100")
    
    print(f"\nComposite Risk Assessment:")
    print(f"  Composite Score:    {composite['composite_score']:.1f}/100")
    print(f"  Risk Level:         {composite['risk_level']}")
    
    print("\n" + "="*60)
    print("TEST COMPLETE")
    print("="*60)