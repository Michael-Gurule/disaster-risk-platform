"""
FastAPI REST API for Natural Disaster Risk Platform

Provides RESTful endpoints for risk assessment queries.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api_connectors import USGSConnector, NASAFIRMSConnector, NOAAWeatherConnector
from src.risk_scoring import RiskScorer

app = FastAPI(
    title="Natural Disaster Risk Intelligence API",
    description="Real-time multi-hazard risk assessment for locations",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize connectors
usgs_connector = USGSConnector()
nasa_connector = NASAFIRMSConnector()
noaa_connector = NOAAWeatherConnector()
risk_scorer = RiskScorer()


# Pydantic models
class LocationInput(BaseModel):
    latitude: float = Field(..., ge=-90, le=90, description="Latitude (-90 to 90)")
    longitude: float = Field(..., ge=-180, le=180, description="Longitude (-180 to 180)")
    radius_km: Optional[float] = Field(500, gt=0, description="Search radius in kilometers")


class RiskResponse(BaseModel):
    location: Dict[str, float]
    composite_score: float
    risk_level: str
    earthquake_score: float
    wildfire_score: float
    severe_weather_score: float
    flood_score: float
    extreme_heat_score: float
    timestamp: datetime
    data_sources: Dict[str, int]


class PropertyInput(BaseModel):
    properties: List[LocationInput]


class PortfolioResponse(BaseModel):
    total_properties: int
    average_composite_score: float
    risk_distribution: Dict[str, int]
    highest_risk_properties: List[Dict]
    timestamp: datetime


# API Endpoints

@app.get("/")
async def root():
    """API root endpoint"""
    return {
        "message": "Natural Disaster Risk Intelligence API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "risk_assessment": "/api/v1/risk/location",
            "portfolio_analysis": "/api/v1/risk/portfolio",
            "earthquakes": "/api/v1/data/earthquakes",
            "wildfires": "/api/v1/data/wildfires",
            "weather_alerts": "/api/v1/data/weather-alerts"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {
            "usgs_api": "operational",
            "nasa_firms_api": "operational",
            "noaa_api": "operational"
        }
    }


@app.post("/api/v1/risk/location", response_model=RiskResponse)
async def assess_location_risk(location: LocationInput):
    """
    Assess natural disaster risk for a specific location
    
    Returns composite risk score and individual hazard scores.
    """
    try:
        # Fetch data from all sources
        earthquakes = usgs_connector.get_earthquakes_near_location(
            location.latitude,
            location.longitude,
            max_radius_km=location.radius_km,
            days=365
        )
        
        wildfires = nasa_connector.get_wildfires_near_location(
            location.latitude,
            location.longitude,
            radius_deg=location.radius_km / 111.0,  # Convert km to degrees
            days=30
        )
        
        weather_alerts = noaa_connector.get_alerts_near_location(
            location.latitude,
            location.longitude
        )
        
        # Calculate risk scores
        eq_risk = risk_scorer.calculate_earthquake_risk(
            earthquakes,
            location.latitude,
            location.longitude,
            radius_km=location.radius_km
        )
        
        fire_risk = risk_scorer.calculate_wildfire_risk(
            wildfires,
            location.latitude,
            location.longitude,
            radius_km=location.radius_km
        )
        
        weather_risk = risk_scorer.calculate_weather_alert_risk(weather_alerts)
        
        # Calculate composite score
        composite = risk_scorer.calculate_composite_risk(
            eq_risk,
            fire_risk,
            weather_risk
        )
        
        return RiskResponse(
            location={
                "latitude": location.latitude,
                "longitude": location.longitude,
                "radius_km": location.radius_km
            },
            composite_score=composite["composite_score"],
            risk_level=composite["risk_level"],
            earthquake_score=composite["earthquake_score"],
            wildfire_score=composite["wildfire_score"],
            severe_weather_score=composite["severe_weather_score"],
            flood_score=composite["flood_score"],
            extreme_heat_score=composite["extreme_heat_score"],
            timestamp=datetime.now(),
            data_sources={
                "earthquakes": len(earthquakes),
                "wildfires": len(wildfires),
                "weather_alerts": len(weather_alerts)
            }
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error assessing risk: {str(e)}")


@app.post("/api/v1/risk/portfolio", response_model=PortfolioResponse)
async def assess_portfolio_risk(portfolio: PropertyInput):
    """
    Assess risk across a portfolio of properties
    
    Returns aggregate metrics and highest-risk properties.
    """
    try:
        if not portfolio.properties:
            raise HTTPException(status_code=400, detail="No properties provided")
        
        property_risks = []
        
        for prop in portfolio.properties:
            # Assess each property
            result = await assess_location_risk(prop)
            property_risks.append({
                "latitude": prop.latitude,
                "longitude": prop.longitude,
                "composite_score": result.composite_score,
                "risk_level": result.risk_level
            })
        
        # Calculate aggregate metrics
        scores = [p["composite_score"] for p in property_risks]
        avg_score = sum(scores) / len(scores)
        
        # Risk distribution
        risk_distribution = {
            "Extreme": sum(1 for p in property_risks if p["risk_level"] == "Extreme"),
            "High": sum(1 for p in property_risks if p["risk_level"] == "High"),
            "Moderate": sum(1 for p in property_risks if p["risk_level"] == "Moderate"),
            "Low": sum(1 for p in property_risks if p["risk_level"] == "Low")
        }
        
        # Highest risk properties
        highest_risk = sorted(
            property_risks,
            key=lambda x: x["composite_score"],
            reverse=True
        )[:5]
        
        return PortfolioResponse(
            total_properties=len(portfolio.properties),
            average_composite_score=round(avg_score, 1),
            risk_distribution=risk_distribution,
            highest_risk_properties=highest_risk,
            timestamp=datetime.now()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error assessing portfolio: {str(e)}")


@app.get("/api/v1/data/earthquakes")
async def get_earthquakes(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    radius_km: float = Query(500, gt=0),
    min_magnitude: float = Query(2.5, ge=0),
    days: int = Query(30, ge=1, le=365)
):
    """Get earthquake data for a location"""
    try:
        df = usgs_connector.get_earthquakes_near_location(
            latitude, longitude, radius_km, days
        )
        
        if df.empty:
            return {"count": 0, "earthquakes": []}
        
        # Filter by magnitude
        df = df[df["magnitude"] >= min_magnitude]
        
        return {
            "count": len(df),
            "earthquakes": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/data/wildfires")
async def get_wildfires(
    latitude: float = Query(..., ge=-90, le=90),
    longitude: float = Query(..., ge=-180, le=180),
    radius_deg: float = Query(2.0, gt=0),
    days: int = Query(7, ge=1, le=10)
):
    """Get wildfire data for a location"""
    try:
        df = nasa_connector.get_wildfires_near_location(
            latitude, longitude, radius_deg, days
        )
        
        if df.empty:
            return {"count": 0, "fires": []}
        
        return {
            "count": len(df),
            "fires": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/data/weather-alerts")
async def get_weather_alerts(
    state: Optional[str] = Query(None, regex="^[A-Z]{2}$"),
    latitude: Optional[float] = Query(None, ge=-90, le=90),
    longitude: Optional[float] = Query(None, ge=-180, le=180)
):
    """Get weather alerts for a state or location"""
    try:
        if latitude and longitude:
            df = noaa_connector.get_alerts_near_location(latitude, longitude)
        elif state:
            df = noaa_connector.get_active_alerts(state=state)
        else:
            raise HTTPException(status_code=400, detail="Must provide either state or lat/lon")
        
        if df.empty:
            return {"count": 0, "alerts": []}
        
        return {
            "count": len(df),
            "alerts": df.to_dict(orient="records")
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)