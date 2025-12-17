#  Natural Disaster Risk Intelligence Platform

**Real-Time Multi-Hazard Risk Assessment for Real Estate Portfolios**

A production-grade data pipeline that ingests real-time data from government APIs (USGS, NASA, NOAA) to assess natural disaster risk across earthquakes, wildfires, and severe weather.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)

##  Business Problem

Real estate investors and insurance companies need comprehensive risk assessment across multiple natural hazards to make informed decisions and comply with SEC climate disclosure rules.

###  Solution

Production-grade data pipeline integrating real-time data from USGS (earthquakes), NASA FIRMS (wildfires), and NOAA (weather alerts) to calculate composite risk scores. 

###  Business Value

- **Multi-source government API integration** with error handling
- **Geographic risk scoring algorithm** with configurable weights
- **RESTful API with FastAPI** (6 endpoints)
- **Interactive Streamlit dashboard** with real-time visualizations
- **Production-grade error handling and retry logic**

###  Key Metrics

- **API Response Time:** <2 seconds for single location analysis
- **Data Sources:** 3 government APIs integrated
- **Risk Factors:** 5 hazard types assessed
- **Dashboard Load Time:** <3 seconds
- **Data Freshness:** Real-time (as current as government sources)

###  Features

- **Data Engineering:** Multi-source data integration, schema validation, error handling
- **API Development:** RESTful design, FastAPI, Pydantic models
- **Data Science:** Risk scoring algorithms, geographic analysis, statistical modeling
- **Visualization:** Plotly charts, interactive dashboards, Streamlit
- **Production Patterns:** Retry logic, connection pooling, monitoring


##  Algorithm Architecture

### Composite Risk Calculation:

- Weighted multi-hazard scoring system combining 5 disaster types
- Default weights: Wildfire (30%), Earthquake (25%), Weather (20%), Flood (15%), Heat (10%)
- Configurable weight parameters for custom risk profiles
- Scores normalized to 0-100 scale with risk level classification (Low/Moderate/High/Extreme)

### Individual Hazard Scoring:

- **Earthquake Risk:** Historical frequency × magnitude severity × proximity decay

  - **Frequency score:** Event count normalized to 50+ events = max risk
  - **Magnitude score:** Max magnitude normalized to 7.0+ = max risk
  - **Weighted combination:** 60% frequency + 40% magnitude

- **Wildfire Risk:** Active fire detections × intensity × distance decay

  - **Frequency score:** Fire count normalized to 20+ fires = max risk
  - **Intensity score:** Fire Radiative Power normalized to 500 MW = max risk
  - **Proximity score:** Distance-based decay function
  - **Weighted combination:** 40% frequency + 30% intensity + 30% proximity

- **Weather Risk:** NOAA alert severity mapping

  - **Extreme alerts** = 100, **Severe** = 75, **Moderate** = 50, **Minor** = 25

### Geographic Processing:

- Haversine distance calculation for accurate geographic proximity
- Configurable search radius (default: 500km for earthquakes, 100km for wildfires)
- Time-based filtering (365 days for seismic, 30 days for fires)

### Data Quality & Validation:

- Schema validation for inconsistent government API responses
- Null handling and data completeness checks
- Outlier detection and normalization

### Performance Optimization:

- Vectorized NumPy operations for distance calculations
- Pandas-based data processing for efficiency
- Cached connector instances for reduced API calls

###  Data Sources

- **USGS**: Earthquake data (earthquake.usgs.gov)
- **NASA FIRMS**: Wildfire data (firms.modaps.eosdis.nasa.gov)
- **NOAA**: Weather alerts (api.weather.gov)


##  Project Structure
```
disaster-risk-platform/
├── src/
│   ├── api_connectors/      # Data ingestion from government APIs
│   ├── risk_scoring/        # Risk calculation algorithms
│   └── utils/               # Helper functions
├── api/                     # FastAPI endpoints
├── dashboard/               # Streamlit dashboard
├── data/                    # Data storage
└── tests/                   # Unit tests
```

###  Key Files

- `src/api_connectors/` - Data ingestion from government APIs
- `src/risk_scoring/` - Risk calculation algorithms
- `api/main.py` - FastAPI REST endpoints
- `dashboard/app.py` - Streamlit interactive dashboard


##  Quick Start

### 1. Setup
```bash
# Close repository
git clone https://github.com/michael-gurule/disaster-risk-platform.git
cd disaster-risk-platform

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Test connectors
python src/api_connectors/usgs_connector.py
python src/api_connectors/nasa_firms_connector.py
python src/api_connectors/noaa_weather_connector.py

# Run API
python api/main.py
# Visit http://localhost:8000/docs

# Run Dashboard
streamlit run dashboard/app.py
# Visit http://localhost:8501
```

###  Dashboard Features:
- Interactive location search
- Real-time data fetching
- Risk score visualizations
- Multi-tab detailed analysis
- Quick location presets


### Future Improvements

- [ ] FEMA flood zone integration
- [ ] Historical risk trends
- [ ] Email/Slack alerting
- [ ] PostgreSQL database
- [ ] Docker containerization
- [ ] AWS deployment


##  Screenshots

1. Dashboard overview showing composite risk score
2. Earthquake data visualization
3. Wildfire detection map
4. Weather alerts display
5. API interactive documentation

##  License

MIT License

## Author

# Michael Gurule 
- GitHub: [@yourusername](https://github.com/yourusername)
- LinkedIn: [Your Profile](https://linkedin.com/in/yourprofile)
- Email: michaelgurule1164@gmail.com
