# 🌋 Natural Disaster Risk Intelligence Platform

**Real-Time Multi-Hazard Risk Assessment for Real Estate Portfolios**

A production-grade data pipeline that ingests real-time data from government APIs (USGS, NASA, NOAA) to assess natural disaster risk across earthquakes, wildfires, and severe weather.

##  Quick Start

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Test USGS connector
python src/api_connectors/usgs_connector.py
```

## 📊 Data Sources

- **USGS**: Earthquake data (earthquake.usgs.gov)
- **NASA FIRMS**: Wildfire data (firms.modaps.eosdis.nasa.gov)
- **NOAA**: Weather alerts (api.weather.gov)

## 🏗️ Project Structure

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

##  Technical Highlights

- Multi-source API integration with error handling
- Geographic risk scoring algorithms
- Real-time monitoring capabilities
- Production-grade data pipeline design

## 📝 License

MIT License

## 🤝 Contact

**Michael Gurule**
- Email: michaelgurule1164@gmail.com
- LinkedIn: linkedin.com/in/michaelgurule
- GitHub: github.com/michael-gurule
