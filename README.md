"""
# SAP IBP XYZ Analysis API

FastAPI application for fetching SAP IBP data and performing XYZ segmentation analysis.

## Features

- Fetch product data from SAP IBP OData API
- Perform XYZ segmentation based on demand variability
- Export results in CSV, JSON, or Excel format
- Structured logging with JSON format
- Modular architecture with separated concerns

## Installation

1. Clone the repository
2. Create virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create `.env` file from `.env.example`:
   ```bash
   cp .env.example .env
   ```

5. Update `.env` with your SAP credentials

## Running the Application

```bash
# Development
python -m app.main

# Or with uvicorn directly
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Endpoints

- `GET /` - Health check
- `GET /health` - Health check
- `GET /api/v1/xyz-analysis` - Perform XYZ analysis
- `GET /api/v1/xyz-analysis/export` - Export analysis results
- `GET /api/v1/xyz-analysis/summary` - Get segment summary

## Documentation

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Project Structure

```
sap_xyz_api/
├── app/
│   ├── __init__.py
│   ├── main.py                 # Application entry point
│   ├── config.py               # Configuration management
│   ├── models/
│   │   └── schemas.py          # Pydantic models
│   ├── services/
│   │   ├── sap_service.py      # SAP API integration
│   │   └── analysis_service.py # XYZ analysis logic
│   ├── api/
│   │   ├── dependencies.py     # Dependency injection
│   │   └── routes/
│   │       ├── health.py       # Health check routes
│   │       └── xyz_analysis.py # Analysis routes
│   └── utils/
│       └── logger.py           # Logging utilities
├── requirements.txt
├── .env
└── README.md
```

## XYZ Segmentation

- **X Segment**: Stable demand (CV ≤ 10%)
- **Y Segment**: Moderate variability (10% < CV ≤ 25%)
- **Z Segment**: High variability (CV > 25%)