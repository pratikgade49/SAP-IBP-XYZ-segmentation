"""
app/main.py - CLEANED VERSION

Removed:
- xyz_analysis router (redundant)
- Reference to old analysis service
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import datetime

from app.config import get_settings
from app.utils.logger import setup_logger, get_logger
from app.api.routes import health, xyz_write, dynamic_segmentation  # REMOVED xyz_analysis

from dotenv import load_dotenv
load_dotenv()

# Initialize settings
settings = get_settings()

# Setup logging
setup_logger("app", level=settings.LOG_LEVEL, format_type=settings.LOG_FORMAT)
logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="""
    API for SAP IBP XYZ Segmentation Analysis with Write-Back
    
    ## Features
    - **Dynamic Segmentation**: Analyze by Product, Location, Customer, or any combination
    - **Flexible Primary Keys**: Segment by PRDID, LOCID, CUSTID, etc.
    - **Write-Back**: Automatically write segments to SAP IBP
    - **Multi-dimensional**: Support for Product-Location, Location-Customer, and more
    
    ## Quick Start
    
    ### 1. Discover Available Dimensions
    ```
    GET /api/v1/dynamic-segmentation/available-attributes
    ```
    
    ### 2. Run Analysis
    ```json
    POST /api/v1/dynamic-segmentation/analyze
    {
      "primary_key": "PRDID",
      "groupby_attributes": ["PRDID", "LOCID"],
      "x_threshold": 10.0,
      "y_threshold": 25.0
    }
    ```
    
    ### 3. Write to SAP IBP
    ```json
    POST /api/v1/xyz-write/write-segments
    {
      "groupby_attributes": ["PRDID", "LOCID"],
      "x_threshold": 10.0,
      "y_threshold": 25.0,
      "write_mode": "batched",
      "version_id": "CONSENSUS"
    }
    ```
    
    ## Segmentation Examples
    
    | Use Case | Primary Key | Groupby Attributes |
    |----------|-------------|-------------------|
    | Product-only | PRDID | ["PRDID"] |
    | Location-only | LOCID | ["LOCID"] |
    | Product-Location | PRDID | ["PRDID", "LOCID"] |
    | Location-Customer | LOCID | ["LOCID", "CUSTID"] |
    | Customer-Product | CUSTID | ["CUSTID", "PRDID"] |
    """,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers - CLEANED UP
app.include_router(health.router)
app.include_router(dynamic_segmentation.router)
app.include_router(xyz_write.router)


@app.on_event("startup")
async def startup_event():
    """Application startup"""
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    logger.info(f"Debug mode: {settings.DEBUG}")
    logger.info(f"Write operations enabled: {settings.ENABLE_WRITE_OPERATIONS}")
    logger.info("Dynamic segmentation with flexible primary keys enabled")
    
    if settings.ENABLE_WRITE_OPERATIONS:
        logger.info(f"Write API URL: {settings.SAP_WRITE_API_URL}")
        logger.info(f"Planning Area: {settings.SAP_PLANNING_AREA}")
        logger.info(f"XYZ Key Figure: {settings.SAP_XYZ_KEY_FIGURE}")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown"""
    logger.info("Shutting down application")


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc) if settings.DEBUG else "An error occurred",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
        log_level=settings.LOG_LEVEL.lower()
    )