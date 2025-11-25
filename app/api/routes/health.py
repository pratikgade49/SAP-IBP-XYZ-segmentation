from fastapi import APIRouter
from datetime import datetime
from app.models.schemas import HealthResponse
from app.config import get_settings
from app.utils.logger import get_logger

router = APIRouter(tags=["health"])
logger = get_logger(__name__)


@router.get("/", response_model=HealthResponse)
@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    settings = get_settings()
    logger.debug("Health check requested")
    
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        version=settings.APP_VERSION
    )