from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # API Configuration
    APP_NAME: str = "SAP IBP XYZ Analysis API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    
    # SAP IBP Configuration
    SAP_API_URL: str
    SAP_USERNAME: str
    SAP_PASSWORD: str
    SAP_TIMEOUT: int = 30
    
    # Analysis Configuration
    DEFAULT_X_THRESHOLD: float = 10.0
    DEFAULT_Y_THRESHOLD: float = 25.0
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()