"""
Configuration settings for SAP IBP Integration
"""
import os
from dotenv import load_dotenv

# Force reload of .env file
load_dotenv(override=True)

class Config:
    """Base configuration"""
    
    # Try to load from local_config.py first (for when .env doesn't work)
    try:
        import local_config
        _use_local_config = True
        print("📝 Loading configuration from local_config.py")
    except ImportError:
        _use_local_config = False
        print("📝 Loading configuration from environment variables")
    
    # Flask settings
    if _use_local_config:
        SECRET_KEY = getattr(local_config, 'SECRET_KEY', 'dev-secret-key-change-in-production')
        DEBUG = getattr(local_config, 'DEBUG', False)
        HOST = getattr(local_config, 'HOST', '0.0.0.0')
        PORT = getattr(local_config, 'PORT', 5000)
    else:
        SECRET_KEY = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
        DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
        HOST = os.getenv('HOST', '0.0.0.0')
        PORT = int(os.getenv('PORT', 5000))
    
    # SAP IBP OData Service settings
    if _use_local_config:
        SAP_IBP_HOST = getattr(local_config, 'SAP_IBP_HOST', 'your-sap-ibp-host.com')
        SAP_IBP_USERNAME = getattr(local_config, 'SAP_IBP_USERNAME', None)
        SAP_IBP_PASSWORD = getattr(local_config, 'SAP_IBP_PASSWORD', None)
        REQUEST_TIMEOUT = getattr(local_config, 'REQUEST_TIMEOUT', 300)
        MAX_RETRIES = getattr(local_config, 'MAX_RETRIES', 3)
        BATCH_SIZE = getattr(local_config, 'BATCH_SIZE', 5000)
        DELTA_QUERY_TIMEOUT = getattr(local_config, 'DELTA_QUERY_TIMEOUT', 1200)
        LOG_LEVEL = getattr(local_config, 'LOG_LEVEL', 'INFO')
        LOG_FILE = getattr(local_config, 'LOG_FILE', 'logs/app.log')
    else:
        SAP_IBP_HOST = os.getenv('SAP_IBP_HOST', 'your-sap-ibp-host.com')
        SAP_IBP_USERNAME = os.getenv('SAP_IBP_USERNAME')
        SAP_IBP_PASSWORD = os.getenv('SAP_IBP_PASSWORD')
        REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', 300))
        MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
        BATCH_SIZE = int(os.getenv('BATCH_SIZE', 5000))
        DELTA_QUERY_TIMEOUT = int(os.getenv('DELTA_QUERY_TIMEOUT', 1200))
        LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        LOG_FILE = os.getenv('LOG_FILE', 'logs/app.log')
    
    SAP_IBP_BASE_URL = f"https://{SAP_IBP_HOST}/sap/opu/odata/ibp/PLANNING_DATA_API_SRV"
    
    # Optional authentication methods
    SAP_IBP_API_KEY = os.getenv('SAP_IBP_API_KEY')
    SAP_IBP_CLIENT_ID = os.getenv('SAP_IBP_CLIENT_ID')
    SAP_IBP_CLIENT_SECRET = os.getenv('SAP_IBP_CLIENT_SECRET')
    
    @classmethod
    def validate(cls):
        """Validate required configuration"""
        errors = []
        
        if not cls.SAP_IBP_USERNAME:
            errors.append("SAP_IBP_USERNAME is not set")
        if not cls.SAP_IBP_PASSWORD:
            errors.append("SAP_IBP_PASSWORD is not set")
        if cls.SAP_IBP_HOST == 'your-sap-ibp-host.com':
            errors.append("SAP_IBP_HOST is not configured")
            
        if errors:
            print("\n" + "=" * 60)
            print("⚠️  CONFIGURATION ERRORS:")
            for error in errors:
                print(f"   ❌ {error}")
            print("\nPlease either:")
            print("   1. Create local_config.py with your credentials")
            print("   2. Fix your .env file")
            print("   3. Set environment variables manually")
            print("=" * 60 + "\n")
            
        return len(errors) == 0

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False

class TestConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True