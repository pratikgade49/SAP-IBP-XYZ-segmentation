"""
Local configuration file - Direct credential storage
⚠️ DO NOT commit this file to git!
Add local_config.py to your .gitignore
"""

# SAP IBP Configuration
SAP_IBP_HOST = "my303361-api.scmibp1.ondemand.com"
SAP_IBP_USERNAME = "ODATA_USER"  # Replace with actual username
SAP_IBP_PASSWORD = "JyqEsTPHyn7uzVZnHMyfLpN$SFJLwktUloHPaxAu"  # Replace with actual password

# Flask Configuration
DEBUG = True
HOST = "0.0.0.0"
PORT = 5000

# Request Settings
REQUEST_TIMEOUT = 300
MAX_RETRIES = 3

# Data Handling
BATCH_SIZE = 5000
DELTA_QUERY_TIMEOUT = 1200

# Logging
LOG_LEVEL = "INFO"
LOG_FILE = "logs/app.log"