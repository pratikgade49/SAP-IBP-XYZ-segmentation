"""
SAP IBP OData client for handling HTTP requests
"""
import requests
from requests.auth import HTTPBasicAuth
from typing import Dict, Optional, Any
from config import Config
from utils.logger import get_logger

logger = get_logger(__name__)

class SAPClient:
    """Client for SAP IBP OData service"""
    
    def __init__(self):
        self.base_url = Config.SAP_IBP_BASE_URL
        self.username = Config.SAP_IBP_USERNAME
        self.password = Config.SAP_IBP_PASSWORD
        self.timeout = Config.REQUEST_TIMEOUT
        self.max_retries = Config.MAX_RETRIES
        self.csrf_token = None
        
        # Setup session
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(self.username, self.password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'x-csrf-token': 'fetch'  # Request CSRF token
        })
        
        # Fetch CSRF token on initialization
        self._fetch_csrf_token()
    
    def get(
        self,
        endpoint: str,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Send GET request to SAP IBP
        
        Args:
            endpoint: API endpoint (e.g., '/SAPIBP1')
            params: Query parameters
            
        Returns:
            Response data as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        
        logger.debug(f"GET request to: {url}")
        logger.debug(f"Parameters: {params}")
        
        try:
            response = self._make_request('GET', url, params=params)
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"GET request failed: {str(e)}")
            raise
    
    def post(
        self,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Send POST request to SAP IBP
        
        Args:
            endpoint: API endpoint
            data: Request body data
            params: Query parameters
            
        Returns:
            Response data as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        
        logger.debug(f"POST request to: {url}")
        logger.debug(f"Data: {data}")
        
        try:
            response = self._make_request('POST', url, json=data, params=params)
            
            # Some POST responses may be empty
            if response.text:
                return response.json()
            return {'status': 'success', 'message': 'Request completed'}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"POST request failed: {str(e)}")
            raise
    
    def delete(self, endpoint: str) -> Dict[str, Any]:
        """
        Send DELETE request to SAP IBP
        
        Args:
            endpoint: API endpoint
            
        Returns:
            Response data as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        
        logger.debug(f"DELETE request to: {url}")
        
        try:
            response = self._make_request('DELETE', url)
            
            # DELETE responses are typically empty
            if response.text:
                return response.json()
            return {'status': 'success', 'message': 'Resource deleted'}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"DELETE request failed: {str(e)}")
            raise
    
    def _make_request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> requests.Response:
        """
        Make HTTP request with retry logic
        
        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional arguments for requests
            
        Returns:
            Response object
        """
        kwargs['timeout'] = self.timeout
        
        for attempt in range(self.max_retries):
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                
                logger.info(f"{method} request successful: {url}")
                return response
                
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP error (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                
                # Don't retry on client errors (4xx)
                if 400 <= response.status_code < 500:
                    logger.error(f"Client error: {response.text}")
                    raise
                
                # Retry on server errors (5xx)
                if attempt == self.max_retries - 1:
                    logger.error(f"Max retries reached. Last error: {str(e)}")
                    raise
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error (attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                
                if attempt == self.max_retries - 1:
                    logger.error(f"Max retries reached. Last error: {str(e)}")
                    raise
    
    def close(self):
        """Close the session"""
        self.session.close()
    
    def _fetch_csrf_token(self):
        """
        Fetch CSRF token from SAP IBP
        Required for POST, PUT, DELETE operations
        """
        try:
            # Make a HEAD request to get the CSRF token
            url = f"{self.base_url}/"
            headers = {
                'x-csrf-token': 'fetch',
                'Accept': 'application/json'
            }
            
            response = self.session.head(url, headers=headers, timeout=30)
            
            # Extract CSRF token from response headers
            if 'x-csrf-token' in response.headers:
                self.csrf_token = response.headers['x-csrf-token']
                # Update session headers with the token
                self.session.headers.update({
                    'x-csrf-token': self.csrf_token
                })
                logger.info("CSRF token fetched successfully")
            else:
                logger.warning("CSRF token not found in response headers")
                
        except Exception as e:
            logger.warning(f"Failed to fetch CSRF token: {str(e)}")
            # Continue without CSRF token - some endpoints may not require it