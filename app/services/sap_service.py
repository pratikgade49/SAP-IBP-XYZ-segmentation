import requests
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional, List
from app.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SAPService:
    """Service for interacting with SAP IBP OData API"""
    
    # List of common SAP IBP attributes that can be used for segmentation
    AVAILABLE_ATTRIBUTES = [
        'PRDID',        # Product ID (required)
        'LOCID',        # Location ID
        'CUSTID',       # Customer ID
        'PRDGRPID',     # Product Group ID
        'REGIONID',     # Region ID
        'SALESORGID',   # Sales Organization ID
        'CHANID',       # Channel ID
        'DIVID',        # Division ID
    ]
    
    def __init__(self):
        self.settings = get_settings()
        self.api_url = self.settings.SAP_API_URL
        self.username = self.settings.SAP_USERNAME
        self.password = self.settings.SAP_PASSWORD
        self.timeout = self.settings.SAP_TIMEOUT
        
        self.namespaces = {
            'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
            'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
        }
    
    def fetch_data(
        self, 
        additional_filters: Optional[str] = None,
        additional_attributes: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Fetch product data from SAP IBP OData API
        
        Args:
            additional_filters: Optional OData filter string
            additional_attributes: List of additional attributes to fetch (e.g., ['LOCID', 'CUSTID'])
            
        Returns:
            DataFrame with product data
        """
        logger.info("Fetching data from SAP IBP API")
        
        # Base select fields (always needed)
        select_fields = ['PRDID', 'ACTUALSQTY', 'PERIODID3_TSTAMP']
        
        # Add additional attributes if requested
        if additional_attributes:
            for attr in additional_attributes:
                if attr not in select_fields and attr in self.AVAILABLE_ATTRIBUTES:
                    select_fields.append(attr)
                    logger.info(f"Adding attribute: {attr}")
        
        # Build $select clause
        select_clause = ','.join(select_fields)
        
        # Build filter
        base_filter = "UOMTOID eq 'EA' and ACTUALSQTY gt 0"
        query_filter = f"{base_filter} and {additional_filters}" if additional_filters else base_filter
        
        # Build complete URL
        url = f"{self.api_url}?$select={select_clause}&$filter={query_filter}"
        
        try:
            logger.debug(f"Making request to: {url}")
            response = requests.get(
                url,
                auth=(self.username, self.password),
                timeout=self.timeout
            )
            response.raise_for_status()
            logger.info("API request successful")
            
        except requests.exceptions.Timeout:
            logger.error("API request timeout")
            raise Exception("SAP API request timeout")
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise Exception(f"Failed to fetch data from SAP: {str(e)}")
        
        # Parse XML
        try:
            df = self._parse_xml_response(response.content, select_fields)
            logger.info(f"Successfully parsed {len(df)} records with columns: {list(df.columns)}")
            return df
            
        except ET.ParseError as e:
            logger.error(f"XML parsing failed: {str(e)}")
            raise Exception(f"Failed to parse XML response: {str(e)}")
    
    def _parse_xml_response(self, xml_content: bytes, expected_fields: List[str]) -> pd.DataFrame:
        """Parse XML response and convert to DataFrame"""
        root = ET.fromstring(xml_content)
        extracted_data = []
        
        for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
            properties = entry.find('.//m:properties', self.namespaces)
            
            if properties is not None:
                record = {}
                
                # Extract all requested fields
                for field in expected_fields:
                    element = properties.find(f'd:{field}', self.namespaces)
                    record[field] = element.text if element is not None else None
                
                extracted_data.append(record)
        
        if not extracted_data:
            logger.warning("No data found in API response")
            raise Exception("No data found")
        
        df = pd.DataFrame(extracted_data)
        
        # Convert ACTUALSQTY to numeric
        df['ACTUALSQTY'] = pd.to_numeric(df['ACTUALSQTY'], errors='coerce')
        df = df.dropna(subset=['ACTUALSQTY'])
        
        return df
    
    @classmethod
    def get_available_attributes(cls) -> List[str]:
        """Get list of available attributes for segmentation"""
        return cls.AVAILABLE_ATTRIBUTES