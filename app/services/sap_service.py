import requests
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional
from app.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SAPService:
    """Service for interacting with SAP IBP OData API"""
    
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
    
    def fetch_data(self, additional_filters: Optional[str] = None) -> pd.DataFrame:
        """
        Fetch product data from SAP IBP OData API
        
        Args:
            additional_filters: Optional OData filter string
            
        Returns:
            DataFrame with product data
        """
        logger.info("Fetching data from SAP IBP API")
        
        # Build query
        base_filter = "UOMTOID eq 'EA' and ACTUALSQTY gt 0"
        query_filter = f"{base_filter} and {additional_filters}" if additional_filters else base_filter
        
        url = f"{self.api_url}?$select=PRDID,ACTUALSQTY,PERIODID3_TSTAMP&$filter={query_filter}"
        
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
            df = self._parse_xml_response(response.content)
            logger.info(f"Successfully parsed {len(df)} records")
            return df
            
        except ET.ParseError as e:
            logger.error(f"XML parsing failed: {str(e)}")
            raise Exception(f"Failed to parse XML response: {str(e)}")
    
    def _parse_xml_response(self, xml_content: bytes) -> pd.DataFrame:
        """Parse XML response and convert to DataFrame"""
        root = ET.fromstring(xml_content)
        extracted_data = []
        
        for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
            properties = entry.find('.//m:properties', self.namespaces)
            
            if properties is not None:
                prdid = properties.find('d:PRDID', self.namespaces)
                period = properties.find('d:PERIODID3_TSTAMP', self.namespaces)
                qty = properties.find('d:ACTUALSQTY', self.namespaces)
                
                extracted_data.append({
                    'PRDID': prdid.text if prdid is not None else None,
                    'KF_DATE': period.text if period is not None else None,
                    'ACTUALSQTY': qty.text if qty is not None else None,
                })
        
        if not extracted_data:
            logger.warning("No data found in API response")
            raise Exception("No data found")
        
        df = pd.DataFrame(extracted_data)
        df['ACTUALSQTY'] = pd.to_numeric(df['ACTUALSQTY'], errors='coerce')
        
        # Remove rows with invalid quantities
        df = df.dropna(subset=['ACTUALSQTY'])
        
        return df