"""
Service for extracting data from SAP IBP
"""
import time
from typing import Dict, Optional, Any
from clients.sap_client import SAPClient
from utils.logger import get_logger

logger = get_logger(__name__)

class ExtractionService:
    """Handle data extraction from SAP IBP"""
    
    def __init__(self):
        self.client = SAPClient()
    
    def extract_keyfigures(
        self,
        planning_area: str,
        select: str,
        filter_str: Optional[str] = None,
        format_type: str = 'json',
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract key figure data from SAP IBP
        
        Args:
            planning_area: Planning area ID (e.g., 'SAPIBP1')
            select: Select fields (e.g., 'PRDID,LOCID,DEMANDQTY')
            filter_str: Filter criteria (optional)
            format_type: Response format ('json' or 'xml')
            top: Number of records to return
            skip: Number of records to skip
            orderby: Order by field
            
        Returns:
            Dictionary containing extracted data
        """
        logger.info(f"Extracting key figures from planning area: {planning_area}")
        
        params = {
            '$select': select,
            '$format': format_type
        }
        
        if filter_str:
            params['$filter'] = filter_str
        if top:
            params['$top'] = top
        if skip:
            params['$skip'] = skip
        if orderby:
            params['$orderby'] = orderby
        
        endpoint = f"/{planning_area}"
        response = self.client.get(endpoint, params=params)
        
        logger.info(f"Successfully extracted data from {planning_area}")
        return response
    
    def create_delta_query(
        self,
        delta_query_id: str,
        description: str,
        planning_area: str,
        select: str,
        filter_str: str
    ) -> Dict[str, Any]:
        """
        Create a key figure delta query definition
        
        Args:
            delta_query_id: Unique delta query identifier
            description: Query description
            planning_area: Planning area ID
            select: Select fields for delta query
            filter_str: Filter criteria for delta query
            
        Returns:
            Dictionary containing creation result
        """
        logger.info(f"Creating delta query: {delta_query_id}")
        
        payload = {
            "DeltaQueryID": delta_query_id,
            "DeltaQueryDescription": description,
            "PlanningAreaID": planning_area,
            "DeltaQuerySelect": select,
            "DeltaQueryFilter": filter_str
        }
        
        endpoint = "/KeyFigureDeltaDefinitionSet"
        response = self.client.post(endpoint, data=payload)
        
        logger.info(f"Successfully created delta query: {delta_query_id}")
        return response
    
    def execute_delta_load(self, delta_query_id: str) -> Dict[str, Any]:
        """
        Execute delta load for a delta query
        
        Args:
            delta_query_id: Delta query identifier
            
        Returns:
            Dictionary containing execution result
        """
        logger.info(f"Executing delta load for: {delta_query_id}")
        
        params = {
            'P_DeltaQueryID': f"'{delta_query_id}'",
            '$format': 'json'
        }
        
        endpoint = "/executeKeyFigureDeltaLoad"
        response = self.client.get(endpoint, params=params)
        
        # Wait for processing to complete
        max_attempts = 60
        attempt = 0
        
        while attempt < max_attempts:
            status = self.get_delta_status(delta_query_id)
            status_value = status.get('d', {}).get('StatusKeyFigureDeltaLoad', '')
            
            logger.info(f"Delta load status: {status_value}")
            
            if status_value in ['Extractable', 'Error', 'Confirmed']:
                break
            
            time.sleep(5)
            attempt += 1
        
        logger.info(f"Delta load execution completed for: {delta_query_id}")
        return response
    
    def get_delta_status(self, delta_query_id: str) -> Dict[str, Any]:
        """
        Get status of delta load
        
        Args:
            delta_query_id: Delta query identifier
            
        Returns:
            Dictionary containing status information
        """
        params = {
            'P_DeltaQueryID': f"'{delta_query_id}'",
            '$format': 'json'
        }
        
        endpoint = "/getStatusKeyFigureDeltaLoad"
        return self.client.get(endpoint, params=params)
    
    def extract_delta_data(
        self,
        delta_query_id: str,
        planning_area: str,
        select: str,
        top: int = 1000,
        skip: int = 0
    ) -> Dict[str, Any]:
        """
        Extract delta data
        
        Args:
            delta_query_id: Delta query identifier
            planning_area: Planning area ID
            select: Select fields (must match delta query definition)
            top: Number of records to return
            skip: Number of records to skip
            
        Returns:
            Dictionary containing delta data
        """
        logger.info(f"Extracting delta data for: {delta_query_id}")
        
        params = {
            '$select': select,
            'P_DeltaQueryID': delta_query_id,
            '$top': top,
            '$skip': skip,
            '$format': 'json'
        }
        
        endpoint = f"/{planning_area}"
        response = self.client.get(endpoint, params=params)
        
        logger.info(f"Successfully extracted delta data")
        return response
    
    def confirm_delta_load(self, delta_query_id: str) -> Dict[str, Any]:
        """
        Confirm delta load completion
        
        Args:
            delta_query_id: Delta query identifier
            
        Returns:
            Dictionary containing confirmation result
        """
        logger.info(f"Confirming delta load for: {delta_query_id}")
        
        params = {
            'P_DeltaQueryID': f"'{delta_query_id}'",
            '$format': 'json'
        }
        
        endpoint = "/confirmKeyFigureDeltaLoad"
        response = self.client.get(endpoint, params=params)
        
        logger.info(f"Successfully confirmed delta load: {delta_query_id}")
        return response
    
    def cancel_delta_load(self, delta_query_id: str) -> Dict[str, Any]:
        """
        Cancel delta load
        
        Args:
            delta_query_id: Delta query identifier
            
        Returns:
            Dictionary containing cancellation result
        """
        logger.info(f"Cancelling delta load for: {delta_query_id}")
        
        params = {
            'P_DeltaQueryID': f"'{delta_query_id}'",
            '$format': 'json'
        }
        
        endpoint = "/cancelKeyfigureDeltaLoad"
        response = self.client.get(endpoint, params=params)
        
        logger.info(f"Successfully cancelled delta load: {delta_query_id}")
        return response
    
    def reset_delta_load(self, delta_query_id: str) -> Dict[str, Any]:
        """
        Reset delta load
        
        Args:
            delta_query_id: Delta query identifier
            
        Returns:
            Dictionary containing reset result
        """
        logger.info(f"Resetting delta load for: {delta_query_id}")
        
        params = {
            'P_DeltaQueryID': f"'{delta_query_id}'",
            '$format': 'json'
        }
        
        endpoint = "/resetKeyFigureDeltaLoad"
        response = self.client.get(endpoint, params=params)
        
        logger.info(f"Successfully reset delta load: {delta_query_id}")
        return response
    
    def list_delta_queries(self) -> Dict[str, Any]:
        """
        List all delta query definitions
        
        Returns:
            Dictionary containing all delta queries
        """
        logger.info("Listing all delta query definitions")
        
        endpoint = "/KeyFigureDeltaDefinitionSet"
        response = self.client.get(endpoint)
        
        return response
    
    def delete_delta_query(self, delta_query_id: str) -> Dict[str, Any]:
        """
        Delete a delta query definition
        
        Args:
            delta_query_id: Delta query identifier
            
        Returns:
            Dictionary containing deletion result
        """
        logger.info(f"Deleting delta query: {delta_query_id}")
        
        endpoint = f"/KeyFigureDeltaDefinitionSet('{delta_query_id}')"
        response = self.client.delete(endpoint)
        
        logger.info(f"Successfully deleted delta query: {delta_query_id}")
        return response