"""
Service for importing data to SAP IBP
"""
import uuid
from typing import Dict, List, Optional, Any
from clients.sap_client import SAPClient
from utils.logger import get_logger
from config import Config

logger = get_logger(__name__)

class ImportService:
    """Handle data import to SAP IBP"""
    
    def __init__(self):
        self.client = SAPClient()
        self.batch_size = Config.BATCH_SIZE
    
    def import_standard(
        self,
        planning_area: str,
        aggregation_level_fields: str,
        data: List[Dict],
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        auto_commit: bool = True,
        transaction_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Import key figure data using standard method
        
        Args:
            planning_area: Planning area ID (e.g., 'SAPIBP1')
            aggregation_level_fields: Comma-separated field names
            data: List of data records to import
            version_id: Planning version ID (optional)
            scenario_id: Scenario ID (optional)
            auto_commit: Whether to auto-commit (default: True)
            transaction_id: Custom transaction ID (optional)
            
        Returns:
            Dictionary containing import result
        """
        logger.info(f"Starting standard import for planning area: {planning_area}")
        
        # Generate or use provided transaction ID
        if not transaction_id:
            transaction_id = self._generate_transaction_id()
        
        # Split data into batches
        batches = self._create_batches(data)
        logger.info(f"Split data into {len(batches)} batches")
        
        results = []
        
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1}/{len(batches)}")
            
            # Prepare payload
            payload = {
                "Transactionid": transaction_id,
                "AggregationLevelFieldsString": aggregation_level_fields,
                f"Nav{planning_area}": batch
            }
            
            if version_id:
                payload["VersionID"] = version_id
            
            if scenario_id:
                payload["ScenarioID"] = scenario_id
            
            # For last batch with auto_commit, add DoCommit flag
            if auto_commit and i == len(batches) - 1:
                payload["DoCommit"] = True
            
            # Send POST request
            endpoint = f"/{planning_area}Trans"
            response = self.client.post(endpoint, data=payload)
            results.append(response)
        
        # If not auto_commit, send separate commit
        if not auto_commit:
            commit_result = self.commit_transaction(transaction_id)
            results.append(commit_result)
        
        logger.info(f"Standard import completed for transaction: {transaction_id}")
        
        return {
            'transaction_id': transaction_id,
            'batches_processed': len(batches),
            'status': 'success',
            'details': results
        }
    
    def import_parallel(
        self,
        planning_area: str,
        aggregation_level_fields: str,
        data: List[Dict],
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        transaction_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Import key figure data using parallel method for high volumes
        
        Args:
            planning_area: Planning area ID
            aggregation_level_fields: Comma-separated field names
            data: List of data records to import
            version_id: Planning version ID (optional)
            scenario_id: Scenario ID (optional)
            transaction_id: Custom transaction ID (optional)
            
        Returns:
            Dictionary containing import result
        """
        logger.info(f"Starting parallel import for planning area: {planning_area}")
        
        # Step 1: Initiate parallel process
        init_result = self._initiate_parallel_process(
            planning_area=planning_area,
            version_id=version_id,
            scenario_id=scenario_id,
            transaction_id=transaction_id
        )
        
        # Extract transaction ID from response
        used_transaction_id = init_result.get('d', {}).get('Transactionid', transaction_id)
        logger.info(f"Using transaction ID: {used_transaction_id}")
        
        # Step 2: Send data in parallel batches
        batches = self._create_batches(data)
        logger.info(f"Split data into {len(batches)} batches for parallel processing")
        
        results = []
        
        # In production, these would be sent in parallel threads/processes
        # For simplicity, sending sequentially here
        for i, batch in enumerate(batches):
            logger.info(f"Processing parallel batch {i+1}/{len(batches)}")
            
            payload = {
                "Transactionid": used_transaction_id,
                "AggregationLevelFieldsString": aggregation_level_fields,
                f"Nav{planning_area}": batch
            }
            
            endpoint = f"/{planning_area}Trans"
            response = self.client.post(endpoint, data=payload)
            results.append(response)
        
        # Step 3: Commit transaction
        commit_result = self.commit_transaction(used_transaction_id)
        results.append(commit_result)
        
        logger.info(f"Parallel import completed for transaction: {used_transaction_id}")
        
        return {
            'transaction_id': used_transaction_id,
            'batches_processed': len(batches),
            'status': 'success',
            'details': results
        }
    
    def _initiate_parallel_process(
        self,
        planning_area: str,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        transaction_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Initiate parallel import process
        
        Args:
            planning_area: Planning area ID
            version_id: Planning version ID
            scenario_id: Scenario ID
            transaction_id: Transaction ID
            
        Returns:
            Dictionary containing initiation result
        """
        logger.info("Initiating parallel import process")
        
        payload = {
            "PlanningAreaID": planning_area
        }
        
        if transaction_id:
            payload["Transactionid"] = transaction_id
        
        if version_id:
            payload["VersionID"] = version_id
        
        if scenario_id:
            payload["ScenarioID"] = scenario_id
        
        endpoint = "/InitiateParallelProcess"
        return self.client.post(endpoint, data=payload)
    
    def commit_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """
        Commit import transaction
        
        Args:
            transaction_id: Transaction ID to commit
            
        Returns:
            Dictionary containing commit result
        """
        logger.info(f"Committing transaction: {transaction_id}")
        
        params = {
            'Transactionid': f"'{transaction_id}'"
        }
        
        endpoint = "/commit"
        response = self.client.post(endpoint, params=params)
        
        logger.info(f"Transaction committed successfully: {transaction_id}")
        return response
    
    def get_import_status(self, transaction_id: str) -> Dict[str, Any]:
        """
        Get import transaction status
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Dictionary containing status information
        """
        logger.info(f"Getting status for transaction: {transaction_id}")
        
        params = {
            'Transactionid': f"'{transaction_id}'"
        }
        
        endpoint = "/GetExportResult"
        return self.client.get(endpoint, params=params)
    
    def get_error_messages(
        self,
        transaction_id: str,
        planning_area: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get error messages for import transaction
        
        Args:
            transaction_id: Transaction ID
            planning_area: Planning area ID (for expanded details)
            
        Returns:
            Dictionary containing error messages
        """
        logger.info(f"Getting error messages for transaction: {transaction_id}")
        
        params = {
            '$filter': f"Transactionid eq '{transaction_id}'"
        }
        
        if planning_area:
            params['$expand'] = f"Nav{planning_area}Message"
        
        endpoint = "/Message"
        return self.client.get(endpoint, params=params)
    
    def get_transaction_id(self) -> str:
        """
        Get a new transaction ID from SAP IBP
        
        Returns:
            Generated transaction ID
        """
        logger.info("Requesting new transaction ID from SAP IBP")
        
        endpoint = "/getTransactionID"
        response = self.client.get(endpoint)
        
        transaction_id = response.get('d', {}).get('Transactionid', '')
        logger.info(f"Received transaction ID: {transaction_id}")
        
        return transaction_id
    
    def _generate_transaction_id(self) -> str:
        """
        Generate a unique transaction ID
        
        Returns:
            Generated transaction ID (32 characters max)
        """
        return uuid.uuid4().hex.upper()
    
    def _create_batches(self, data: List[Dict]) -> List[List[Dict]]:
        """
        Split data into batches
        
        Args:
            data: List of data records
            
        Returns:
            List of batches
        """
        batches = []
        for i in range(0, len(data), self.batch_size):
            batches.append(data[i:i + self.batch_size])
        return batches