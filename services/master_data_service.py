"""
Service for extracting and importing master data from/to SAP IBP
Uses the /IBP/MASTER_DATA_API_SRV OData service
"""
from typing import Dict, List, Optional, Any
from clients.sap_client import SAPClient
from utils.logger import get_logger
from config import Config

logger = get_logger(__name__)

class MasterDataService:
    """Handle master data extraction and import for SAP IBP"""
    
    def __init__(self):
        # Use a separate base URL for master data API
        self.client = SAPClient()
        # Override base URL for master data operations
        self.master_data_base_url = f"https://{Config.SAP_IBP_HOST}/sap/opu/odata/IBP/MASTER_DATA_API_SRV"
        self.batch_size = Config.BATCH_SIZE
    
    # ==================== EXTRACTION METHODS ====================
    
    def extract_master_data(
        self,
        master_data_type: str,
        select: Optional[str] = None,
        filter_str: Optional[str] = None,
        orderby: Optional[str] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        format_type: str = 'json',
        inlinecount: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Extract master data from SAP IBP
        
        Args:
            master_data_type: Master data type (e.g., 'LOCATION', 'PRODUCT', 'LOCATIONPRODUCT')
            select: Comma-separated attributes to return (e.g., 'PRDID,LOCID')
            filter_str: Filter criteria (e.g., "PRDID eq 'IBP-100' and LOCID eq 'Frankfurt'")
            orderby: Order by field
            top: Number of records to return (recommended max: 50,000)
            skip: Number of records to skip (for pagination)
            format_type: Response format ('json' or 'xml')
            inlinecount: Request record count ('allpages' to get total count)
            
        Returns:
            Dictionary containing extracted master data
        """
        logger.info(f"Extracting master data type: {master_data_type}")
        
        # Build query parameters
        params = {
            '$format': format_type
        }
        
        if select:
            params['$select'] = select
        
        if filter_str:
            params['$filter'] = filter_str
        
        if orderby:
            params['$orderby'] = orderby
        
        if top:
            params['$top'] = top
        
        if skip:
            params['$skip'] = skip
        
        if inlinecount:
            params['$inlinecount'] = inlinecount
        
        # Construct endpoint
        endpoint = f"/{master_data_type}"
        
        # Temporarily override base URL
        original_base_url = self.client.base_url
        self.client.base_url = self.master_data_base_url
        
        try:
            response = self.client.get(endpoint, params=params)
            logger.info(f"Successfully extracted master data: {master_data_type}")
            return response
        finally:
            # Restore original base URL
            self.client.base_url = original_base_url
    
    def extract_version_specific_master_data(
        self,
        master_data_type: str,
        planning_area_id: str,
        version_id: str,
        select: Optional[str] = None,
        filter_str: Optional[str] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Extract version-specific master data
        
        Args:
            master_data_type: Master data type ID
            planning_area_id: Planning area ID (e.g., 'SAPIBP1')
            version_id: Version ID (use '__BASELINE' or empty string for baseline)
            select: Comma-separated attributes to return
            filter_str: Additional filter criteria
            top: Number of records to return
            skip: Number of records to skip
            
        Returns:
            Dictionary containing version-specific master data
        """
        logger.info(f"Extracting version-specific master data: {master_data_type}, PA: {planning_area_id}, Version: {version_id}")
        
        # Build filter with version-specific criteria
        version_filter = f"MasterDataTypeID eq '{master_data_type}' and PlanningAreaID eq '{planning_area_id}'"
        
        if version_id and version_id != '__BASELINE':
            version_filter += f" and VersionID eq '{version_id}'"
        else:
            # For baseline version, leave VersionID empty
            version_filter += " and VersionID eq ''"
        
        # Combine with additional filters if provided
        if filter_str:
            version_filter = f"({version_filter}) and ({filter_str})"
        
        return self.extract_master_data(
            master_data_type='VersionSpecificMasterDataTypes',
            select=select,
            filter_str=version_filter,
            top=top,
            skip=skip
        )
    
    def list_version_specific_master_data_types(self) -> Dict[str, Any]:
        """
        List all version-specific master data types
        
        Returns:
            Dictionary containing all version-specific master data types
        """
        logger.info("Listing all version-specific master data types")
        
        return self.extract_master_data(
            master_data_type='VersionSpecificMasterDataTypes',
            format_type='json'
        )
    
    def extract_master_data_paginated(
        self,
        master_data_type: str,
        select: Optional[str] = None,
        filter_str: Optional[str] = None,
        page_size: int = 50000,
        max_records: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Extract master data with automatic pagination
        
        Args:
            master_data_type: Master data type
            select: Attributes to return
            filter_str: Filter criteria
            page_size: Records per page (default: 50,000)
            max_records: Maximum total records to retrieve
            
        Returns:
            Dictionary containing all extracted records
        """
        logger.info(f"Starting paginated extraction for {master_data_type}")
        
        all_records = []
        skip = 0
        total_fetched = 0
        
        while True:
            # Determine how many records to fetch in this batch
            current_top = page_size
            if max_records:
                remaining = max_records - total_fetched
                if remaining <= 0:
                    break
                current_top = min(page_size, remaining)
            
            logger.info(f"Fetching page: skip={skip}, top={current_top}")
            
            response = self.extract_master_data(
                master_data_type=master_data_type,
                select=select,
                filter_str=filter_str,
                top=current_top,
                skip=skip
            )
            
            # Extract records from response
            records = response.get('d', {}).get('results', [])
            
            if not records:
                logger.info("No more records to fetch")
                break
            
            all_records.extend(records)
            total_fetched += len(records)
            
            logger.info(f"Fetched {len(records)} records. Total: {total_fetched}")
            
            # Check if we've reached the end
            if len(records) < current_top:
                break
            
            skip += current_top
        
        logger.info(f"Paginated extraction completed. Total records: {len(all_records)}")
        
        return {
            'd': {
                'results': all_records,
                '__count': len(all_records)
            }
        }
    
    # ==================== IMPORT METHODS ====================
    
    def import_master_data(
        self,
        master_data_type: str,
        requested_attributes: str,
        data: List[Dict],
        planning_area_id: Optional[str] = None,
        version_id: Optional[str] = None,
        do_commit: bool = True,
        transaction_id: Optional[str] = None,
        transaction_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Import master data to SAP IBP (standard method)
        
        Args:
            master_data_type: Master data type (e.g., 'LOCATION', 'PRODUCT')
            requested_attributes: Comma-separated attribute names
            data: List of master data records to import
            planning_area_id: Planning area ID (for version-specific data)
            version_id: Version ID (for version-specific data)
            do_commit: Whether to commit immediately (default: True)
            transaction_id: Custom transaction ID (max 32 chars)
            transaction_name: Custom transaction name (max 60 chars)
            
        Returns:
            Dictionary containing import result
        """
        logger.info(f"Starting master data import for type: {master_data_type}")
        
        # Generate transaction ID if not provided
        if not transaction_id:
            import uuid
            transaction_id = uuid.uuid4().hex.upper()[:32]
        
        # Split data into batches
        batches = self._create_batches(data)
        logger.info(f"Split data into {len(batches)} batches")
        
        results = []
        
        # Temporarily override base URL
        original_base_url = self.client.base_url
        self.client.base_url = self.master_data_base_url
        
        try:
            for i, batch in enumerate(batches):
                logger.info(f"Processing batch {i+1}/{len(batches)}")
                
                # Prepare payload
                payload = {
                    "TransactionID": transaction_id,
                    "RequestedAttributes": requested_attributes,
                    f"Nav{master_data_type}": batch
                }
                
                # Add optional fields
                if planning_area_id:
                    payload["PlanningAreaID"] = planning_area_id
                
                if version_id:
                    payload["VersionID"] = version_id
                
                if transaction_name:
                    payload["TransactionName"] = transaction_name
                
                # Only add DoCommit on last batch
                if do_commit and i == len(batches) - 1:
                    payload["DoCommit"] = True
                
                # Send POST request
                endpoint = f"/{master_data_type}Trans"
                response = self.client.post(endpoint, data=payload)
                results.append(response)
            
            logger.info(f"Master data import completed for transaction: {transaction_id}")
            
            return {
                'transaction_id': transaction_id,
                'batches_processed': len(batches),
                'records_imported': len(data),
                'status': 'success',
                'details': results
            }
        
        finally:
            # Restore original base URL
            self.client.base_url = original_base_url
    
    def import_master_data_parallel(
        self,
        master_data_type: str,
        requested_attributes: str,
        data: List[Dict],
        transaction_id: str,
        planning_area_id: Optional[str] = None,
        version_id: Optional[str] = None,
        transaction_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Import master data using parallel method for high volumes
        Note: DoCommit parameter cannot be used with parallel requests
        
        Args:
            master_data_type: Master data type
            requested_attributes: Comma-separated attribute names
            data: List of master data records
            transaction_id: Transaction ID (must be same for all parallel requests)
            planning_area_id: Planning area ID (for version-specific data)
            version_id: Version ID (for version-specific data)
            transaction_name: Custom transaction name
            
        Returns:
            Dictionary containing import result
        """
        logger.info(f"Starting parallel master data import for type: {master_data_type}")
        
        # Split data into batches
        batches = self._create_batches(data)
        logger.info(f"Split data into {len(batches)} batches for parallel processing")
        
        results = []
        
        # Temporarily override base URL
        original_base_url = self.client.base_url
        self.client.base_url = self.master_data_base_url
        
        try:
            # In production, these would be sent in parallel threads/processes
            for i, batch in enumerate(batches):
                logger.info(f"Processing parallel batch {i+1}/{len(batches)}")
                
                payload = {
                    "TransactionID": transaction_id,
                    "RequestedAttributes": requested_attributes,
                    f"Nav{master_data_type}": batch
                }
                
                if planning_area_id:
                    payload["PlanningAreaID"] = planning_area_id
                
                if version_id:
                    payload["VersionID"] = version_id
                
                if transaction_name:
                    payload["TransactionName"] = transaction_name
                
                endpoint = f"/{master_data_type}Trans"
                response = self.client.post(endpoint, data=payload)
                results.append(response)
            
            # Commit must be done separately for parallel imports
            commit_result = self._commit_master_data_transaction(transaction_id)
            results.append(commit_result)
            
            logger.info(f"Parallel master data import completed for transaction: {transaction_id}")
            
            return {
                'transaction_id': transaction_id,
                'batches_processed': len(batches),
                'records_imported': len(data),
                'status': 'success',
                'details': results
            }
        
        finally:
            # Restore original base URL
            self.client.base_url = original_base_url
    
    def delete_master_data(
        self,
        master_data_type: str,
        requested_attributes: str,
        data: List[Dict],
        planning_area_id: Optional[str] = None,
        version_id: Optional[str] = None,
        do_commit: bool = True,
        transaction_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Delete master data records from SAP IBP
        
        Args:
            master_data_type: Master data type
            requested_attributes: Key attributes (must include all key fields)
            data: List of records to delete (must contain all key attributes)
            planning_area_id: Planning area ID
            version_id: Version ID (if not provided, deletes from base version)
            do_commit: Whether to commit immediately
            transaction_id: Custom transaction ID
            
        Returns:
            Dictionary containing deletion result
        """
        logger.info(f"Starting master data deletion for type: {master_data_type}")
        
        # Generate transaction ID if not provided
        if not transaction_id:
            import uuid
            transaction_id = uuid.uuid4().hex.upper()[:32]
        
        # Prepare payload
        payload = {
            "TransactionID": transaction_id,
            "RequestedAttributes": requested_attributes,
            "DeleteEntries": True,
            f"Nav{master_data_type}": data
        }
        
        if planning_area_id:
            payload["PlanningAreaID"] = planning_area_id
        
        if version_id:
            payload["VersionID"] = version_id
        
        if do_commit:
            payload["DoCommit"] = True
        
        # Temporarily override base URL
        original_base_url = self.client.base_url
        self.client.base_url = self.master_data_base_url
        
        try:
            endpoint = f"/{master_data_type}Trans"
            response = self.client.post(endpoint, data=payload)
            
            logger.info(f"Master data deletion completed for transaction: {transaction_id}")
            
            return {
                'transaction_id': transaction_id,
                'records_deleted': len(data),
                'status': 'success',
                'details': response
            }
        
        finally:
            # Restore original base URL
            self.client.base_url = original_base_url
    
    def get_import_status(self, transaction_id: str) -> Dict[str, Any]:
        """
        Get import transaction status
        
        Args:
            transaction_id: Transaction ID
            
        Returns:
            Dictionary containing status information
        """
        logger.info(f"Getting import status for transaction: {transaction_id}")
        
        params = {
            'Transactionid': f"'{transaction_id}'"
        }
        
        original_base_url = self.client.base_url
        self.client.base_url = self.master_data_base_url
        
        try:
            endpoint = "/GetExportResult"
            return self.client.get(endpoint, params=params)
        finally:
            self.client.base_url = original_base_url
    
    def get_error_messages(
        self,
        transaction_id: str,
        master_data_type: Optional[str] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get error messages for import transaction
        
        Args:
            transaction_id: Transaction ID
            master_data_type: Master data type (to expand error details)
            top: Limit number of error messages
            skip: Skip number of error messages
            
        Returns:
            Dictionary containing error messages
        """
        logger.info(f"Getting error messages for transaction: {transaction_id}")
        
        params = {
            '$filter': f"Transactionid eq '{transaction_id}'"
        }
        
        if master_data_type:
            params['$expand'] = f"Nav{master_data_type}"
        
        if top:
            params['$top'] = top
        
        if skip:
            params['$skip'] = skip
        
        original_base_url = self.client.base_url
        self.client.base_url = self.master_data_base_url
        
        try:
            endpoint = "/Message"
            return self.client.get(endpoint, params=params)
        finally:
            self.client.base_url = original_base_url
    
    # ==================== HELPER METHODS ====================
    
    def _commit_master_data_transaction(self, transaction_id: str) -> Dict[str, Any]:
        """
        Commit master data import transaction
        
        Args:
            transaction_id: Transaction ID to commit
            
        Returns:
            Dictionary containing commit result
        """
        logger.info(f"Committing master data transaction: {transaction_id}")
        
        # Note: The commit endpoint may vary - adjust if needed
        # This assumes similar structure to key figure imports
        params = {
            'Transactionid': f"'{transaction_id}'"
        }
        
        endpoint = "/commit"
        response = self.client.post(endpoint, params=params)
        
        logger.info(f"Master data transaction committed: {transaction_id}")
        return response
    
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