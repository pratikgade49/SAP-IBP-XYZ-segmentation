"""
app/services/sap_write_service.py - Complete version with all write modes

This is the COMPLETE file - replace your existing sap_write_service.py with this
"""

import requests
import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime
import uuid
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)


class SAPWriteService:
    """Service for writing data back to SAP IBP via PLANNING_DATA_API_SRV"""
    
    def __init__(self):
        self.settings = get_settings()
        self.api_url = self.settings.SAP_WRITE_API_URL.rstrip('/')
        self.username = self.settings.SAP_USERNAME
        self.password = self.settings.SAP_PASSWORD
        self.timeout = self.settings.SAP_TIMEOUT
        self.planning_area = self.settings.SAP_PLANNING_AREA
        self.xyz_key_figure = self.settings.SAP_XYZ_KEY_FIGURE
        self.enable_null_handling = self.settings.SAP_ENABLE_NULL_HANDLING
        
        logger.info(f"Initialized write service with URL: {self.api_url}")
        logger.info(f"Planning area: {self.planning_area}")
        logger.info(f"Key figure: {self.xyz_key_figure}")
    
    def _get_csrf_token(self) -> tuple[requests.Session, str]:
        """Fetch CSRF token required for POST operations"""
        logger.debug("Fetching CSRF token from SAP")
        
        session = requests.Session()
        session.auth = (self.username, self.password)
        
        try:
            response = session.get(
                self.api_url,
                headers={
                    "X-CSRF-Token": "Fetch",
                    "Accept": "application/json"
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            csrf_token = response.headers.get("X-CSRF-Token")
            
            if not csrf_token:
                raise Exception("CSRF token not found in response headers")
            
            logger.info(f"CSRF token obtained successfully")
            return session, csrf_token
            
        except Exception as e:
            logger.error(f"Failed to get CSRF token: {str(e)}")
            raise Exception(f"Failed to obtain CSRF token: {str(e)}")
    
    def _generate_transaction_id(self) -> str:
        """Generate a unique transaction ID"""
        return uuid.uuid4().hex.upper()[:32]
    
    def _prepare_payload(
        self,
        segment_data: pd.DataFrame,
        transaction_id: str,
        primary_key: str = "PRDID",
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP",
        do_commit: bool = False
    ) -> Dict[str, Any]:
        """
        Prepare POST payload for SAP IBP with flexible primary key
        
        FIXED: Added validation and proper timestamp formatting
        """
        logger.debug(f"Preparing payload for {len(segment_data)} records with primary_key={primary_key}")
        
        # Validate that primary_key exists in data
        if primary_key not in segment_data.columns:
            raise ValueError(f"Primary key {primary_key} not found in segment_data. Available: {list(segment_data.columns)}")
        
        # Identify all dimension columns (everything except XYZ_Segment and period)
        dimension_cols = [col for col in segment_data.columns 
                         if col not in ['XYZ_Segment', period_field, 'mean', 'std', 'CV', 'count']]
        
        logger.info(f"Dimension columns identified: {dimension_cols}")
        
        # Build AggregationLevelFieldsString per SAP format
        # Order: Dimensions -> Key Figure -> (NULL Flag if enabled) -> Period
        agg_fields_list = []
        
        # 1. Add all dimensions (in order: primary_key first, then others)
        agg_fields_list.append(primary_key)
        for dim in dimension_cols:
            if dim != primary_key:
                agg_fields_list.append(dim)
        
        # 2. Add key figure
        agg_fields_list.append(self.xyz_key_figure)
        
        # 3. Add NULL flag only if enabled
        if self.enable_null_handling:
            agg_fields_list.append(f"{self.xyz_key_figure}_isNull")
        
        # 4. Add period field last
        agg_fields_list.append(period_field)
        
        agg_fields = ','.join(agg_fields_list)
        logger.info(f"AggregationLevelFieldsString: {agg_fields}")
        
        # Build navigation property data
        nav_data = []
        for idx, row in segment_data.iterrows():
            record = {}
            
            # Add fields in same order as AggregationLevelFieldsString
            # 1. Dimensions (primary_key first, then others)
            if primary_key in row.index and pd.notna(row[primary_key]):
                record[primary_key] = str(row[primary_key])
            
            for dim in dimension_cols:
                if dim != primary_key and dim in row.index and pd.notna(row[dim]):
                    record[dim] = str(row[dim])
            
            # 2. Key figure (XYZ segment value)
            record[self.xyz_key_figure] = str(row['XYZ_Segment'])
            
            # 3. NULL flag (always required per SAP OData API)
            record[f"{self.xyz_key_figure}_isNull"] = False
            
            # 4. Period field
            if period_field in row.index and pd.notna(row[period_field]):
                timestamp_str = str(row[period_field])
                if 'T' not in timestamp_str:
                    timestamp_str = f"{timestamp_str}T00:00:00"
                record[period_field] = timestamp_str
            else:
                record[period_field] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
            
            nav_data.append(record)
        
        # Navigation property name format: Nav{PlanningArea}
        nav_property_name = f"Nav{self.planning_area}"
        logger.info(f"Navigation property name: {nav_property_name}")
        
        # Build main payload
        payload = {
            "Transactionid": transaction_id,
            "AggregationLevelFieldsString": agg_fields,
            nav_property_name: nav_data
        }
        
        if version_id:
            payload["VersionID"] = version_id
        
        if scenario_id:
            payload["ScenarioID"] = scenario_id
        
        if do_commit:
            payload["DoCommit"] = True
        
        logger.debug(f"Payload prepared: {len(nav_data)} records")
        logger.debug(f"Sample record: {nav_data[0] if nav_data else 'None'}")
        
        # ADDED: Log first 2 complete records for debugging
        logger.info(f"First record details: {json.dumps(nav_data[0], indent=2) if nav_data else 'None'}")
        if len(nav_data) > 1:
            logger.info(f"Second record details: {json.dumps(nav_data[1], indent=2)}")
        
        return payload
    
    def write_segments_simple(
        self,
        segment_data: pd.DataFrame,
        primary_key: str = "PRDID",
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP"
    ) -> Dict[str, Any]:
        """
        Write XYZ segments using simple single-request method
        
        FIXED: Added detailed logging for debugging
        """
        record_count = len(segment_data)
        logger.info(f"Starting simple write for {record_count} segments with primary_key={primary_key}")
        
        if record_count > 5000:
            logger.warning(f"Record count {record_count} exceeds recommended limit of 5000")
        
        # Generate transaction ID
        transaction_id = self._generate_transaction_id()
        logger.info(f"Generated transaction ID: {transaction_id}")
        
        # Prepare payload
        payload = self._prepare_payload(
            segment_data=segment_data,
            transaction_id=transaction_id,
            primary_key=primary_key,
            version_id=version_id,
            scenario_id=scenario_id,
            period_field=period_field,
            do_commit=True
        )
        
        # ADDED: Log complete payload structure (first record only for brevity)
        payload_sample = payload.copy()
        nav_key = f"Nav{self.planning_area}"
        if nav_key in payload_sample and len(payload_sample[nav_key]) > 2:
            payload_sample[nav_key] = payload_sample[nav_key][:2]  # Only first 2 records
        logger.info(f"Complete payload structure:\n{json.dumps(payload_sample, indent=2)}")
        
        # Get CSRF token
        session, csrf_token = self._get_csrf_token()
        
        # Send POST request
        url = f"{self.api_url}/{self.planning_area}Trans"
        
        try:
            logger.info(f"Sending POST to: {url}")
            logger.info(f"Request headers: Content-Type=application/json, X-CSRF-Token={csrf_token[:10]}...")
            
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token,
                    "Accept": "application/json"
                },
                timeout=self.timeout
            )
            
            # Log response details
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            logger.info(f"Write successful - Transaction ID: {transaction_id}")
            
            return {
                "status": "success",
                "transaction_id": transaction_id,
                "records_sent": record_count,
                "primary_key": primary_key,
                "message": "Data written and committed successfully"
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Write request failed: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response body: {e.response.text}")
                
                # Try to parse error details from XML
                try:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(e.response.text)
                    error_msg = root.find('.//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}message')
                    if error_msg is not None:
                        logger.error(f"SAP Error Message: {error_msg.text}")
                except:
                    pass
            
            raise Exception(f"Failed to write data to SAP: {str(e)}")
        
        finally:
            session.close()
    
    def write_segments_batched(
        self,
        segment_data: pd.DataFrame,
        primary_key: str = "PRDID",
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP",
        batch_size: int = 5000
    ) -> Dict[str, Any]:
        """
        Write XYZ segments using multi-batch method with explicit commit
        
        Args:
            segment_data: DataFrame with primary_key and XYZ_Segment columns
            primary_key: Primary key field
            version_id: Target version
            scenario_id: Target scenario
            period_field: Period timestamp field name
            batch_size: Number of records per batch (default 5000)
            
        Returns:
            Response with transaction ID, batch info, and status
        """
        record_count = len(segment_data)
        logger.info(f"Starting batched write for {record_count} segments with primary_key={primary_key}")
        
        # Get CSRF token and session
        session, csrf_token = self._get_csrf_token()
        
        try:
            # Generate transaction ID locally (similar to simple mode)
            transaction_id = self._generate_transaction_id()
            logger.info(f"Generated transaction ID: {transaction_id}")
            
            # Split data into batches
            batches = [segment_data[i:i+batch_size] for i in range(0, record_count, batch_size)]
            batch_count = len(batches)
            logger.info(f"Split into {batch_count} batches of max {batch_size} records")
            
            url = f"{self.api_url}/{self.planning_area}Trans"
            
            # Send batches
            for idx, batch in enumerate(batches, 1):
                logger.info(f"Sending batch {idx}/{batch_count} ({len(batch)} records)")
                
                payload = self._prepare_payload(
                    segment_data=batch,
                    transaction_id=transaction_id,
                    primary_key=primary_key,
                    version_id=version_id,
                    scenario_id=scenario_id,
                    period_field=period_field,
                    do_commit=False
                )
                
                try:
                    response = session.post(
                        url,
                        json=payload,
                        headers={
                            "Content-Type": "application/json",
                            "X-CSRF-Token": csrf_token
                        },
                        timeout=self.timeout
                    )
                    response.raise_for_status()
                    logger.info(f"Batch {idx}/{batch_count} sent successfully")
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Batch {idx} failed: {str(e)}")
                    raise Exception(f"Failed to send batch {idx}: {str(e)}")
            
            # Commit transaction
            logger.info("All batches sent, committing transaction")
            commit_result = self._commit_transaction(session, csrf_token, transaction_id)
            
            # Get export result
            export_result = self._get_export_result(session, csrf_token, transaction_id)
            
            return {
                "status": "success",
                "transaction_id": transaction_id,
                "records_sent": record_count,
                "batch_count": batch_count,
                "batch_size": batch_size,
                "primary_key": primary_key,
                "commit_status": commit_result,
                "export_result": export_result,
                "message": "Data written and committed in batches"
            }
        
        finally:
            # Close session
            session.close()
    
    def write_segments_parallel(
        self,
        segment_data: pd.DataFrame,
        primary_key: str = "PRDID",
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None,
        period_field: str = "PERIODID3_TSTAMP",
        batch_size: int = 5000,
        max_workers: int = 4
    ) -> Dict[str, Any]:
        """
        Write XYZ segments using parallel processing for high volumes
        
        Args:
            segment_data: DataFrame with primary_key and XYZ_Segment columns
            primary_key: Primary key field
            version_id: Target version
            scenario_id: Target scenario
            period_field: Period timestamp field name
            batch_size: Number of records per batch
            max_workers: Maximum parallel threads
            
        Returns:
            Response with transaction ID and status
        """
        record_count = len(segment_data)
        logger.info(f"Starting parallel write for {record_count} segments with primary_key={primary_key}")
        
        # Get CSRF token and session
        session, csrf_token = self._get_csrf_token()
        
        try:
            # Initiate parallel process
            transaction_id = self._initiate_parallel_process(
                session=session,
                csrf_token=csrf_token,
                version_id=version_id,
                scenario_id=scenario_id
            )
            
            # Split data into batches
            batches = [segment_data[i:i+batch_size] for i in range(0, record_count, batch_size)]
            batch_count = len(batches)
            logger.info(f"Split into {batch_count} batches for parallel processing")
            
            url = f"{self.api_url}/{self.planning_area}Trans"
            
            # Send batches in parallel
            results = []
            failed_batches = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(
                        self._send_batch_parallel,
                        url,
                        batch,
                        transaction_id,
                        csrf_token,
                        primary_key,
                        period_field,
                        idx
                    ): idx for idx, batch in enumerate(batches, 1)
                }
                
                for future in as_completed(future_to_batch):
                    batch_idx = future_to_batch[future]
                    try:
                        result = future.result()
                        results.append(result)
                        logger.info(f"Batch {batch_idx} completed successfully")
                    except Exception as e:
                        logger.error(f"Batch {batch_idx} failed: {str(e)}")
                        failed_batches.append(batch_idx)
            
            if failed_batches:
                logger.error(f"Failed batches: {failed_batches}")
                raise Exception(f"Some batches failed: {failed_batches}")
            
            # Commit transaction
            logger.info("All batches sent, committing transaction")
            commit_result = self._commit_transaction(session, csrf_token, transaction_id)
            
            # Get export result
            export_result = self._get_export_result(session, csrf_token, transaction_id)
            
            return {
                "status": "success",
                "transaction_id": transaction_id,
                "records_sent": record_count,
                "batch_count": batch_count,
                "parallel_workers": max_workers,
                "primary_key": primary_key,
                "commit_status": commit_result,
                "export_result": export_result,
                "message": "Data written in parallel and committed"
            }
        
        finally:
            session.close()
    
    def _get_transaction_id(self, session: requests.Session, csrf_token: str) -> str:
        """Get transaction ID from SAP system"""
        url = f"{self.api_url}/getTransactionID"

        try:
            logger.debug(f"Requesting transaction ID from SAP with URL: {url}")
            response = session.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response content: {response.text}")
            response.raise_for_status()

            # Parse response to extract transaction ID
            data = response.json()
            transaction_id = data.get('d', {}).get('TransactionID')

            if not transaction_id:
                raise Exception("Transaction ID not found in response")

            logger.info(f"Transaction ID obtained: {transaction_id}")
            return transaction_id

        except Exception as e:
            logger.error(f"Failed to get transaction ID: {str(e)}")
            raise
    
    def _commit_transaction(self, session: requests.Session, csrf_token: str, transaction_id: str) -> Dict[str, Any]:
        """Commit a transaction"""
        url = f"{self.api_url}/commit"
        
        payload = {"Transactionid": transaction_id}
        
        try:
            logger.info(f"Committing transaction: {transaction_id}")
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info("Transaction committed successfully")
            return {
                "status": "committed",
                "transaction_id": transaction_id
            }
            
        except Exception as e:
            logger.error(f"Commit failed: {str(e)}")
            raise Exception(f"Failed to commit transaction: {str(e)}")
    
    def _get_export_result(self, session: requests.Session, csrf_token: str, transaction_id: str) -> Dict[str, Any]:
        """Get export/import result status"""
        url = f"{self.api_url}/GetExportResult"
        
        params = {"Transactionid": transaction_id}
        
        try:
            logger.debug(f"Getting export result for transaction: {transaction_id}")
            response = session.get(
                url,
                params=params,
                headers={"X-CSRF-Token": csrf_token},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info("Export result retrieved successfully")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to get export result: {str(e)}")
            return {"status": "unknown", "error": str(e)}
    
    def get_messages(self, transaction_id: str) -> List[Dict[str, Any]]:
        """Get error messages for a transaction"""
        # Create new session for this request
        session, csrf_token = self._get_csrf_token()
        
        try:
            url = f"{self.api_url}/Message"
            params = {"Transactionid": transaction_id}
            
            logger.debug(f"Getting messages for transaction: {transaction_id}")
            response = session.get(
                url,
                params=params,
                headers={"X-CSRF-Token": csrf_token},
                timeout=self.timeout
            )
            response.raise_for_status()
            
            messages = response.json()
            logger.info("Messages retrieved successfully")
            return messages
            
        except Exception as e:
            logger.warning(f"Failed to get messages: {str(e)}")
            return []
        
        finally:
            session.close()
    
    def _initiate_parallel_process(
        self,
        session: requests.Session,
        csrf_token: str,
        version_id: Optional[str] = None,
        scenario_id: Optional[str] = None
    ) -> str:
        """Initiate parallel processing and get transaction ID"""
        url = f"{self.api_url}/InitiateParallelProcess"
        
        payload = {
            "PlanningArea": self.planning_area
        }
        
        if version_id:
            payload["VersionID"] = version_id
        
        if scenario_id:
            payload["ScenarioID"] = scenario_id
        
        try:
            logger.info("Initiating parallel process")
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            transaction_id = data.get('d', {}).get('TransactionID')
            
            if not transaction_id:
                raise Exception("Transaction ID not found in response")
            
            logger.info(f"Parallel process initiated with transaction ID: {transaction_id}")
            return transaction_id
            
        except Exception as e:
            logger.error(f"Failed to initiate parallel process: {str(e)}")
            raise
    
    def _send_batch_parallel(
        self,
        url: str,
        batch: pd.DataFrame,
        transaction_id: str,
        csrf_token: str,
        primary_key: str,
        period_field: str,
        batch_idx: int
    ) -> Dict[str, Any]:
        """Send a single batch in parallel processing"""
        # Create new session for this thread
        session = requests.Session()
        session.auth = (self.username, self.password)
        
        try:
            payload = self._prepare_payload(
                segment_data=batch,
                transaction_id=transaction_id,
                primary_key=primary_key,
                period_field=period_field,
                do_commit=False
            )
            
            response = session.post(
                url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-CSRF-Token": csrf_token
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            
            return {
                "batch_idx": batch_idx,
                "records": len(batch),
                "status": "success"
            }
        
        finally:
            session.close()