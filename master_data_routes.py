"""
API Routes for SAP IBP Master Data Operations
"""
from flask import Blueprint, request, jsonify
from services.master_data_service import MasterDataService
from utils.validators import validate_master_data_extraction_request, validate_master_data_import_request
from utils.logger import get_logger

logger = get_logger(__name__)

# Master Data blueprint
master_data_bp = Blueprint('master_data', __name__)

# ==================== EXTRACTION ENDPOINTS ====================

@master_data_bp.route('/extract', methods=['POST'])
def extract_master_data():
    """
    Extract master data from SAP IBP
    
    Expected JSON payload:
    {
        "master_data_type": "LOCATIONPRODUCT",
        "select": "PRDID,LOCID",
        "filter": "PRDID eq 'IBP-100' and LOCID eq 'Frankfurt'",
        "format": "json",
        "top": 1000,
        "skip": 0,
        "orderby": "PRDID",
        "inlinecount": "allpages"
    }
    """
    try:
        data = request.get_json()
        
        # Validate request
        is_valid, error_msg = validate_master_data_extraction_request(data)
        if not is_valid:
            return jsonify({'error': error_msg}), 400
        
        # Extract data
        service = MasterDataService()
        result = service.extract_master_data(
            master_data_type=data['master_data_type'],
            select=data.get('select'),
            filter_str=data.get('filter'),
            orderby=data.get('orderby'),
            top=data.get('top'),
            skip=data.get('skip'),
            format_type=data.get('format', 'json'),
            inlinecount=data.get('inlinecount')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error extracting master data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/extract/paginated', methods=['POST'])
def extract_master_data_paginated():
    """
    Extract master data with automatic pagination
    
    Expected JSON payload:
    {
        "master_data_type": "LOCATION",
        "select": "LOCID,LOCREGION,LOCDESCR",
        "filter": "LOCREGION eq 'A'",
        "page_size": 50000,
        "max_records": 100000
    }
    """
    try:
        data = request.get_json()
        
        # Validate master data type
        if 'master_data_type' not in data:
            return jsonify({'error': 'master_data_type is required'}), 400
        
        service = MasterDataService()
        result = service.extract_master_data_paginated(
            master_data_type=data['master_data_type'],
            select=data.get('select'),
            filter_str=data.get('filter'),
            page_size=data.get('page_size', 50000),
            max_records=data.get('max_records')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error in paginated extraction: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/extract/version-specific', methods=['POST'])
def extract_version_specific_master_data():
    """
    Extract version-specific master data
    
    Expected JSON payload:
    {
        "master_data_type": "CUSTOMER",
        "planning_area_id": "SAPIBP1",
        "version_id": "NEWPRODUCT",
        "select": "CUSTID,CUSTNAME",
        "filter": "CUSTREGION eq 'US'",
        "top": 1000,
        "skip": 0
    }
    
    Note: Use "__BASELINE" or empty string for baseline version
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['master_data_type', 'planning_area_id']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'{field} is required'}), 400
        
        service = MasterDataService()
        result = service.extract_version_specific_master_data(
            master_data_type=data['master_data_type'],
            planning_area_id=data['planning_area_id'],
            version_id=data.get('version_id', ''),
            select=data.get('select'),
            filter_str=data.get('filter'),
            top=data.get('top'),
            skip=data.get('skip')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error extracting version-specific master data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/version-specific/list', methods=['GET'])
def list_version_specific_master_data_types():
    """
    List all version-specific master data types
    """
    try:
        service = MasterDataService()
        result = service.list_version_specific_master_data_types()
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error listing version-specific master data types: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== IMPORT ENDPOINTS ====================

@master_data_bp.route('/import', methods=['POST'])
def import_master_data():
    """
    Import master data to SAP IBP
    
    Expected JSON payload:
    {
        "master_data_type": "LOCATION",
        "requested_attributes": "LOCID,LOCREGION,LOCDESCR",
        "data": [
            {
                "LOCID": "2000",
                "LOCREGION": "A",
                "LOCDESCR": "New York"
            },
            {
                "LOCID": "2001",
                "LOCREGION": "B",
                "LOCDESCR": "Los Angeles"
            }
        ],
        "planning_area_id": "USAIBP1",
        "version_id": "NEWPRODUCT",
        "do_commit": true,
        "transaction_id": "1111222233334444",
        "transaction_name": "Location Import"
    }
    
    Note: If you don't provide a value for an attribute in RequestedAttributes,
          the service will delete that attribute's value.
    """
    try:
        payload = request.get_json()
        
        # Validate request
        is_valid, error_msg = validate_master_data_import_request(payload)
        if not is_valid:
            return jsonify({'error': error_msg}), 400
        
        service = MasterDataService()
        result = service.import_master_data(
            master_data_type=payload['master_data_type'],
            requested_attributes=payload['requested_attributes'],
            data=payload['data'],
            planning_area_id=payload.get('planning_area_id'),
            version_id=payload.get('version_id'),
            do_commit=payload.get('do_commit', True),
            transaction_id=payload.get('transaction_id'),
            transaction_name=payload.get('transaction_name')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error importing master data: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/import/parallel', methods=['POST'])
def import_master_data_parallel():
    """
    Import master data using parallel method for high volumes
    
    Expected JSON payload:
    {
        "master_data_type": "PRODUCT",
        "requested_attributes": "PRDID,PRDDESCR",
        "transaction_id": "4444555566667777",
        "data": [
            {
                "PRDID": "PRD01",
                "PRDDESCR": "AB Shoes"
            },
            {
                "PRDID": "PRD02",
                "PRDDESCR": "CD Hoodie"
            }
        ],
        "planning_area_id": "USAIBP1",
        "version_id": "NEWPRODUCT",
        "transaction_name": "Product Import"
    }
    
    Note: DoCommit parameter cannot be used with parallel requests.
          The transaction will be committed automatically at the end.
    """
    try:
        payload = request.get_json()
        
        # Validate required fields
        required_fields = ['master_data_type', 'requested_attributes', 'transaction_id', 'data']
        for field in required_fields:
            if field not in payload:
                return jsonify({'error': f'{field} is required'}), 400
        
        service = MasterDataService()
        result = service.import_master_data_parallel(
            master_data_type=payload['master_data_type'],
            requested_attributes=payload['requested_attributes'],
            data=payload['data'],
            transaction_id=payload['transaction_id'],
            planning_area_id=payload.get('planning_area_id'),
            version_id=payload.get('version_id'),
            transaction_name=payload.get('transaction_name')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error in parallel master data import: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/delete', methods=['POST'])
def delete_master_data():
    """
    Delete master data records from SAP IBP
    
    Expected JSON payload:
    {
        "master_data_type": "LOCATIONPRODUCT",
        "requested_attributes": "LOCID,PRDID",
        "data": [
            {
                "LOCID": "2002",
                "PRDID": "PRD01"
            },
            {
                "LOCID": "2003",
                "PRDID": "PRD02"
            }
        ],
        "planning_area_id": "USAIBP1",
        "version_id": "NEWPRODUCT",
        "do_commit": true,
        "transaction_id": "5555666677778888"
    }
    
    Note: 
    - RequestedAttributes must include all key attributes
    - The service deletes entire records (partial deletion not supported)
    - If VersionID is not provided, deletes from base version
    """
    try:
        payload = request.get_json()
        
        # Validate required fields
        required_fields = ['master_data_type', 'requested_attributes', 'data']
        for field in required_fields:
            if field not in payload:
                return jsonify({'error': f'{field} is required'}), 400
        
        service = MasterDataService()
        result = service.delete_master_data(
            master_data_type=payload['master_data_type'],
            requested_attributes=payload['requested_attributes'],
            data=payload['data'],
            planning_area_id=payload.get('planning_area_id'),
            version_id=payload.get('version_id'),
            do_commit=payload.get('do_commit', True),
            transaction_id=payload.get('transaction_id')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error deleting master data: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== STATUS AND ERROR ENDPOINTS ====================

@master_data_bp.route('/status/<transaction_id>', methods=['GET'])
def get_import_status(transaction_id):
    """
    Get master data import transaction status
    """
    try:
        service = MasterDataService()
        result = service.get_import_status(transaction_id)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error getting import status: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/messages/<transaction_id>', methods=['GET'])
def get_import_messages(transaction_id):
    """
    Get error messages for master data import transaction
    
    Query parameters:
    - master_data_type: Master data type to expand error details
    - top: Limit number of messages
    - skip: Skip number of messages
    """
    try:
        master_data_type = request.args.get('master_data_type')
        top = request.args.get('top', type=int)
        skip = request.args.get('skip', type=int)
        
        service = MasterDataService()
        result = service.get_error_messages(
            transaction_id=transaction_id,
            master_data_type=master_data_type,
            top=top,
            skip=skip
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error getting import messages: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

@master_data_bp.route('/discover/metadata', methods=['GET'])
def get_master_data_metadata():
    """
    Get the OData service metadata to discover available master data types
    This will show all entity sets (master data types) available in your system
    """
    try:
        from clients.sap_client import SAPClient
        from config import Config
        
        client = SAPClient()
        master_data_base_url = f"https://{Config.SAP_IBP_HOST}/sap/opu/odata/IBP/MASTER_DATA_API_SRV"
        
        # Temporarily override base URL
        original_base_url = client.base_url
        client.base_url = master_data_base_url
        
        try:
            # Get metadata in XML format
            response = client.get('/$metadata', params={})
            
            # Try to parse entity sets from metadata
            import re
            entity_sets = re.findall(r'<EntitySet Name="([^"]+)"', str(response))
            
            return jsonify({
                'status': 'success',
                'entity_sets': entity_sets,
                'message': 'These are the available master data types in your system',
                'note': 'Use these names in the master_data_type field'
            }), 200
            
        finally:
            client.base_url = original_base_url
            
    except Exception as e:
        logger.error(f"Error getting metadata: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/discover/collections', methods=['GET'])
def get_master_data_collections():
    """
    Get the OData service document to see all available collections
    This is a simpler way to discover available master data types
    """
    try:
        from clients.sap_client import SAPClient
        from config import Config
        
        client = SAPClient()
        master_data_base_url = f"https://{Config.SAP_IBP_HOST}/sap/opu/odata/IBP/MASTER_DATA_API_SRV"
        
        # Temporarily override base URL
        original_base_url = client.base_url
        client.base_url = master_data_base_url
        
        try:
            # Get service document in JSON format
            response = client.get('/', params={'$format': 'json'})
            
            # Extract entity sets from response
            entity_sets = []
            if 'd' in response and 'EntitySets' in response['d']:
                entity_sets = response['d']['EntitySets']
            
            return jsonify({
                'status': 'success',
                'master_data_types': entity_sets,
                'count': len(entity_sets),
                'message': 'Use these names in your API requests'
            }), 200
            
        finally:
            client.base_url = original_base_url
            
    except Exception as e:
        logger.error(f"Error getting collections: {str(e)}")
        return jsonify({'error': str(e)}), 500


@master_data_bp.route('/discover/test/<master_data_type>', methods=['GET'])
def test_master_data_type(master_data_type):
    """
    Test if a specific master data type exists and can be queried
    Returns sample data (first 5 records) if successful
    """
    try:
        service = MasterDataService()
        
        # Try to extract just 5 records without any filters
        result = service.extract_master_data(
            master_data_type=master_data_type,
            top=5,
            format_type='json'
        )
        
        record_count = len(result.get('d', {}).get('results', []))
        
        return jsonify({
            'status': 'success',
            'master_data_type': master_data_type,
            'exists': True,
            'sample_records': record_count,
            'sample_data': result
        }), 200
        
    except Exception as e:
        error_msg = str(e)
        
        if '404' in error_msg:
            return jsonify({
                'status': 'not_found',
                'master_data_type': master_data_type,
                'exists': False,
                'message': f"Master data type '{master_data_type}' does not exist in your system",
                'suggestion': "Use /api/master-data/discover/collections to see available types"
            }), 404
        else:
            return jsonify({
                'status': 'error',
                'master_data_type': master_data_type,
                'error': error_msg
            }), 500