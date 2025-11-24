"""
API Routes for SAP IBP Integration
"""
from flask import Blueprint, request, jsonify
from services.extraction_service import ExtractionService
from services.import_service import ImportService
from utils.validators import validate_extraction_request, validate_import_request
from utils.logger import get_logger

logger = get_logger(__name__)

# Health check blueprint
health_bp = Blueprint('health', __name__)

@health_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'SAP IBP Integration'}), 200

@health_bp.route('/test-connection', methods=['GET'])
def test_connection():
    """
    Test SAP IBP connection and authentication
    Returns diagnostic information about the connection
    """
    try:
        from clients.sap_client import SAPClient
        from config import Config
        import requests
        from requests.auth import HTTPBasicAuth
        
        results = {
            'config_check': {},
            'connection_test': {},
            'auth_test': {}
        }
        
        # Check configuration
        results['config_check'] = {
            'host': Config.SAP_IBP_HOST,
            'base_url': Config.SAP_IBP_BASE_URL,
            'username_set': bool(Config.SAP_IBP_USERNAME),
            'password_set': bool(Config.SAP_IBP_PASSWORD),
            'username_preview': Config.SAP_IBP_USERNAME[:4] + '***' if Config.SAP_IBP_USERNAME else 'NOT SET'
        }
        
        # Test basic connectivity (without auth)
        try:
            response = requests.head(
                f"https://{Config.SAP_IBP_HOST}",
                timeout=10,
                allow_redirects=True
            )
            results['connection_test'] = {
                'status': 'success',
                'status_code': response.status_code,
                'reachable': True
            }
        except Exception as e:
            results['connection_test'] = {
                'status': 'failed',
                'error': str(e),
                'reachable': False
            }
        
        # Test authentication with metadata endpoint
        try:
            auth = HTTPBasicAuth(Config.SAP_IBP_USERNAME, Config.SAP_IBP_PASSWORD)
            response = requests.get(
                f"{Config.SAP_IBP_BASE_URL}/$metadata",
                auth=auth,
                timeout=30,
                headers={'Accept': 'application/xml'}
            )
            
            results['auth_test'] = {
                'status': 'success' if response.status_code == 200 else 'failed',
                'status_code': response.status_code,
                'authenticated': response.status_code == 200,
                'content_type': response.headers.get('content-type', 'unknown')
            }
            
            if response.status_code != 200:
                results['auth_test']['error_preview'] = response.text[:500]
                
        except Exception as e:
            results['auth_test'] = {
                'status': 'failed',
                'error': str(e),
                'authenticated': False
            }
        
        return jsonify(results), 200
        
    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Extraction blueprint
extraction_bp = Blueprint('extraction', __name__)

@extraction_bp.route('/keyfigures', methods=['POST'])
def extract_keyfigures():
    """
    Extract key figure data from SAP IBP
    
    Expected JSON payload:
    {
        "planning_area": "SAPIBP1",
        "select": "PRDID,LOCID,DEMANDQTY,PERIODID3_TSTAMP",
        "filter": "PRDID eq 'IBP-100' and LOCID eq '1720'",
        "format": "json",
        "top": 1000,
        "skip": 0
    }
    """
    try:
        data = request.get_json()
        
        # Validate request
        is_valid, error_msg = validate_extraction_request(data)
        if not is_valid:
            return jsonify({'error': error_msg}), 400
        
        # Extract data
        service = ExtractionService()
        result = service.extract_keyfigures(
            planning_area=data['planning_area'],
            select=data['select'],
            filter_str=data.get('filter'),
            format_type=data.get('format', 'json'),
            top=data.get('top'),
            skip=data.get('skip'),
            orderby=data.get('orderby')
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error extracting key figures: {str(e)}")
        return jsonify({'error': str(e)}), 500

@extraction_bp.route('/delta/create', methods=['POST'])
def create_delta_query():
    """
    Create a key figure delta query definition
    
    Expected JSON payload:
    {
        "delta_query_id": "FCST_KF_DELTA_LOAD1",
        "description": "Forecasting Key Figure Delta Query",
        "planning_area": "SAPIBP1",
        "select": "PRDID,LOCID,PERIODID4_TSTAMP,CONSENSUSDEMANDQTY",
        "filter": "PRDID eq 'IBP-100' and LOCID eq '1720'"
    }
    """
    try:
        data = request.get_json()
        
        service = ExtractionService()
        result = service.create_delta_query(
            delta_query_id=data['delta_query_id'],
            description=data['description'],
            planning_area=data['planning_area'],
            select=data['select'],
            filter_str=data['filter']
        )
        
        return jsonify(result), 201
        
    except Exception as e:
        logger.error(f"Error creating delta query: {str(e)}")
        return jsonify({'error': str(e)}), 500

@extraction_bp.route('/delta/execute', methods=['POST'])
def execute_delta_load():
    """
    Execute delta load for a given delta query ID
    
    Expected JSON payload:
    {
        "delta_query_id": "FCST_KF_DELTA_LOAD1"
    }
    """
    try:
        data = request.get_json()
        delta_query_id = data.get('delta_query_id')
        
        if not delta_query_id:
            return jsonify({'error': 'delta_query_id is required'}), 400
        
        service = ExtractionService()
        result = service.execute_delta_load(delta_query_id)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error executing delta load: {str(e)}")
        return jsonify({'error': str(e)}), 500

@extraction_bp.route('/delta/extract', methods=['POST'])
def extract_delta_data():
    """
    Extract delta data
    
    Expected JSON payload:
    {
        "delta_query_id": "FCST_KF_DELTA_LOAD1",
        "planning_area": "SAPIBP1",
        "select": "PRDID,LOCID,PERIODID4_TSTAMP,CONSENSUSDEMANDQTY",
        "top": 1000,
        "skip": 0
    }
    """
    try:
        data = request.get_json()
        
        service = ExtractionService()
        result = service.extract_delta_data(
            delta_query_id=data['delta_query_id'],
            planning_area=data['planning_area'],
            select=data['select'],
            top=data.get('top', 1000),
            skip=data.get('skip', 0)
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error extracting delta data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@extraction_bp.route('/delta/confirm', methods=['POST'])
def confirm_delta_load():
    """Confirm delta load completion"""
    try:
        data = request.get_json()
        delta_query_id = data.get('delta_query_id')
        
        service = ExtractionService()
        result = service.confirm_delta_load(delta_query_id)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error confirming delta load: {str(e)}")
        return jsonify({'error': str(e)}), 500

# Import blueprint
import_bp = Blueprint('import', __name__)

@import_bp.route('/keyfigures', methods=['POST'])
def import_keyfigures():
    """
    Import key figure data to SAP IBP
    
    Expected JSON payload:
    {
        "planning_area": "SAPIBP1",
        "version_id": "UPSIDE",
        "scenario_id": null,
        "aggregation_level_fields": "LOCID,PRDID,CONSENSUSDEMAND,PERIODID4_TSTAMP",
        "data": [
            {
                "LOCID": "1720",
                "PRDID": "IBP-100",
                "CONSENSUSDEMAND": "100",
                "PERIODID4_TSTAMP": "2020-10-07T16:07:30"
            }
        ],
        "use_parallel": false,
        "auto_commit": true
    }
    """
    try:
        payload = request.get_json()
        
        # Validate request
        is_valid, error_msg = validate_import_request(payload)
        if not is_valid:
            return jsonify({'error': error_msg}), 400
        
        service = ImportService()
        
        if payload.get('use_parallel', False):
            result = service.import_parallel(
                planning_area=payload['planning_area'],
                version_id=payload.get('version_id'),
                scenario_id=payload.get('scenario_id'),
                aggregation_level_fields=payload['aggregation_level_fields'],
                data=payload['data']
            )
        else:
            result = service.import_standard(
                planning_area=payload['planning_area'],
                version_id=payload.get('version_id'),
                scenario_id=payload.get('scenario_id'),
                aggregation_level_fields=payload['aggregation_level_fields'],
                data=payload['data'],
                auto_commit=payload.get('auto_commit', True)
            )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error importing key figures: {str(e)}")
        return jsonify({'error': str(e)}), 500

@import_bp.route('/status/<transaction_id>', methods=['GET'])
def get_import_status(transaction_id):
    """Get import transaction status"""
    try:
        service = ImportService()
        result = service.get_import_status(transaction_id)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error getting import status: {str(e)}")
        return jsonify({'error': str(e)}), 500

@import_bp.route('/messages/<transaction_id>', methods=['GET'])
def get_import_messages(transaction_id):
    """Get error messages for import transaction"""
    try:
        service = ImportService()
        result = service.get_error_messages(transaction_id)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error getting import messages: {str(e)}")
        return jsonify({'error': str(e)}), 500
    

"""
Add these discovery endpoints to your routes.py file
to help identify available planning areas and key figures
"""

@extraction_bp.route('/discover/planning-areas', methods=['GET'])
def discover_planning_areas():
    """
    Discover all available planning areas in the system
    """
    try:
        from clients.sap_client import SAPClient
        from config import Config
        
        client = SAPClient()
        
        # Get service document to see all available entity sets (planning areas)
        response = client.get('/', params={'$format': 'json'})
        
        # Extract entity sets (these are your planning areas)
        entity_sets = []
        if 'd' in response and 'EntitySets' in response['d']:
            entity_sets = response['d']['EntitySets']
        
        # Filter out special entity sets that are not planning areas
        special_sets = ['Message', 'KeyFigureDeltaDefinitionSet', 'GetExportResult']
        planning_areas = [e for e in entity_sets if e not in special_sets]
        
        return jsonify({
            'status': 'success',
            'planning_areas': planning_areas,
            'count': len(planning_areas),
            'message': 'These are the available planning areas in your system'
        }), 200
        
    except Exception as e:
        logger.error(f"Error discovering planning areas: {str(e)}")
        return jsonify({'error': str(e)}), 500


@extraction_bp.route('/discover/metadata/<planning_area>', methods=['GET'])
def discover_planning_area_metadata(planning_area):
    """
    Get metadata for a specific planning area to see available key figures
    This will show all properties (key figures) available in the planning area
    """
    try:
        from clients.sap_client import SAPClient
        import re
        
        client = SAPClient()
        
        # Get full metadata
        response = client.get('/$metadata', params={})
        
        # Convert response to string for parsing
        metadata_str = str(response)
        
        # Find the EntityType definition for this planning area
        entity_pattern = rf'<EntityType Name="{planning_area}"[^>]*>(.*?)</EntityType>'
        entity_match = re.search(entity_pattern, metadata_str, re.DOTALL)
        
        if not entity_match:
            return jsonify({
                'status': 'not_found',
                'planning_area': planning_area,
                'message': f'Planning area {planning_area} not found in metadata',
                'suggestion': 'Use /api/extract/discover/planning-areas to see available planning areas'
            }), 404
        
        entity_content = entity_match.group(1)
        
        # Extract all properties (key figures and dimensions)
        property_pattern = r'<Property Name="([^"]+)"[^>]*Type="([^"]+)"[^>]*(?:Nullable="([^"]+)")?'
        properties = re.findall(property_pattern, entity_content)
        
        # Organize properties
        key_figures = []
        dimensions = []
        
        for prop_name, prop_type, nullable in properties:
            prop_info = {
                'name': prop_name,
                'type': prop_type,
                'nullable': nullable if nullable else 'true'
            }
            
            # Key figures typically have numeric types
            if 'Decimal' in prop_type or 'Double' in prop_type or 'Int' in prop_type:
                key_figures.append(prop_info)
            else:
                dimensions.append(prop_info)
        
        return jsonify({
            'status': 'success',
            'planning_area': planning_area,
            'key_figures': key_figures,
            'dimensions': dimensions,
            'total_properties': len(properties),
            'message': 'Use these property names in your select parameter'
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting metadata for {planning_area}: {str(e)}")
        return jsonify({'error': str(e)}), 500


@extraction_bp.route('/discover/test/<planning_area>', methods=['GET'])
def test_planning_area(planning_area):
    """
    Test if a planning area exists and get sample data
    Returns first 5 records with all fields
    """
    try:
        from services.extraction_service import ExtractionService
        
        service = ExtractionService()
        
        # Try to extract just 5 records without selecting specific fields
        result = service.extract_keyfigures(
            planning_area=planning_area,
            select=None,  # Get all fields
            top=5
        )
        
        records = result.get('d', {}).get('results', [])
        
        # Extract field names from first record
        field_names = []
        if records:
            field_names = list(records[0].keys())
            # Remove metadata fields
            field_names = [f for f in field_names if not f.startswith('__')]
        
        return jsonify({
            'status': 'success',
            'planning_area': planning_area,
            'exists': True,
            'sample_records': len(records),
            'available_fields': field_names,
            'sample_data': records
        }), 200
        
    except Exception as e:
        error_msg = str(e)
        
        if '404' in error_msg:
            return jsonify({
                'status': 'not_found',
                'planning_area': planning_area,
                'exists': False,
                'message': f"Planning area '{planning_area}' does not exist",
                'suggestion': "Use /api/extract/discover/planning-areas to see available planning areas"
            }), 404
        else:
            return jsonify({
                'status': 'error',
                'planning_area': planning_area,
                'error': error_msg
            }), 500


@extraction_bp.route('/discover/key-figures/<planning_area>', methods=['GET'])
def discover_key_figures_simple(planning_area):
    """
    Simple way to discover key figures: extract one record and see all fields
    This is often more reliable than parsing metadata
    """
    try:
        from services.extraction_service import ExtractionService
        
        service = ExtractionService()
        
        # Extract one record to see structure
        result = service.extract_keyfigures(
            planning_area=planning_area,
            select=None,
            top=1
        )
        
        records = result.get('d', {}).get('results', [])
        
        if not records:
            return jsonify({
                'status': 'success',
                'planning_area': planning_area,
                'message': 'Planning area exists but contains no data',
                'suggestion': 'Try importing data first or use /api/extract/discover/metadata/' + planning_area
            }), 200
        
        # Get all field names and their sample values
        first_record = records[0]
        fields = {}
        
        for key, value in first_record.items():
            if not key.startswith('__'):  # Skip metadata fields
                fields[key] = {
                    'sample_value': value,
                    'type': type(value).__name__
                }
        
        # Separate into likely dimensions and key figures
        dimensions = {}
        key_figures = {}
        
        for field, info in fields.items():
            if isinstance(info['sample_value'], (int, float)) and not field.endswith('ID'):
                key_figures[field] = info
            else:
                dimensions[field] = info
        
        return jsonify({
            'status': 'success',
            'planning_area': planning_area,
            'all_fields': list(fields.keys()),
            'likely_dimensions': list(dimensions.keys()),
            'likely_key_figures': list(key_figures.keys()),
            'field_details': fields,
            'message': 'Use these field names in your select parameter'
        }), 200
        
    except Exception as e:
        logger.error(f"Error discovering key figures for {planning_area}: {str(e)}")
        return jsonify({'error': str(e)}), 500