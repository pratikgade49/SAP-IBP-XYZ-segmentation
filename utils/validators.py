"""
Validators for API request payloads
"""
from typing import Dict, Tuple, Any

def validate_extraction_request(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate extraction request payload
    
    Args:
        data: Request data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['planning_area', 'select']
    
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    
    # Validate planning area format
    if not isinstance(data['planning_area'], str):
        return False, "planning_area must be a string"
    
    # Validate select format
    if not isinstance(data['select'], str):
        return False, "select must be a comma-separated string"
    
    # Validate optional numeric fields
    if 'top' in data and data['top'] is not None:
        if not isinstance(data['top'], int) or data['top'] < 0:
            return False, "top must be a positive integer"
    
    if 'skip' in data and data['skip'] is not None:
        if not isinstance(data['skip'], int) or data['skip'] < 0:
            return False, "skip must be a positive integer"
    
    return True, ""

def validate_import_request(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate import request payload
    
    Args:
        data: Request data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['planning_area', 'aggregation_level_fields', 'data']
    
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    
    # Validate planning area
    if not isinstance(data['planning_area'], str):
        return False, "planning_area must be a string"
    
    # Validate aggregation level fields
    if not isinstance(data['aggregation_level_fields'], str):
        return False, "aggregation_level_fields must be a comma-separated string"
    
    # Validate data
    if not isinstance(data['data'], list):
        return False, "data must be a list of records"
    
    if len(data['data']) == 0:
        return False, "data cannot be empty"
    
    # Validate each data record is a dictionary
    for i, record in enumerate(data['data']):
        if not isinstance(record, dict):
            return False, f"data[{i}] must be a dictionary"
    
    # Validate version_id if provided
    if 'version_id' in data and data['version_id'] is not None:
        if not isinstance(data['version_id'], str):
            return False, "version_id must be a string"
    
    # Validate scenario_id if provided
    if 'scenario_id' in data and data['scenario_id'] is not None:
        if not isinstance(data['scenario_id'], str):
            return False, "scenario_id must be a string"
    
    return True, ""

def validate_delta_query_request(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate delta query creation request
    
    Args:
        data: Request data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['delta_query_id', 'description', 'planning_area', 'select', 'filter']
    
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    
    # Validate all fields are strings
    for field in required_fields:
        if not isinstance(data[field], str):
            return False, f"{field} must be a string"
    
    # Validate select contains required fields
    select_fields = data['select'].split(',')
    if len(select_fields) < 2:
        return False, "select must contain at least 2 fields"
    
    # Validate filter contains time period
    if 'PERIODID' not in data['filter']:
        return False, "filter must contain a time period filter (PERIODID)"
    
    return True, ""


def validate_master_data_extraction_request(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate master data extraction request payload
    
    Args:
        data: Request data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required field
    if 'master_data_type' not in data or not data['master_data_type']:
        return False, "Missing required field: master_data_type"
    
    # Validate master data type format
    if not isinstance(data['master_data_type'], str):
        return False, "master_data_type must be a string"
    
    # Validate optional fields
    if 'select' in data and data['select'] is not None:
        if not isinstance(data['select'], str):
            return False, "select must be a comma-separated string"
    
    if 'filter' in data and data['filter'] is not None:
        if not isinstance(data['filter'], str):
            return False, "filter must be a string"
        
        # Check for complex conditions (not allowed per documentation)
        if '(' in data['filter'] and ' or ' in data['filter'].lower():
            return False, "Complex filter conditions with 'or' operator and brackets are not allowed"
    
    if 'top' in data and data['top'] is not None:
        if not isinstance(data['top'], int) or data['top'] < 0:
            return False, "top must be a positive integer"
        
        # Warn if exceeding recommended limit
        if data['top'] > 50000:
            logger = __import__('utils.logger', fromlist=['get_logger']).get_logger(__name__)
            logger.warning(f"top value of {data['top']} exceeds recommended limit of 50,000 records")
    
    if 'skip' in data and data['skip'] is not None:
        if not isinstance(data['skip'], int) or data['skip'] < 0:
            return False, "skip must be a positive integer"
    
    if 'orderby' in data and data['orderby'] is not None:
        if not isinstance(data['orderby'], str):
            return False, "orderby must be a string"
    
    if 'format' in data and data['format'] is not None:
        if data['format'] not in ['json', 'xml']:
            return False, "format must be either 'json' or 'xml'"
    
    if 'inlinecount' in data and data['inlinecount'] is not None:
        if data['inlinecount'] not in ['allpages', 'none']:
            return False, "inlinecount must be either 'allpages' or 'none'"
    
    return True, ""


def validate_master_data_import_request(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate master data import request payload
    
    Args:
        data: Request data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['master_data_type', 'requested_attributes', 'data']
    
    # Check required fields
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    
    # Validate master data type
    if not isinstance(data['master_data_type'], str):
        return False, "master_data_type must be a string"
    
    # Validate requested attributes
    if not isinstance(data['requested_attributes'], str):
        return False, "requested_attributes must be a comma-separated string"
    
    # Validate data
    if not isinstance(data['data'], list):
        return False, "data must be a list of records"
    
    if len(data['data']) == 0:
        return False, "data cannot be empty"
    
    # Warn if data exceeds recommended batch size
    if len(data['data']) > 5000:
        logger = __import__('utils.logger', fromlist=['get_logger']).get_logger(__name__)
        logger.warning(f"data contains {len(data['data'])} records, which exceeds recommended batch size of 5000")
    
    # Validate each data record is a dictionary
    for i, record in enumerate(data['data']):
        if not isinstance(record, dict):
            return False, f"data[{i}] must be a dictionary"
    
    # Validate optional fields
    if 'planning_area_id' in data and data['planning_area_id'] is not None:
        if not isinstance(data['planning_area_id'], str):
            return False, "planning_area_id must be a string"
    
    if 'version_id' in data and data['version_id'] is not None:
        if not isinstance(data['version_id'], str):
            return False, "version_id must be a string"
    
    if 'do_commit' in data and data['do_commit'] is not None:
        if not isinstance(data['do_commit'], bool):
            return False, "do_commit must be a boolean"
    
    if 'transaction_id' in data and data['transaction_id'] is not None:
        if not isinstance(data['transaction_id'], str):
            return False, "transaction_id must be a string"
        
        if len(data['transaction_id']) > 32:
            return False, "transaction_id must be maximum 32 characters"
    
    if 'transaction_name' in data and data['transaction_name'] is not None:
        if not isinstance(data['transaction_name'], str):
            return False, "transaction_name must be a string"
        
        if len(data['transaction_name']) > 60:
            return False, "transaction_name must be maximum 60 characters"
    
    return True, ""


def validate_master_data_delete_request(data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate master data deletion request payload
    
    Args:
        data: Request data
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['master_data_type', 'requested_attributes', 'data']
    
    # Check required fields
    for field in required_fields:
        if field not in data or not data[field]:
            return False, f"Missing required field: {field}"
    
    # Validate master data type
    if not isinstance(data['master_data_type'], str):
        return False, "master_data_type must be a string"
    
    # Validate requested attributes (must include all key fields for deletion)
    if not isinstance(data['requested_attributes'], str):
        return False, "requested_attributes must be a comma-separated string containing all key fields"
    
    # Validate data
    if not isinstance(data['data'], list):
        return False, "data must be a list of records to delete"
    
    if len(data['data']) == 0:
        return False, "data cannot be empty"
    
    # Validate each data record is a dictionary with key fields
    for i, record in enumerate(data['data']):
        if not isinstance(record, dict):
            return False, f"data[{i}] must be a dictionary"
        
        # Verify that all requested key attributes are present
        requested_attrs = [attr.strip() for attr in data['requested_attributes'].split(',')]
        for attr in requested_attrs:
            if attr not in record:
                return False, f"data[{i}] is missing required key attribute: {attr}"
    
    # Validate optional fields
    if 'planning_area_id' in data and data['planning_area_id'] is not None:
        if not isinstance(data['planning_area_id'], str):
            return False, "planning_area_id must be a string"
    
    if 'version_id' in data and data['version_id'] is not None:
        if not isinstance(data['version_id'], str):
            return False, "version_id must be a string"
    
    if 'do_commit' in data and data['do_commit'] is not None:
        if not isinstance(data['do_commit'], bool):
            return False, "do_commit must be a boolean"
    
    if 'transaction_id' in data and data['transaction_id'] is not None:
        if not isinstance(data['transaction_id'], str):
            return False, "transaction_id must be a string"
        
        if len(data['transaction_id']) > 32:
            return False, "transaction_id must be maximum 32 characters"
    
    return True, ""