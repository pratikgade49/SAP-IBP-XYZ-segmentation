"""
API Routes for XYZ Segmentation Operations
"""
from flask import Blueprint, request, jsonify
from services.xyz_segmentation_service import (
    XYZSegmentationService,
    SegmentationThresholds,
    CalculationStrategy
)
from services.master_data_service import MasterDataService
from services.extraction_service import ExtractionService
from utils.logger import get_logger

logger = get_logger(__name__)

# XYZ Segmentation blueprint
xyz_segmentation_bp = Blueprint('xyz_segmentation', __name__)


# ==================== SEGMENTATION ENDPOINTS ====================

@xyz_segmentation_bp.route('/segment/cv', methods=['POST'])
def segment_by_cv():
    """
    Segment items using Coefficient of Variation calculation
    
    Expected JSON payload:
    {
        "demand_data": {
            "PROD001": [100, 120, 110, 105, 115, 125, 130],
            "PROD002": [50, 200, 30, 180, 40, 190, 35],
            "PROD003": [1000, 1010, 1005, 1015, 1020, 1008, 1012]
        },
        "thresholds": {
            "x_threshold": 0.5,
            "y_threshold": 1.0
        },
        "remove_zeros": true,
        "use_cv_squared": false
    }
    """
    try:
        payload = request.get_json()
        
        if 'demand_data' not in payload:
            return jsonify({'error': 'demand_data is required'}), 400
        
        # Parse thresholds if provided
        thresholds = None
        if 'thresholds' in payload:
            thresholds = SegmentationThresholds(
                x_threshold=payload['thresholds']['x_threshold'],
                y_threshold=payload['thresholds']['y_threshold']
            )
        
        # Perform segmentation
        service = XYZSegmentationService()
        results = service.segment_items_cv(
            demand_data=payload['demand_data'],
            thresholds=thresholds,
            remove_zeros=payload.get('remove_zeros', True),
            use_cv_squared=payload.get('use_cv_squared', False)
        )
        
        # Get summary
        summary = service.get_segment_summary(results)
        
        # Format results
        formatted_results = {
            item_id: {
                'segment': result.segment.value,
                'cv': result.coefficient_of_variation,
                'mean_demand': result.mean_demand,
                'std_deviation': result.std_deviation,
                'data_points': result.data_points
            }
            for item_id, result in results.items()
        }
        
        return jsonify({
            'status': 'success',
            'method': 'coefficient_of_variation',
            'summary': summary,
            'results': formatted_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in CV segmentation: {str(e)}")
        return jsonify({'error': str(e)}), 500


@xyz_segmentation_bp.route('/segment/aggregated', methods=['POST'])
def segment_by_aggregated_metric():
    """
    Segment items using pre-calculated metrics (e.g., MAPE)
    
    Expected JSON payload:
    {
        "metric_data": {
            "PROD001": [0.15, 0.18, 0.12, 0.16, 0.14],
            "PROD002": [0.45, 0.52, 0.48, 0.50, 0.47],
            "PROD003": [1.2, 1.5, 1.3, 1.4, 1.6]
        },
        "thresholds": {
            "x_threshold": 0.3,
            "y_threshold": 0.8
        },
        "aggregation_method": "average"
    }
    """
    try:
        payload = request.get_json()
        
        if 'metric_data' not in payload:
            return jsonify({'error': 'metric_data is required'}), 400
        
        # Parse thresholds if provided
        thresholds = None
        if 'thresholds' in payload:
            thresholds = SegmentationThresholds(
                x_threshold=payload['thresholds']['x_threshold'],
                y_threshold=payload['thresholds']['y_threshold']
            )
        
        # Perform segmentation
        service = XYZSegmentationService()
        results = service.segment_items_aggregated(
            metric_data=payload['metric_data'],
            thresholds=thresholds,
            aggregation_method=payload.get('aggregation_method', 'average')
        )
        
        # Get summary
        summary = service.get_segment_summary(results)
        
        # Format results
        formatted_results = {
            item_id: {
                'segment': result.segment.value,
                'aggregated_metric': result.coefficient_of_variation,
                'mean': result.mean_demand,
                'std_deviation': result.std_deviation,
                'data_points': result.data_points
            }
            for item_id, result in results.items()
        }
        
        return jsonify({
            'status': 'success',
            'method': 'aggregated_metric',
            'aggregation_method': payload.get('aggregation_method', 'average'),
            'summary': summary,
            'results': formatted_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in aggregated segmentation: {str(e)}")
        return jsonify({'error': str(e)}), 500


@xyz_segmentation_bp.route('/segment/kmeans', methods=['POST'])
def segment_by_kmeans():
    """
    Segment items using K-means clustering (ML method)
    
    Expected JSON payload:
    {
        "demand_data": {
            "PROD001": [100, 120, 110, 105, 115],
            "PROD002": [50, 200, 30, 180, 40],
            "PROD003": [1000, 1010, 1005, 1015, 1020]
        },
        "n_clusters": 3,
        "remove_zeros": true
    }
    """
    try:
        payload = request.get_json()
        
        if 'demand_data' not in payload:
            return jsonify({'error': 'demand_data is required'}), 400
        
        # Perform segmentation
        service = XYZSegmentationService()
        results = service.segment_items_kmeans(
            demand_data=payload['demand_data'],
            n_clusters=payload.get('n_clusters', 3),
            remove_zeros=payload.get('remove_zeros', True)
        )
        
        # Get summary
        summary = service.get_segment_summary(results)
        
        # Format results
        formatted_results = {
            item_id: {
                'segment': result.segment.value,
                'cv': result.coefficient_of_variation,
                'mean_demand': result.mean_demand,
                'std_deviation': result.std_deviation,
                'data_points': result.data_points
            }
            for item_id, result in results.items()
        }
        
        return jsonify({
            'status': 'success',
            'method': 'kmeans_clustering',
            'n_clusters': payload.get('n_clusters', 3),
            'summary': summary,
            'results': formatted_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in K-means segmentation: {str(e)}")
        return jsonify({'error': str(e)}), 500


@xyz_segmentation_bp.route('/segment/deseasonalized', methods=['POST'])
def segment_deseasonalized():
    """
    Segment items after removing seasonality
    
    Expected JSON payload:
    {
        "demand_data": {
            "PROD001": [100, 120, 110, 105, 115, 125, 130, 115, 120, 125, 135, 140],
            "PROD002": [50, 200, 30, 180, 40, 190, 35, 195, 45, 185, 38, 200]
        },
        "seasonal_period": 12,
        "thresholds": {
            "x_threshold": 0.5,
            "y_threshold": 1.0
        }
    }
    """
    try:
        payload = request.get_json()
        
        if 'demand_data' not in payload:
            return jsonify({'error': 'demand_data is required'}), 400
        
        # Parse thresholds if provided
        thresholds = None
        if 'thresholds' in payload:
            thresholds = SegmentationThresholds(
                x_threshold=payload['thresholds']['x_threshold'],
                y_threshold=payload['thresholds']['y_threshold']
            )
        
        # Perform segmentation
        service = XYZSegmentationService()
        results = service.calculate_deseasonalized_cv(
            demand_data=payload['demand_data'],
            seasonal_period=payload.get('seasonal_period', 12),
            thresholds=thresholds
        )
        
        # Get summary
        summary = service.get_segment_summary(results)
        
        # Format results
        formatted_results = {
            item_id: {
                'segment': result.segment.value,
                'cv': result.coefficient_of_variation,
                'mean_demand': result.mean_demand,
                'std_deviation': result.std_deviation,
                'data_points': result.data_points
            }
            for item_id, result in results.items()
        }
        
        return jsonify({
            'status': 'success',
            'method': 'deseasonalized_cv',
            'seasonal_period': payload.get('seasonal_period', 12),
            'summary': summary,
            'results': formatted_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in deseasonalized segmentation: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== INTEGRATED WORKFLOW ENDPOINTS ====================

@xyz_segmentation_bp.route('/workflow/extract-and-segment', methods=['POST'])
def extract_and_segment():
    """
    Complete workflow: Extract demand data from SAP IBP and perform segmentation
    
    Expected JSON payload:
    {
        "planning_area": "SAPIBP1",
        "product_filter": "PRDID in ('PRD001', 'PRD002', 'PRD003')",
        "location_filter": "LOCID eq '1720'",
        "date_from": "2023-01-01T00:00:00",
        "date_to": "2023-12-31T23:59:59",
        "key_figure": "DEMANDQTY",
        "segmentation_method": "cv",
        "thresholds": {
            "x_threshold": 0.5,
            "y_threshold": 1.0
        }
    }
    """
    try:
        payload = request.get_json()
        
        # Validate required fields
        required_fields = ['planning_area', 'date_from', 'date_to', 'key_figure']
        for field in required_fields:
            if field not in payload:
                return jsonify({'error': f'{field} is required'}), 400
        
        logger.info("Starting extract and segment workflow")
        
        # Step 1: Extract demand data from SAP IBP
        extraction_service = ExtractionService()
        
        # Build filter
        filters = []
        if 'product_filter' in payload:
            filters.append(payload['product_filter'])
        if 'location_filter' in payload:
            filters.append(payload['location_filter'])
        
        # Add date filter
        date_filter = (f"PERIODID3_TSTAMP ge datetime'{payload['date_from']}' and "
                      f"PERIODID3_TSTAMP le datetime'{payload['date_to']}'")
        filters.append(date_filter)
        
        combined_filter = ' and '.join(filters)
        
        # Extract data
        logger.info(f"Extracting demand data with filter: {combined_filter}")
        extraction_result = extraction_service.extract_keyfigures(
            planning_area=payload['planning_area'],
            select=f"PRDID,{payload['key_figure']},PERIODID3_TSTAMP",
            filter_str=combined_filter,
            format_type='json'
        )
        
        # Step 2: Transform extracted data into demand_data format
        records = extraction_result.get('d', {}).get('results', [])
        logger.info(f"Extracted {len(records)} records")
        
        demand_data = {}
        for record in records:
            product_id = record.get('PRDID')
            demand_value = float(record.get(payload['key_figure'], 0))
            
            if product_id not in demand_data:
                demand_data[product_id] = []
            
            demand_data[product_id].append(demand_value)
        
        logger.info(f"Organized data for {len(demand_data)} products")
        
        # Step 3: Perform segmentation
        segmentation_service = XYZSegmentationService()
        
        thresholds = None
        if 'thresholds' in payload:
            thresholds = SegmentationThresholds(
                x_threshold=payload['thresholds']['x_threshold'],
                y_threshold=payload['thresholds']['y_threshold']
            )
        
        method = payload.get('segmentation_method', 'cv')
        
        if method == 'cv':
            results = segmentation_service.segment_items_cv(
                demand_data=demand_data,
                thresholds=thresholds
            )
        elif method == 'kmeans':
            results = segmentation_service.segment_items_kmeans(
                demand_data=demand_data,
                n_clusters=3
            )
        elif method == 'deseasonalized':
            results = segmentation_service.calculate_deseasonalized_cv(
                demand_data=demand_data,
                seasonal_period=payload.get('seasonal_period', 12),
                thresholds=thresholds
            )
        else:
            return jsonify({'error': f'Invalid segmentation method: {method}'}), 400
        
        # Get summary
        summary = segmentation_service.get_segment_summary(results)
        
        # Format results
        formatted_results = {
            item_id: {
                'segment': result.segment.value,
                'cv': result.coefficient_of_variation,
                'mean_demand': result.mean_demand,
                'std_deviation': result.std_deviation,
                'data_points': result.data_points
            }
            for item_id, result in results.items()
        }
        
        return jsonify({
            'status': 'success',
            'workflow': 'extract_and_segment',
            'records_extracted': len(records),
            'products_analyzed': len(demand_data),
            'segmentation_method': method,
            'summary': summary,
            'results': formatted_results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in extract and segment workflow: {str(e)}")
        return jsonify({'error': str(e)}), 500


@xyz_segmentation_bp.route('/workflow/segment-and-upload', methods=['POST'])
def segment_and_upload():
    """
    Complete workflow: Segment provided data and upload to SAP IBP
    
    Expected JSON payload:
    {
        "demand_data": {
            "PROD001": [100, 120, 110, 105, 115],
            "PROD002": [50, 200, 30, 180, 40]
        },
        "segmentation_method": "cv",
        "thresholds": {
            "x_threshold": 0.5,
            "y_threshold": 1.0
        },
        "upload_config": {
            "master_data_type": "PRODUCT",
            "planning_area_id": "SAPIBP1",
            "version_id": "BASELINE"
        }
    }
    """
    try:
        payload = request.get_json()
        
        if 'demand_data' not in payload:
            return jsonify({'error': 'demand_data is required'}), 400
        
        if 'upload_config' not in payload:
            return jsonify({'error': 'upload_config is required'}), 400
        
        logger.info("Starting segment and upload workflow")
        
        # Step 1: Perform segmentation
        segmentation_service = XYZSegmentationService()
        
        thresholds = None
        if 'thresholds' in payload:
            thresholds = SegmentationThresholds(
                x_threshold=payload['thresholds']['x_threshold'],
                y_threshold=payload['thresholds']['y_threshold']
            )
        
        method = payload.get('segmentation_method', 'cv')
        
        if method == 'cv':
            results = segmentation_service.segment_items_cv(
                demand_data=payload['demand_data'],
                thresholds=thresholds
            )
        elif method == 'kmeans':
            results = segmentation_service.segment_items_kmeans(
                demand_data=payload['demand_data']
            )
        else:
            return jsonify({'error': f'Invalid segmentation method: {method}'}), 400
        
        # Get summary
        summary = segmentation_service.get_segment_summary(results)
        
        # Step 2: Export to SAP IBP format
        upload_config = payload['upload_config']
        export_data = segmentation_service.export_to_sap_ibp_format(
            results=results,
            planning_area_id=upload_config.get('planning_area_id'),
            version_id=upload_config.get('version_id')
        )
        
        # Step 3: Upload to SAP IBP
        master_data_service = MasterDataService()
        
        import_result = master_data_service.import_master_data(
            master_data_type=upload_config['master_data_type'],
            requested_attributes="PRDID,XYZ_SEGMENT,CV_VALUE,MEAN_DEMAND,STD_DEV",
            data=export_data,
            planning_area_id=upload_config.get('planning_area_id'),
            version_id=upload_config.get('version_id'),
            do_commit=True
        )
        
        return jsonify({
            'status': 'success',
            'workflow': 'segment_and_upload',
            'segmentation_summary': summary,
            'records_uploaded': len(export_data),
            'import_result': import_result
        }), 200
        
    except Exception as e:
        logger.error(f"Error in segment and upload workflow: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== UTILITY ENDPOINTS ====================

@xyz_segmentation_bp.route('/calculate/cv', methods=['POST'])
def calculate_cv():
    """
    Calculate Coefficient of Variation for a single item
    
    Expected JSON payload:
    {
        "demand_values": [100, 120, 110, 105, 115, 125, 130],
        "remove_zeros": true
    }
    """
    try:
        payload = request.get_json()
        
        if 'demand_values' not in payload:
            return jsonify({'error': 'demand_values is required'}), 400
        
        import numpy as np
        
        values = payload['demand_values']
        
        if payload.get('remove_zeros', True):
            values = [v for v in values if v != 0]
        
        if len(values) < 2:
            return jsonify({'error': 'Insufficient data points'}), 400
        
        mean = np.mean(values)
        std_dev = np.std(values, ddof=1)
        
        if mean == 0:
            return jsonify({'error': 'Mean is zero'}), 400
        
        cv = std_dev / mean
        
        return jsonify({
            'status': 'success',
            'coefficient_of_variation': round(cv, 4),
            'mean': round(mean, 2),
            'std_deviation': round(std_dev, 2),
            'data_points': len(values)
        }), 200
        
    except Exception as e:
        logger.error(f"Error calculating CV: {str(e)}")
        return jsonify({'error': str(e)}), 500


@xyz_segmentation_bp.route('/thresholds/recommend', methods=['POST'])
def recommend_thresholds():
    """
    Recommend thresholds based on data distribution using percentiles
    
    Expected JSON payload:
    {
        "demand_data": {
            "PROD001": [100, 120, 110],
            "PROD002": [50, 200, 30]
        },
        "x_percentile": 33,
        "y_percentile": 67
    }
    """
    try:
        payload = request.get_json()
        
        if 'demand_data' not in payload:
            return jsonify({'error': 'demand_data is required'}), 400
        
        import numpy as np
        
        # Calculate CV for all items
        cv_values = []
        for item_id, values in payload['demand_data'].items():
            filtered = [v for v in values if v != 0]
            if len(filtered) >= 2:
                mean = np.mean(filtered)
                if mean > 0:
                    std_dev = np.std(filtered, ddof=1)
                    cv = std_dev / mean
                    cv_values.append(cv)
        
        if len(cv_values) < 3:
            return jsonify({'error': 'Insufficient items for threshold recommendation'}), 400
        
        x_percentile = payload.get('x_percentile', 33)
        y_percentile = payload.get('y_percentile', 67)
        
        x_threshold = np.percentile(cv_values, x_percentile)
        y_threshold = np.percentile(cv_values, y_percentile)
        
        return jsonify({
            'status': 'success',
            'recommended_thresholds': {
                'x_threshold': round(x_threshold, 4),
                'y_threshold': round(y_threshold, 4)
            },
            'cv_statistics': {
                'min': round(np.min(cv_values), 4),
                'max': round(np.max(cv_values), 4),
                'mean': round(np.mean(cv_values), 4),
                'median': round(np.median(cv_values), 4)
            },
            'items_analyzed': len(cv_values)
        }), 200
        
    except Exception as e:
        logger.error(f"Error recommending thresholds: {str(e)}")
        return jsonify({'error': str(e)}), 500