"""
app/api/routes/dynamic_segmentation.py

Simplified API routes for dynamic XYZ segmentation
"""

from fastapi import APIRouter, Depends, Query, HTTPException, Body
from fastapi.responses import StreamingResponse
from datetime import datetime
from typing import Optional, List
import pandas as pd
import io

from app.models.segmentation_schemas import (
    SegmentationConfig,
    AvailableAttributesResponse,
    SegmentationPreviewResponse,
    DynamicXYZAnalysisResponse,
    AttributeInfo,
    RecommendedCombination
)
from app.services.sap_service import SAPService
from app.services.dynamic_analysis_service import DynamicAnalysisService
from app.api.dependencies import get_sap_service
from app.config import get_settings
from app.utils.logger import get_logger

router = APIRouter(prefix="/api/v1/dynamic-segmentation", tags=["Dynamic Segmentation"])
logger = get_logger(__name__)


@router.get("/available-attributes")
async def get_available_attributes_list():
    """
    Get list of all attributes that can be used for segmentation
    
    Simply returns the predefined list of SAP IBP attributes.
    Use these in your segmentation configuration.
    """
    attributes = SAPService.get_available_attributes()
    
    # Build detailed info
    attribute_info = {
        'PRDID': 'Product ID - Individual product identifier (REQUIRED)',
        'LOCID': 'Location ID - Warehouse/distribution center',
        'CUSTID': 'Customer ID - Customer identifier',
        'PRDGRPID': 'Product Group ID - Product category/family',
        'REGIONID': 'Region ID - Geographic region',
        'SALESORGID': 'Sales Organization ID - Sales organization unit',
        'CHANID': 'Channel ID - Sales channel',
        'DIVID': 'Division ID - Business division'
    }
    
    detailed = [
        {
            'attribute': attr,
            'description': attribute_info.get(attr, 'Additional attribute')
        }
        for attr in attributes
    ]
    
    return {
        "available_attributes": attributes,
        "detailed_info": detailed,
        "usage_example": {
            "product_only": ["PRDID"],
            "product_location": ["PRDID", "LOCID"],
            "product_location_customer": ["PRDID", "LOCID", "CUSTID"]
        },
        "note": "PRDID is required and always included",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/attributes", response_model=AvailableAttributesResponse)
async def get_attributes_with_data(
    attributes: str = Query("PRDID", description="Comma-separated attributes (e.g., 'PRDID,LOCID,CUSTID')"),
    filters: Optional[str] = Query(None, description="OData filters"),
    sap_service: SAPService = Depends(get_sap_service)
):
    """
    Fetch data with specified attributes and show what's available
    
    **Parameters:**
    - `attributes`: Comma-separated list (e.g., "PRDID,LOCID,CUSTID")
    - `filters`: Optional OData filters
    
    **Example:**
    `/attributes?attributes=PRDID,LOCID,CUSTID`
    """
    logger.info(f"Fetching data with attributes: {attributes}")
    
    try:
        # Parse attributes
        attr_list = [a.strip() for a in attributes.split(',')]
        
        # PRDID is always required
        if 'PRDID' not in attr_list:
            attr_list.insert(0, 'PRDID')
        
        # Fetch data
        additional_attrs = [a for a in attr_list if a != 'PRDID']
        df = sap_service.fetch_data(
            additional_filters=filters,
            additional_attributes=additional_attrs
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Get available attributes from the data
        analysis_service = DynamicAnalysisService()
        attributes_info = analysis_service.get_available_attributes(df)
        
        logger.info(f"Found {len(attributes_info['available_attributes'])} attributes in data")
        
        return AvailableAttributesResponse(
            available_attributes=attributes_info['available_attributes'],
            current_data_attributes=attributes_info['current_data_attributes'],
            recommended_combinations=attributes_info['recommended_combinations'],
            timestamp=datetime.utcnow().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Failed to get attributes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/preview", response_model=SegmentationPreviewResponse)
async def preview_segmentation(
    config: SegmentationConfig = Body(...),
    sap_service: SAPService = Depends(get_sap_service)
):
    """
    Preview segmentation configuration before running full analysis
    
    **Example Request:**
    ```json
    {
        "groupby_attributes": ["PRDID", "LOCID"],
        "x_threshold": 10.0,
        "y_threshold": 25.0,
        "min_periods": 12
    }
    ```
    """
    logger.info(f"Previewing segmentation: {config.groupby_attributes}")
    
    try:
        # Determine additional attributes to fetch
        additional_attrs = [a for a in config.groupby_attributes if a != 'PRDID']
        
        # Fetch data
        df = sap_service.fetch_data(
            additional_filters=config.filters,
            additional_attributes=additional_attrs
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found with given filters")
        
        # Preview configuration
        analysis_service = DynamicAnalysisService()
        preview_result = analysis_service.preview_segmentation(df, config)
        
        if 'error' in preview_result:
            raise HTTPException(status_code=400, detail=preview_result['error'])
        
        logger.info(f"Preview complete: {preview_result['estimated_segments']} estimated segments")
        
        return SegmentationPreviewResponse(
            config=config,
            estimated_segments=preview_result['estimated_segments'],
            data_coverage=preview_result['data_coverage'],
            warnings=preview_result['warnings'],
            timestamp=datetime.utcnow().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Preview failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze", response_model=DynamicXYZAnalysisResponse)
async def analyze_dynamic_segmentation(
    config: SegmentationConfig = Body(...),
    sap_service: SAPService = Depends(get_sap_service)
):
    """
    Perform dynamic XYZ segmentation analysis
    
    **Example Request:**
    ```json
    {
        "groupby_attributes": ["PRDID", "LOCID"],
        "x_threshold": 10.0,
        "y_threshold": 25.0,
        "min_periods": 12,
        "remove_outliers": true
    }
    ```
    """
    logger.info(f"Starting dynamic analysis: {config.groupby_attributes}")
    
    try:
        # Determine additional attributes to fetch
        additional_attrs = [a for a in config.groupby_attributes if a != 'PRDID']
        
        # Fetch data from SAP
        df = sap_service.fetch_data(
            additional_filters=config.filters,
            additional_attributes=additional_attrs
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found with given filters")
        
        logger.info(f"Fetched {len(df)} records for analysis")
        
        # Perform dynamic segmentation
        analysis_service = DynamicAnalysisService()
        result_df, data_quality = analysis_service.calculate_dynamic_xyz_segmentation(df, config)
        
        if result_df.empty:
            raise HTTPException(
                status_code=422,
                detail="No segments produced. Try reducing min_periods or adjusting filters."
            )
        
        # Get segment distribution
        segment_distribution = result_df['XYZ_Segment'].value_counts().to_dict()
        
        # Convert to response format
        data = result_df.to_dict('records')
        
        logger.info(
            f"Analysis complete: {len(result_df)} unique segments, "
            f"distribution: {segment_distribution}"
        )
        
        return DynamicXYZAnalysisResponse(
            total_records=data_quality['total_records_analyzed'],
            unique_segments=data_quality['unique_segments'],
            segmentation_level=config.groupby_attributes,
            segment_distribution=segment_distribution,
            analysis_params={
                "x_threshold": config.x_threshold,
                "y_threshold": config.y_threshold,
                "min_periods": config.min_periods,
                "groupby_attributes": config.groupby_attributes,
                "aggregation_method": config.aggregation_method.value,
                "outliers_removed": config.remove_outliers
            },
            data=data,
            data_quality=data_quality,
            timestamp=datetime.utcnow().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Dynamic analysis failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/export")
async def export_dynamic_analysis(
    config: SegmentationConfig = Body(...),
    format: str = Query("csv", regex="^(csv|json|excel)$"),
    sap_service: SAPService = Depends(get_sap_service)
):
    """Export dynamic XYZ analysis results"""
    logger.info(f"Export requested: format={format}, level={config.groupby_attributes}")
    
    try:
        # Determine additional attributes to fetch
        additional_attrs = [a for a in config.groupby_attributes if a != 'PRDID']
        
        # Fetch and analyze data
        df = sap_service.fetch_data(
            additional_filters=config.filters,
            additional_attributes=additional_attrs
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        analysis_service = DynamicAnalysisService()
        result_df, data_quality = analysis_service.calculate_dynamic_xyz_segmentation(df, config)
        
        if result_df.empty:
            raise HTTPException(status_code=422, detail="No segments produced")
        
        # Add metadata
        result_df['segmentation_level'] = '_'.join(config.groupby_attributes)
        result_df['analysis_date'] = datetime.utcnow().isoformat()
        
        # Generate file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        level_str = '_'.join(config.groupby_attributes).lower()
        
        if format == "csv":
            output = io.StringIO()
            result_df.to_csv(output, index=False)
            output.seek(0)
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=xyz_analysis_{level_str}_{timestamp}.csv"
                }
            )
            
        elif format == "json":
            output = result_df.to_json(orient='records', indent=2)
            
            return StreamingResponse(
                iter([output]),
                media_type="application/json",
                headers={
                    "Content-Disposition": f"attachment; filename=xyz_analysis_{level_str}_{timestamp}.json"
                }
            )
            
        elif format == "excel":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name='XYZ Analysis', index=False)
                
                # Summary sheet
                summary_data = []
                for segment in ['X', 'Y', 'Z']:
                    seg_data = result_df[result_df['XYZ_Segment'] == segment]
                    if not seg_data.empty:
                        summary_data.append({
                            'Segment': segment,
                            'Count': len(seg_data),
                            'Avg_CV': seg_data['CV'].mean(),
                            'Avg_Mean': seg_data['mean'].mean()
                        })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            output.seek(0)
            
            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename=xyz_analysis_{level_str}_{timestamp}.xlsx"
                }
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Export failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))