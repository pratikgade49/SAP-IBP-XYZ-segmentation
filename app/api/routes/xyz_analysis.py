from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime
from typing import Optional
import pandas as pd
import io

from app.models.schemas import XYZAnalysisResponse, ErrorResponse
from app.services.sap_service import SAPService
from app.services.analysis_service import AnalysisService
from app.api.dependencies import get_sap_service, get_analysis_service
from app.config import get_settings
from app.utils.logger import get_logger

router = APIRouter(prefix="/api/v1/xyz-analysis", tags=["XYZ Analysis"])
logger = get_logger(__name__)


@router.get("", response_model=XYZAnalysisResponse)
async def get_xyz_analysis(
    x_threshold: float = Query(None, description="CV threshold for X segment"),
    y_threshold: float = Query(None, description="CV threshold for Y segment"),
    filters: Optional[str] = Query(None, description="Additional OData filters"),
    sap_service: SAPService = Depends(get_sap_service),
    analysis_service: AnalysisService = Depends(get_analysis_service)
):
    """
    Perform XYZ segmentation analysis on SAP IBP data
    
    - **X Segment**: Stable demand (CV <= x_threshold)
    - **Y Segment**: Moderate variability (x_threshold < CV <= y_threshold)
    - **Z Segment**: High variability (CV > y_threshold)
    """
    settings = get_settings()
    x_thresh = x_threshold or settings.DEFAULT_X_THRESHOLD
    y_thresh = y_threshold or settings.DEFAULT_Y_THRESHOLD
    
    logger.info(f"XYZ analysis requested with X={x_thresh}, Y={y_thresh}, filters={filters}")
    
    try:
        # Fetch data from SAP
        df = sap_service.fetch_data(additional_filters=filters)
        
        # Perform analysis
        result_df = analysis_service.calculate_xyz_segmentation(df, x_thresh, y_thresh)
        
        # Calculate segment distribution
        segment_counts = result_df['XYZ_Segment'].value_counts().to_dict()
        
        # Convert to response format
        data = result_df.to_dict('records')
        
        logger.info(f"Analysis complete: {len(result_df)} products analyzed")
        
        return XYZAnalysisResponse(
            total_products=len(result_df),
            segments=segment_counts,
            analysis_params={
                "x_threshold": x_thresh,
                "y_threshold": y_thresh
            },
            data=data,
            timestamp=datetime.utcnow().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export")
async def export_xyz_analysis(
    format: str = Query("csv", regex="^(csv|json|excel)$"),
    x_threshold: float = Query(None),
    y_threshold: float = Query(None),
    filters: Optional[str] = Query(None),
    sap_service: SAPService = Depends(get_sap_service),
    analysis_service: AnalysisService = Depends(get_analysis_service)
):
    """Export XYZ analysis results in CSV, JSON, or Excel format"""
    settings = get_settings()
    x_thresh = x_threshold or settings.DEFAULT_X_THRESHOLD
    y_thresh = y_threshold or settings.DEFAULT_Y_THRESHOLD
    
    logger.info(f"Export requested: format={format}")
    
    try:
        # Fetch and analyze data
        df = sap_service.fetch_data(additional_filters=filters)
        result_df = analysis_service.calculate_xyz_segmentation(df, x_thresh, y_thresh)
        
        # Generate file
        if format == "csv":
            output = io.StringIO()
            result_df.to_csv(output, index=False)
            output.seek(0)
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=xyz_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"}
            )
            
        elif format == "json":
            output = result_df.to_json(orient='records', indent=2)
            
            return StreamingResponse(
                iter([output]),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename=xyz_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"}
            )
            
        elif format == "excel":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                result_df.to_excel(writer, sheet_name='XYZ Analysis', index=False)
            output.seek(0)
            
            return StreamingResponse(
                output,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename=xyz_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"}
            )
            
    except Exception as e:
        logger.error(f"Export failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_segment_summary(
    x_threshold: float = Query(None),
    y_threshold: float = Query(None),
    filters: Optional[str] = Query(None),
    sap_service: SAPService = Depends(get_sap_service),
    analysis_service: AnalysisService = Depends(get_analysis_service)
):
    """Get detailed summary statistics for each XYZ segment"""
    settings = get_settings()
    x_thresh = x_threshold or settings.DEFAULT_X_THRESHOLD
    y_thresh = y_threshold or settings.DEFAULT_Y_THRESHOLD
    
    try:
        df = sap_service.fetch_data(additional_filters=filters)
        result_df = analysis_service.calculate_xyz_segmentation(df, x_thresh, y_thresh)
        summary = analysis_service.get_segment_summary(result_df)
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "analysis_params": {
                "x_threshold": x_thresh,
                "y_threshold": y_thresh
            },
            "segments": summary
        }
        
    except Exception as e:
        logger.error(f"Summary generation failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))