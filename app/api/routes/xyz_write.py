"""
app/api/routes/xyz_write.py - FIXED VERSION

Key fixes:
1. Extract primary_key from groupby_attributes
2. Pass primary_key to all service methods
3. Handle dynamic segmentation properly
4. Preserve all grouping dimensions (PRDID, LOCID, etc.)
"""

from fastapi import APIRouter, Depends, Query, HTTPException, Body
from datetime import datetime
from typing import Optional
from enum import Enum

from app.models.write_schemas import (
    XYZWriteRequest,
    XYZWriteResponse,
    XYZWriteStatus,
    BatchWriteResponse
)
from app.services.sap_service import SAPService
from app.services.sap_write_service import SAPWriteService
from app.services.dynamic_analysis_service import DynamicAnalysisService
from app.models.segmentation_schemas import SegmentationConfig
from app.api.dependencies import get_sap_service, get_sap_write_service
from app.config import get_settings
from app.utils.logger import get_logger

router = APIRouter(prefix="/api/v1/xyz-write", tags=["XYZ Write-Back"])
logger = get_logger(__name__)


class WriteMode(str, Enum):
    """Write mode options"""
    SIMPLE = "simple"
    BATCHED = "batched"
    PARALLEL = "parallel"


@router.post("/write-segments", response_model=XYZWriteResponse)
async def write_xyz_segments(
    request: XYZWriteRequest = Body(...),
    sap_service: SAPService = Depends(get_sap_service),
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    Perform XYZ analysis and write segments back to SAP IBP
    
    **IMPORTANT**: When using dynamic segmentation with groupby_attributes,
    make sure to include all dimensions in your request:
    
    Example for Product-Location segmentation:
    ```json
    {
        "groupby_attributes": ["PRDID", "LOCID"],
        "x_threshold": 10.0,
        "y_threshold": 25.0,
        "write_mode": "batched",
        "version_id": "CONSENSUS"
    }
    ```
    """
    settings = get_settings()
    x_thresh = request.x_threshold or settings.DEFAULT_X_THRESHOLD
    y_thresh = request.y_threshold or settings.DEFAULT_Y_THRESHOLD
    
    # Determine if this is dynamic segmentation or simple
    if request.groupby_attributes:
        # Dynamic segmentation mode
        primary_key = request.primary_key or request.groupby_attributes[0]
        groupby_attrs = request.groupby_attributes
        
        logger.info(
            f"XYZ write-back (DYNAMIC) requested: mode={request.write_mode}, "
            f"primary_key={primary_key}, groupby={groupby_attrs}, "
            f"version={request.version_id}, X={x_thresh}, Y={y_thresh}"
        )
    else:
        # Simple product-only segmentation (backward compatibility)
        primary_key = "PRDID"
        groupby_attrs = ["PRDID"]
        
        logger.info(
            f"XYZ write-back (SIMPLE) requested: mode={request.write_mode}, "
            f"version={request.version_id}, X={x_thresh}, Y={y_thresh}"
        )
    
    try:
        # Step 1: Fetch data from SAP
        logger.info(f"Step 1: Fetching data from SAP IBP with primary_key={primary_key}")
        
        # Determine additional attributes to fetch
        additional_attrs = [attr for attr in groupby_attrs if attr != primary_key]
        
        df = sap_service.fetch_data(
            primary_key=primary_key,
            additional_filters=request.filters,
            additional_attributes=additional_attrs
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found with given filters")
        
        logger.info(f"Fetched {len(df)} records with columns: {list(df.columns)}")
        
        # Step 2: Perform XYZ analysis
        logger.info(f"Step 2: Performing XYZ segmentation with groupby={groupby_attrs}")
        
        # Build segmentation config
        config = SegmentationConfig(
            primary_key=primary_key,
            groupby_attributes=groupby_attrs,
            x_threshold=x_thresh,
            y_threshold=y_thresh,
            min_periods=6,  # Can be made configurable
            filters=request.filters
        )
        
        # Use dynamic analysis service
        analysis_service = DynamicAnalysisService()
        result_df, data_quality = analysis_service.calculate_dynamic_xyz_segmentation(df, config)
        
        if result_df.empty:
            raise HTTPException(
                status_code=422,
                detail="No segments produced. Try adjusting thresholds or filters."
            )
        
        logger.info(f"Analysis complete: {len(result_df)} segments produced")
        
        # Step 3: Prepare data for write-back
        # Keep all grouping dimensions plus XYZ_Segment
        write_columns = groupby_attrs + ['XYZ_Segment']
        write_df = result_df[write_columns].copy()
        
        # Add period field if available and not already in groupby
        if request.period_field and request.period_field in df.columns:
            if request.period_field not in write_df.columns:
                # Get the first period for each unique combination
                period_data = df.groupby(groupby_attrs)[request.period_field].first().reset_index()
                write_df = write_df.merge(period_data, on=groupby_attrs, how='left')
        
        logger.info(f"Prepared {len(write_df)} segments for write-back")
        logger.info(f"Write columns: {list(write_df.columns)}")
        
        # Step 4: Write to SAP based on mode
        logger.info(f"Step 3: Writing to SAP IBP using {request.write_mode} mode")
        
        if request.write_mode == WriteMode.SIMPLE:
            write_result = write_service.write_segments_simple(
                segment_data=write_df,
                primary_key=primary_key,
                version_id=request.version_id,
                scenario_id=request.scenario_id,
                period_field=request.period_field or "PERIODID3_TSTAMP"
            )
        
        elif request.write_mode == WriteMode.BATCHED:
            write_result = write_service.write_segments_batched(
                segment_data=write_df,
                primary_key=primary_key,
                version_id=request.version_id,
                scenario_id=request.scenario_id,
                period_field=request.period_field or "PERIODID3_TSTAMP",
                batch_size=request.batch_size or 5000
            )
        
        elif request.write_mode == WriteMode.PARALLEL:
            write_result = write_service.write_segments_parallel(
                segment_data=write_df,
                primary_key=primary_key,
                version_id=request.version_id,
                scenario_id=request.scenario_id,
                period_field=request.period_field or "PERIODID3_TSTAMP",
                batch_size=request.batch_size or 5000,
                max_workers=request.max_workers or 4
            )
        
        # Calculate segment distribution
        segment_counts = result_df['XYZ_Segment'].value_counts().to_dict()
        
        logger.info(f"Write operation completed successfully: {write_result.get('transaction_id')}")
        
        return XYZWriteResponse(
            status="success",
            transaction_id=write_result.get('transaction_id'),
            total_products=len(result_df),
            segments_written=segment_counts,
            analysis_params={
                "primary_key": primary_key,
                "groupby_attributes": groupby_attrs,
                "x_threshold": x_thresh,
                "y_threshold": y_thresh
            },
            write_mode=request.write_mode,
            version_id=request.version_id,
            scenario_id=request.scenario_id,
            records_sent=write_result.get('records_sent'),
            batch_count=write_result.get('batch_count'),
            message=write_result.get('message'),
            timestamp=datetime.utcnow().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Write-back failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/write-custom", response_model=XYZWriteResponse)
async def write_custom_segments(
    segments: list = Body(..., description="List of segment assignments"),
    primary_key: str = Body("PRDID", description="Primary key for segmentation"),
    version_id: Optional[str] = Body(None),
    scenario_id: Optional[str] = Body(None),
    period_field: str = Body("PERIODID3_TSTAMP"),
    write_mode: WriteMode = Body(WriteMode.SIMPLE),
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    Write custom XYZ segment assignments to SAP IBP
    
    **Request body example for Product-Location:**
    ```json
    {
        "segments": [
            {"PRDID": "IBP-100", "LOCID": "1720", "XYZ_Segment": "X"},
            {"PRDID": "IBP-110", "LOCID": "1720", "XYZ_Segment": "Y"}
        ],
        "primary_key": "PRDID",
        "version_id": "CONSENSUS",
        "write_mode": "simple"
    }
    ```
    """
    logger.info(f"Custom segment write requested: {len(segments)} segments, primary_key={primary_key}")
    
    try:
        import pandas as pd
        
        # Convert to DataFrame
        write_df = pd.DataFrame(segments)
        
        # Validate required columns
        if primary_key not in write_df.columns or 'XYZ_Segment' not in write_df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Each segment must have '{primary_key}' and 'XYZ_Segment' fields"
            )
        
        # Validate segment values
        valid_segments = {'X', 'Y', 'Z'}
        invalid_segments = set(write_df['XYZ_Segment'].unique()) - valid_segments
        if invalid_segments:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid segment values: {invalid_segments}. Must be X, Y, or Z"
            )
        
        logger.info(f"Writing {len(write_df)} custom segments with primary_key={primary_key}")
        
        # Write based on mode
        if write_mode == WriteMode.SIMPLE:
            write_result = write_service.write_segments_simple(
                segment_data=write_df,
                primary_key=primary_key,
                version_id=version_id,
                scenario_id=scenario_id,
                period_field=period_field
            )
        elif write_mode == WriteMode.BATCHED:
            write_result = write_service.write_segments_batched(
                segment_data=write_df,
                primary_key=primary_key,
                version_id=version_id,
                scenario_id=scenario_id,
                period_field=period_field
            )
        else:
            write_result = write_service.write_segments_parallel(
                segment_data=write_df,
                primary_key=primary_key,
                version_id=version_id,
                scenario_id=scenario_id,
                period_field=period_field
            )
        
        segment_counts = write_df['XYZ_Segment'].value_counts().to_dict()
        
        return XYZWriteResponse(
            status="success",
            transaction_id=write_result.get('transaction_id'),
            total_products=len(write_df),
            segments_written=segment_counts,
            analysis_params={"primary_key": primary_key},
            write_mode=write_mode,
            version_id=version_id,
            scenario_id=scenario_id,
            records_sent=write_result.get('records_sent'),
            batch_count=write_result.get('batch_count'),
            message=write_result.get('message'),
            timestamp=datetime.utcnow().isoformat()
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Custom write failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{transaction_id}", response_model=XYZWriteStatus)
async def get_write_status(
    transaction_id: str,
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """Get the status of a write transaction"""
    logger.info(f"Status check requested for transaction: {transaction_id}")
    
    try:
        session, csrf_token = write_service._get_csrf_token()
        
        try:
            export_result = write_service._get_export_result(session, csrf_token, transaction_id)
            
            messages = []
            try:
                msg_session, msg_csrf = write_service._get_csrf_token()
                try:
                    url = f"{write_service.api_url}/Message"
                    response = msg_session.get(
                        url,
                        params={"Transactionid": transaction_id},
                        headers={"X-CSRF-Token": msg_csrf},
                        timeout=write_service.timeout
                    )
                    if response.ok:
                        messages = response.json()
                except Exception as e:
                    logger.warning(f"Could not fetch messages: {str(e)}")
                finally:
                    msg_session.close()
            except Exception:
                pass
            
            return XYZWriteStatus(
                transaction_id=transaction_id,
                status="completed" if export_result else "unknown",
                export_result=export_result,
                messages=messages,
                timestamp=datetime.utcnow().isoformat()
            )
        
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Status check failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/validate-config")
async def validate_write_config(
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """Validate write configuration"""
    settings = get_settings()
    
    config_status = {
        "sap_write_api_url": bool(settings.SAP_WRITE_API_URL),
        "planning_area": bool(settings.SAP_PLANNING_AREA),
        "xyz_key_figure": bool(settings.SAP_XYZ_KEY_FIGURE),
        "credentials_configured": bool(settings.SAP_USERNAME and settings.SAP_PASSWORD)
    }
    
    all_configured = all(config_status.values())
    
    return {
        "configured": all_configured,
        "configuration": config_status,
        "message": "All settings configured" if all_configured else "Missing required settings",
        "timestamp": datetime.utcnow().isoformat()
    }

@router.post("/debug-payload")
async def debug_write_payload(
    request: XYZWriteRequest = Body(...),
    sap_service: SAPService = Depends(get_sap_service),
    write_service: SAPWriteService = Depends(get_sap_write_service)
):
    """
    DEBUG ENDPOINT: Generate and return the payload that would be sent to SAP
    without actually sending it. Use this to troubleshoot SAP write issues.
    """
    settings = get_settings()
    x_thresh = request.x_threshold or settings.DEFAULT_X_THRESHOLD
    y_thresh = request.y_threshold or settings.DEFAULT_Y_THRESHOLD
    
    # Determine configuration
    if request.groupby_attributes:
        primary_key = request.primary_key or request.groupby_attributes[0]
        groupby_attrs = request.groupby_attributes
    else:
        primary_key = "PRDID"
        groupby_attrs = ["PRDID"]
    
    try:
        # Fetch data
        additional_attrs = [attr for attr in groupby_attrs if attr != primary_key]
        df = sap_service.fetch_data(
            primary_key=primary_key,
            additional_filters=request.filters,
            additional_attributes=additional_attrs
        )
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No data found")
        
        # Perform analysis
        from app.services.dynamic_analysis_service import DynamicAnalysisService
        from app.models.segmentation_schemas import SegmentationConfig
        
        config = SegmentationConfig(
            primary_key=primary_key,
            groupby_attributes=groupby_attrs,
            x_threshold=x_thresh,
            y_threshold=y_thresh,
            min_periods=6,
            filters=request.filters
        )
        
        analysis_service = DynamicAnalysisService()
        result_df, data_quality = analysis_service.calculate_dynamic_xyz_segmentation(df, config)
        
        if result_df.empty:
            raise HTTPException(status_code=422, detail="No segments produced")
        
        # Prepare write data
        write_columns = groupby_attrs + ['XYZ_Segment']
        write_df = result_df[write_columns].copy()
        
        # Add period field
        if request.period_field and request.period_field in df.columns:
            if request.period_field not in write_df.columns:
                period_data = df.groupby(groupby_attrs)[request.period_field].first().reset_index()
                write_df = write_df.merge(period_data, on=groupby_attrs, how='left')
        
        # Generate transaction ID
        transaction_id = write_service._generate_transaction_id()
        
        # Generate payload WITHOUT sending
        payload = write_service._prepare_payload(
            segment_data=write_df,
            transaction_id=transaction_id,
            primary_key=primary_key,
            version_id=request.version_id,
            scenario_id=request.scenario_id,
            period_field=request.period_field or "PERIODID3_TSTAMP",
            do_commit=True
        )
        
        # Return payload for inspection
        nav_key = f"Nav{write_service.planning_area}"
        
        return {
            "status": "debug",
            "message": "This is what would be sent to SAP (not actually sent)",
            "url": f"{write_service.api_url}/{write_service.planning_area}Trans",
            "transaction_id": transaction_id,
            "payload_structure": {
                "Transactionid": payload.get("Transactionid"),
                "AggregationLevelFieldsString": payload.get("AggregationLevelFieldsString"),
                "VersionID": payload.get("VersionID"),
                "ScenarioID": payload.get("ScenarioID"),
                "DoCommit": payload.get("DoCommit"),
                "NavigationProperty": nav_key,
                "RecordCount": len(payload.get(nav_key, []))
            },
            "sample_records": payload.get(nav_key, [])[:3],  # First 3 records
            "full_payload_preview": {
                k: v if k != nav_key else f"[{len(v)} records]" 
                for k, v in payload.items()
            },
            "data_analysis": {
                "total_segments": len(result_df),
                "segment_distribution": result_df['XYZ_Segment'].value_counts().to_dict(),
                "primary_key": primary_key,
                "dimensions_included": list(write_df.columns)
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Debug payload generation failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))