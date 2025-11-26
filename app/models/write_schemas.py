from pydantic import BaseModel, Field
from typing import Optional, Dict, List, Any


class XYZWriteRequest(BaseModel):
    """Request model for writing XYZ segments back to SAP"""
    
    # Segmentation configuration
    primary_key: Optional[str] = Field(None, description="Primary key (auto-detected from groupby_attributes if not provided)")
    groupby_attributes: Optional[List[str]] = Field(None, description="Attributes for segmentation (e.g., ['PRDID', 'LOCID'])")
    
    # Analysis parameters
    x_threshold: Optional[float] = Field(None, description="CV threshold for X segment")
    y_threshold: Optional[float] = Field(None, description="CV threshold for Y segment")
    filters: Optional[str] = Field(None, description="Additional OData filters for data fetch")
    
    # Write parameters
    write_mode: str = Field("simple", description="Write mode: simple, batched, or parallel")
    version_id: Optional[str] = Field(None, description="Target version ID (None = base version)")
    scenario_id: Optional[str] = Field(None, description="Target scenario ID (None = baseline)")
    location_id: Optional[str] = Field(None, description="Location ID if location-specific")
    period_field: Optional[str] = Field("PERIODID3_TSTAMP", description="Period field name")
    
    # Batch parameters (for batched/parallel modes)
    batch_size: Optional[int] = Field(5000, description="Records per batch", ge=1, le=10000)
    max_workers: Optional[int] = Field(4, description="Parallel workers (parallel mode only)", ge=1, le=10)
    
    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "name": "Product-Location Segmentation",
                    "value": {
                        "groupby_attributes": ["PRDID", "LOCID"],
                        "x_threshold": 10.0,
                        "y_threshold": 25.0,
                        "write_mode": "batched",
                        "version_id": "CONSENSUS"
                    }
                },
                {
                    "name": "Location-Only Segmentation",
                    "value": {
                        "primary_key": "LOCID",
                        "groupby_attributes": ["LOCID"],
                        "x_threshold": 10.0,
                        "y_threshold": 25.0,
                        "write_mode": "simple",
                        "filters": "LOCID eq '1720'"
                    }
                }
            ]
        }


class XYZWriteResponse(BaseModel):
    """Response model for write operations"""
    status: str = Field(..., description="Operation status")
    transaction_id: str = Field(..., description="SAP transaction ID")
    total_products: int = Field(..., description="Total products analyzed")
    segments_written: Dict[str, int] = Field(..., description="Segment distribution")
    analysis_params: Dict[str, Any] = Field(..., description="Analysis parameters used")
    write_mode: str = Field(..., description="Write mode used")
    version_id: Optional[str] = Field(None, description="Target version")
    scenario_id: Optional[str] = Field(None, description="Target scenario")
    records_sent: int = Field(..., description="Number of records sent to SAP")
    batch_count: Optional[int] = Field(None, description="Number of batches (if batched)")
    message: str = Field(..., description="Status message")
    timestamp: str = Field(..., description="Response timestamp")


class XYZWriteStatus(BaseModel):
    """Status of a write transaction"""
    transaction_id: str = Field(..., description="Transaction ID")
    status: str = Field(..., description="Transaction status")
    export_result: Dict[str, Any] = Field(..., description="Export result from SAP")
    messages: List[Dict[str, Any]] = Field(..., description="Error messages if any")
    timestamp: str = Field(..., description="Status check timestamp")


class BatchWriteResponse(BaseModel):
    """Response for batched write operations"""
    status: str
    transaction_id: str
    total_batches: int
    successful_batches: int
    failed_batches: List[int]
    records_sent: int
    message: str
    timestamp: str