from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import datetime


class ProductData(BaseModel):
    """Raw product data from SAP"""
    PRDID: str
    KF_DATE: str
    ACTUALSQTY: float


class ProductStats(BaseModel):
    """Product statistics with XYZ segment"""
    PRDID: str
    mean: float = Field(..., description="Mean of actual sales quantity")
    std: float = Field(..., description="Standard deviation")
    CV: float = Field(..., description="Coefficient of Variation (%)")
    XYZ_Segment: str = Field(..., description="X, Y, or Z segment")


class XYZAnalysisResponse(BaseModel):
    """Response for XYZ analysis"""
    total_products: int
    segments: Dict[str, int]
    analysis_params: Dict[str, float]
    data: List[ProductStats]
    timestamp: str


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    version: str


class ErrorResponse(BaseModel):
    """Error response"""
    error: str
    detail: Optional[str] = None
    timestamp: str