"""
app/models/segmentation_schemas.py

Pydantic schemas for dynamic XYZ segmentation configuration
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from enum import Enum


class SegmentationAttribute(str, Enum):
    """Available attributes for segmentation"""
    PRODUCT = "PRDID"
    LOCATION = "LOCID"
    CUSTOMER = "CUSTID"
    PRODUCT_GROUP = "PRDGRPID"
    REGION = "REGIONID"
    SALES_ORG = "SALESORGID"
    # Add more as needed based on your SAP IBP data model


class AggregationMethod(str, Enum):
    """Methods for handling multiple periods"""
    MEAN_STD = "mean_std"  # Standard CV calculation
    WEIGHTED_MEAN = "weighted_mean"  # Weight recent periods more
    ROLLING_WINDOW = "rolling_window"  # Use moving window


class AttributeInfo(BaseModel):
    """Information about a single attribute"""
    field: str
    name: str
    description: str
    required: bool
    unique_values: int


class RecommendedCombination(BaseModel):
    """Recommended attribute combination"""
    level: str
    attributes: List[str]
    description: str
    estimated_segments: int
    use_case: str


class AvailableAttributesResponse(BaseModel):
    """Response listing available attributes for segmentation"""
    available_attributes: List[AttributeInfo] = Field(
        ...,
        description="List of attributes with descriptions"
    )
    current_data_attributes: List[str] = Field(
        ...,
        description="Attributes currently present in the data"
    )
    recommended_combinations: List[RecommendedCombination] = Field(
        ...,
        description="Suggested attribute combinations"
    )
    timestamp: str


class SegmentationConfig(BaseModel):
    """Configuration for dynamic segmentation"""
    
    groupby_attributes: List[str] = Field(
        ...,
        description="List of attributes to group by (e.g., ['PRDID', 'LOCID'])",
        min_items=1
    )
    
    x_threshold: float = Field(
        10.0,
        description="CV threshold for X segment (stable)",
        ge=0,
        le=100
    )
    
    y_threshold: float = Field(
        25.0,
        description="CV threshold for Y segment (moderate)",
        ge=0,
        le=100
    )
    
    min_periods: int = Field(
        6,
        description="Minimum number of periods required for analysis",
        ge=3
    )
    
    aggregation_method: AggregationMethod = Field(
        AggregationMethod.MEAN_STD,
        description="Method for calculating variability"
    )
    
    remove_outliers: bool = Field(
        False,
        description="Remove statistical outliers before analysis"
    )
    
    outlier_threshold: float = Field(
        3.0,
        description="Standard deviations for outlier removal",
        ge=1.5,
        le=5.0
    )
    
    filters: Optional[str] = Field(
        None,
        description="Additional OData filters"
    )
    
    @validator('y_threshold')
    def validate_thresholds(cls, v, values):
        """Ensure Y threshold is greater than X threshold"""
        if 'x_threshold' in values and v <= values['x_threshold']:
            raise ValueError('y_threshold must be greater than x_threshold')
        return v
    
    @validator('groupby_attributes')
    def validate_attributes(cls, v):
        """Validate that at least PRDID is included"""
        if 'PRDID' not in v:
            raise ValueError('PRDID must be included in groupby_attributes')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "groupby_attributes": ["PRDID", "LOCID"],
                "x_threshold": 10.0,
                "y_threshold": 25.0,
                "min_periods": 12,
                "aggregation_method": "mean_std",
                "remove_outliers": True,
                "outlier_threshold": 3.0
            }
        }


class AttributeInfo(BaseModel):
    """Information about a single attribute"""
    field: str
    name: str
    description: str
    required: bool
    unique_values: int


class RecommendedCombination(BaseModel):
    """Recommended attribute combination"""
    level: str
    attributes: List[str]
    description: str
    estimated_segments: int
    use_case: str


class AvailableAttributesResponse(BaseModel):
    """Response listing available attributes for segmentation"""
    available_attributes: List[AttributeInfo] = Field(
        ...,
        description="List of attributes with descriptions"
    )
    current_data_attributes: List[str] = Field(
        ...,
        description="Attributes currently present in the data"
    )
    recommended_combinations: List[RecommendedCombination] = Field(
        ...,
        description="Suggested attribute combinations"
    )
    timestamp: str


class SegmentationPreviewResponse(BaseModel):
    """Preview of segmentation configuration"""
    config: SegmentationConfig
    estimated_segments: int = Field(
        ...,
        description="Estimated number of unique segments"
    )
    data_coverage: Dict[str, Any] = Field(
        ...,
        description="Data quality metrics"
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Configuration warnings"
    )
    timestamp: str


class DynamicXYZAnalysisResponse(BaseModel):
    """Response for dynamic XYZ analysis"""
    total_records: int
    unique_segments: int
    segmentation_level: List[str]
    segment_distribution: Dict[str, int]
    analysis_params: Dict[str, Any]
    data: List[Dict[str, Any]]
    data_quality: Dict[str, Any]
    timestamp: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "total_records": 1500,
                "unique_segments": 450,
                "segmentation_level": ["PRDID", "LOCID"],
                "segment_distribution": {"X": 150, "Y": 180, "Z": 120},
                "analysis_params": {
                    "x_threshold": 10.0,
                    "y_threshold": 25.0,
                    "min_periods": 12
                },
                "data_quality": {
                    "records_with_sufficient_history": 450,
                    "records_excluded": 0,
                    "avg_periods_per_segment": 24
                },
                "timestamp": "2024-01-15T10:30:00"
            }
        }