"""
app/models/segmentation_schemas.py - Updated version

Key changes:
1. Removed PRDID requirement
2. Added primary_key field to specify segmentation key
3. Made groupby_attributes more flexible
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from enum import Enum


class AggregationMethod(str, Enum):
    """Methods for handling multiple periods"""
    MEAN_STD = "mean_std"
    WEIGHTED_MEAN = "weighted_mean"
    ROLLING_WINDOW = "rolling_window"


class SegmentationConfig(BaseModel):
    """Configuration for dynamic segmentation"""
    
    primary_key: str = Field(
        "PRDID",
        description="Primary key for segmentation (PRDID, LOCID, CUSTID, etc.)"
    )
    
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
    
    @validator('primary_key')
    def validate_primary_key(cls, v):
        """Ensure primary key is a valid attribute"""
        valid_keys = ['PRDID', 'LOCID', 'CUSTID', 'PRDGRPID', 'REGIONID', 'SALESORGID', 'CHANID', 'DIVID']
        if v not in valid_keys:
            raise ValueError(f'primary_key must be one of: {valid_keys}')
        return v
    
    @validator('y_threshold')
    def validate_thresholds(cls, v, values):
        """Ensure Y threshold is greater than X threshold"""
        if 'x_threshold' in values and v <= values['x_threshold']:
            raise ValueError('y_threshold must be greater than x_threshold')
        return v
    
    @validator('groupby_attributes')
    def validate_attributes(cls, v, values):
        """Validate that primary_key is included in groupby_attributes"""
        # Get primary_key from values - it should already be validated
        primary_key = values.get('primary_key', 'PRDID')
        
        if primary_key not in v:
            raise ValueError(f'{primary_key} must be included in groupby_attributes')
        return v
    
    class Config:
        json_schema_extra = {
            "examples": [
                {
                    "name": "Product-based segmentation",
                    "value": {
                        "primary_key": "PRDID",
                        "groupby_attributes": ["PRDID", "LOCID"],
                        "x_threshold": 10.0,
                        "y_threshold": 25.0,
                        "min_periods": 12
                    }
                },
                {
                    "name": "Location-based segmentation",
                    "value": {
                        "primary_key": "LOCID",
                        "groupby_attributes": ["LOCID"],
                        "x_threshold": 10.0,
                        "y_threshold": 25.0,
                        "min_periods": 12
                    }
                },
                {
                    "name": "Customer-based segmentation",
                    "value": {
                        "primary_key": "CUSTID",
                        "groupby_attributes": ["CUSTID", "PRDID"],
                        "x_threshold": 10.0,
                        "y_threshold": 25.0,
                        "min_periods": 12
                    }
                }
            ]
        }


class AttributeInfo(BaseModel):
    """Information about a single attribute"""
    field: str
    name: str
    description: str
    can_be_primary: bool  # NEW: indicates if can be used as primary key
    unique_values: int


class RecommendedCombination(BaseModel):
    """Recommended attribute combination"""
    level: str
    primary_key: str  # NEW: primary key for this combination
    attributes: List[str]
    description: str
    estimated_segments: int
    use_case: str


class AvailableAttributesResponse(BaseModel):
    """Response listing available attributes for segmentation"""
    available_attributes: List[AttributeInfo]
    current_data_attributes: List[str]
    recommended_combinations: List[RecommendedCombination]
    timestamp: str


class SegmentationPreviewResponse(BaseModel):
    """Preview of segmentation configuration"""
    config: SegmentationConfig
    estimated_segments: int
    data_coverage: Dict[str, Any]
    warnings: List[str] = Field(default_factory=list)
    timestamp: str


class DynamicXYZAnalysisResponse(BaseModel):
    """Response for dynamic XYZ analysis"""
    total_records: int
    unique_segments: int
    primary_key: str  # NEW: indicates which key was used
    segmentation_level: List[str]
    segment_distribution: Dict[str, int]
    analysis_params: Dict[str, Any]
    data: List[Dict[str, Any]]
    data_quality: Dict[str, Any]
    timestamp: str