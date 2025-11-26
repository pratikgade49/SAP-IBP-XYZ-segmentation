"""
app/services/dynamic_analysis_service.py

Enhanced analysis service with dynamic segmentation support
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from scipy import stats
from app.utils.logger import get_logger
from app.models.segmentation_schemas import SegmentationConfig, AggregationMethod

logger = get_logger(__name__)


class DynamicAnalysisService:
    """Service for performing dynamic XYZ segmentation analysis"""
    
    @staticmethod
    def get_available_attributes(df: pd.DataFrame) -> Dict[str, Any]:
        """
        Discover available attributes in the dataset
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with available attributes and metadata
        """
        from app.models.segmentation_schemas import AttributeInfo
        
        logger.info("Discovering available attributes in dataset")
        
        # Define attribute descriptions
        attribute_descriptions = {
            'PRDID': {'name': 'Product ID', 'description': 'Individual product identifier', 'required': True},
            'LOCID': {'name': 'Location ID', 'description': 'Warehouse/location identifier', 'required': False},
            'CUSTID': {'name': 'Customer ID', 'description': 'Customer identifier', 'required': False},
            'PRDGRPID': {'name': 'Product Group', 'description': 'Product category/family', 'required': False},
            'REGIONID': {'name': 'Region ID', 'description': 'Geographic region', 'required': False},
            'SALESORGID': {'name': 'Sales Organization', 'description': 'Sales organization unit', 'required': False},
        }
        
        # Get columns present in data
        current_attributes = [col for col in df.columns if col in attribute_descriptions]
        
        # Build available attributes list
        available = []
        for attr, info in attribute_descriptions.items():
            if attr in current_attributes:
                attr_info = AttributeInfo(
                    field=attr,
                    name=info['name'],
                    description=info['description'],
                    required=info['required'],
                    unique_values=int(df[attr].nunique())
                )
                available.append(attr_info)
        
        # Recommended combinations based on data
        recommended = DynamicAnalysisService._get_recommended_combinations(df, current_attributes)
        
        return {
            'available_attributes': available,
            'current_data_attributes': current_attributes,
            'recommended_combinations': recommended
        }
    
    @staticmethod
    def _get_recommended_combinations(df: pd.DataFrame, attributes: List[str]) -> List[dict]:
        """Generate recommended attribute combinations based on data"""
        from app.models.segmentation_schemas import RecommendedCombination
        
        recommendations = []
        
        # Product-only (baseline)
        if 'PRDID' in attributes:
            rec = RecommendedCombination(
                level='Product Level',
                attributes=['PRDID'],
                description='Basic product-level segmentation',
                estimated_segments=int(df['PRDID'].nunique()),
                use_case='Global product classification, basic inventory policies'
            )
            recommendations.append(rec.model_dump())
        
        # Product-Location (most common)
        if 'PRDID' in attributes and 'LOCID' in attributes:
            estimated = df.groupby(['PRDID', 'LOCID']).ngroups
            rec = RecommendedCombination(
                level='Product-Location Level',
                attributes=['PRDID', 'LOCID'],
                description='Location-specific product segmentation',
                estimated_segments=int(estimated),
                use_case='Location-specific safety stock, replenishment strategies'
            )
            recommendations.append(rec.model_dump())
        
        # Product-Customer
        if 'PRDID' in attributes and 'CUSTID' in attributes:
            estimated = df.groupby(['PRDID', 'CUSTID']).ngroups
            rec = RecommendedCombination(
                level='Product-Customer Level',
                attributes=['PRDID', 'CUSTID'],
                description='Customer-specific product segmentation',
                estimated_segments=int(estimated),
                use_case='Customer-specific service levels, demand forecasting'
            )
            recommendations.append(rec.model_dump())
        
        return recommendations
    
    @staticmethod
    def preview_segmentation(
        df: pd.DataFrame,
        config: SegmentationConfig
    ) -> Dict[str, Any]:
        """
        Preview segmentation configuration without full analysis
        
        Args:
            df: Input DataFrame
            config: Segmentation configuration
            
        Returns:
            Preview results with warnings and estimates
        """
        logger.info(f"Previewing segmentation for attributes: {config.groupby_attributes}")
        
        warnings = []
        
        # Check if all requested attributes exist
        missing_attrs = [attr for attr in config.groupby_attributes if attr not in df.columns]
        if missing_attrs:
            warnings.append(f"Attributes not found in data: {missing_attrs}")
            return {
                'error': f"Missing attributes: {missing_attrs}",
                'warnings': warnings
            }
        
        # Estimate number of segments
        estimated_segments = df.groupby(config.groupby_attributes).ngroups
        
        # Check data sufficiency
        periods_per_group = df.groupby(config.groupby_attributes).size()
        insufficient = (periods_per_group < config.min_periods).sum()
        
        if insufficient > 0:
            warnings.append(
                f"{insufficient}/{estimated_segments} segment groups have fewer than "
                f"{config.min_periods} periods of data"
            )
        
        # Calculate data coverage
        data_coverage = {
            'total_records': len(df),
            'unique_segments': estimated_segments,
            'avg_periods_per_segment': float(periods_per_group.mean()),
            'min_periods_per_segment': int(periods_per_group.min()),
            'max_periods_per_segment': int(periods_per_group.max()),
            'segments_with_sufficient_data': int((periods_per_group >= config.min_periods).sum())
        }
        
        # Check for extremely high cardinality
        if estimated_segments > 10000:
            warnings.append(
                f"High number of segments ({estimated_segments}). Consider using fewer attributes "
                "or adding filters to reduce cardinality."
            )
        
        return {
            'estimated_segments': estimated_segments,
            'data_coverage': data_coverage,
            'warnings': warnings
        }
    
    @staticmethod
    def calculate_dynamic_xyz_segmentation(
        df: pd.DataFrame,
        config: SegmentationConfig
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Perform XYZ segmentation with dynamic configuration
        
        Args:
            df: DataFrame with time-series data
            config: Segmentation configuration
            
        Returns:
            Tuple of (result_df, data_quality_metrics)
        """
        logger.info(
            f"Starting dynamic XYZ analysis with attributes: {config.groupby_attributes}, "
            f"X={config.x_threshold}, Y={config.y_threshold}"
        )
        
        if df.empty:
            logger.warning("Empty DataFrame provided for analysis")
            return pd.DataFrame(), {}
        
        # Validate attributes exist
        missing_attrs = [attr for attr in config.groupby_attributes if attr not in df.columns]
        if missing_attrs:
            raise ValueError(f"Missing attributes in data: {missing_attrs}")
        
        # Remove outliers if requested
        if config.remove_outliers:
            logger.info(f"Removing outliers using {config.outlier_threshold} std threshold")
            df = DynamicAnalysisService._remove_outliers(df, config)
        
        # Calculate statistics by configured grouping
        logger.debug(f"Grouping by: {config.groupby_attributes}")
        
        # Group and calculate basic stats
        group_stats = df.groupby(config.groupby_attributes).agg({
            'ACTUALSQTY': ['mean', 'std', 'count']
        }).reset_index()
        
        # Flatten column names
        group_stats.columns = config.groupby_attributes + ['mean', 'std', 'count']
        
        # Filter by minimum periods
        initial_count = len(group_stats)
        group_stats = group_stats[group_stats['count'] >= config.min_periods].copy()
        excluded_count = initial_count - len(group_stats)
        
        if excluded_count > 0:
            logger.warning(
                f"Excluded {excluded_count} segment groups with insufficient data "
                f"(< {config.min_periods} periods)"
            )
        
        # Calculate Coefficient of Variation
        group_stats['CV'] = (group_stats['std'] / group_stats['mean']) * 100
        
        # Handle edge cases
        group_stats['CV'] = group_stats['CV'].fillna(0)  # When std is 0
        group_stats['CV'] = group_stats['CV'].replace([np.inf, -np.inf], 999)  # When mean is 0
        
        # Apply segmentation logic
        conditions = [
            (group_stats['CV'] <= config.x_threshold),
            ((group_stats['CV'] > config.x_threshold) & (group_stats['CV'] <= config.y_threshold)),
            (group_stats['CV'] > config.y_threshold)
        ]
        choices = ['X', 'Y', 'Z']
        
        group_stats['XYZ_Segment'] = np.select(conditions, choices, default='Unknown')
        
        # Calculate data quality metrics
        segment_counts = group_stats['XYZ_Segment'].value_counts().to_dict()
        
        data_quality = {
            'total_records_analyzed': len(df),
            'unique_segments': len(group_stats),
            'records_with_sufficient_history': len(group_stats),
            'records_excluded': excluded_count,
            'avg_periods_per_segment': float(group_stats['count'].mean()),
            'min_periods_per_segment': int(group_stats['count'].min()) if len(group_stats) > 0 else 0,
            'max_periods_per_segment': int(group_stats['count'].max()) if len(group_stats) > 0 else 0,
            'segment_distribution': segment_counts,
            'avg_cv_by_segment': {
                segment: float(group_stats[group_stats['XYZ_Segment'] == segment]['CV'].mean())
                for segment in ['X', 'Y', 'Z']
                if segment in group_stats['XYZ_Segment'].values
            }
        }
        
        logger.info(f"Segmentation complete: {segment_counts}")
        logger.info(f"Data quality: {excluded_count} excluded, {len(group_stats)} analyzed")
        
        return group_stats, data_quality
    
    @staticmethod
    def _remove_outliers(df: pd.DataFrame, config: SegmentationConfig) -> pd.DataFrame:
        """Remove statistical outliers from the dataset"""
        initial_count = len(df)
        
        # Calculate z-scores within each group
        df_clean = df.copy()
        
        def remove_group_outliers(group):
            z_scores = np.abs(stats.zscore(group['ACTUALSQTY']))
            return group[z_scores < config.outlier_threshold]
        
        df_clean = df_clean.groupby(config.groupby_attributes).apply(
            remove_group_outliers
        ).reset_index(drop=True)
        
        removed_count = initial_count - len(df_clean)
        logger.info(f"Removed {removed_count} outlier records ({removed_count/initial_count*100:.2f}%)")
        
        return df_clean
    
    @staticmethod
    def get_segment_details(
        result_df: pd.DataFrame,
        groupby_attributes: List[str]
    ) -> Dict[str, Any]:
        """Get detailed breakdown of segments"""
        details = {}
        
        for segment in ['X', 'Y', 'Z']:
            segment_data = result_df[result_df['XYZ_Segment'] == segment]
            
            if not segment_data.empty:
                # Create segment key (combination of all groupby attributes)
                segment_keys = segment_data[groupby_attributes].to_dict('records')
                
                details[segment] = {
                    'count': len(segment_data),
                    'avg_cv': float(segment_data['CV'].mean()),
                    'avg_mean_demand': float(segment_data['mean'].mean()),
                    'avg_std_demand': float(segment_data['std'].mean()),
                    'min_cv': float(segment_data['CV'].min()),
                    'max_cv': float(segment_data['CV'].max()),
                    'sample_records': segment_keys[:5]  # First 5 for preview
                }
        
        return details