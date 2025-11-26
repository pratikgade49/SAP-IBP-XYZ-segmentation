"""
app/services/dynamic_analysis_service.py - Updated version

Key changes:
1. No longer requires PRDID specifically
2. Uses primary_key from config
3. More flexible grouping logic
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from scipy import stats
from app.utils.logger import get_logger
from app.models.segmentation_schemas import SegmentationConfig

logger = get_logger(__name__)


class DynamicAnalysisService:
    """Service for performing dynamic XYZ segmentation analysis"""
    
    @staticmethod
    def get_recommended_combinations(df: pd.DataFrame, attributes: List[str]) -> List[dict]:
        """Generate recommended attribute combinations based on data"""
        from app.models.segmentation_schemas import RecommendedCombination
        
        recommendations = []
        
        # Single key recommendations
        for key in ['PRDID', 'LOCID', 'CUSTID']:
            if key in attributes:
                rec = RecommendedCombination(
                    level=f'{key} Level',
                    primary_key=key,
                    attributes=[key],
                    description=f'Single {key} segmentation',
                    estimated_segments=int(df[key].nunique()),
                    use_case=f'Basic {key.lower()}-level classification'
                )
                recommendations.append(rec.model_dump())
        
        # Multi-dimensional combinations
        if 'PRDID' in attributes and 'LOCID' in attributes:
            estimated = df.groupby(['PRDID', 'LOCID']).ngroups
            rec = RecommendedCombination(
                level='Product-Location Level',
                primary_key='PRDID',
                attributes=['PRDID', 'LOCID'],
                description='Location-specific product segmentation',
                estimated_segments=int(estimated),
                use_case='Location-specific safety stock, replenishment'
            )
            recommendations.append(rec.model_dump())
        
        if 'LOCID' in attributes and 'CUSTID' in attributes:
            estimated = df.groupby(['LOCID', 'CUSTID']).ngroups
            rec = RecommendedCombination(
                level='Location-Customer Level',
                primary_key='LOCID',
                attributes=['LOCID', 'CUSTID'],
                description='Customer demand patterns by location',
                estimated_segments=int(estimated),
                use_case='Distribution center customer analysis'
            )
            recommendations.append(rec.model_dump())
        
        return recommendations
    
    @staticmethod
    def preview_segmentation(
        df: pd.DataFrame,
        config: SegmentationConfig
    ) -> Dict[str, Any]:
        """Preview segmentation configuration without full analysis"""
        logger.info(f"Previewing segmentation for primary_key={config.primary_key}, attributes={config.groupby_attributes}")
        
        warnings = []
        
        # Check if all requested attributes exist
        missing_attrs = [attr for attr in config.groupby_attributes if attr not in df.columns]
        if missing_attrs:
            warnings.append(f"Attributes not found in data: {missing_attrs}")
            return {
                'error': f"Missing attributes: {missing_attrs}",
                'warnings': warnings
            }
        
        # Verify primary key exists
        if config.primary_key not in df.columns:
            return {
                'error': f"Primary key {config.primary_key} not found in data",
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
            'primary_key_unique_values': int(df[config.primary_key].nunique()),
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
        Perform XYZ segmentation with dynamic primary key configuration
        
        Args:
            df: DataFrame with time-series data
            config: Segmentation configuration with primary_key specified
            
        Returns:
            Tuple of (result_df, data_quality_metrics)
        """
        logger.info(
            f"Starting dynamic XYZ analysis with primary_key={config.primary_key}, "
            f"attributes={config.groupby_attributes}, X={config.x_threshold}, Y={config.y_threshold}"
        )
        
        if df.empty:
            logger.warning("Empty DataFrame provided for analysis")
            return pd.DataFrame(), {}
        
        # Validate attributes exist
        missing_attrs = [attr for attr in config.groupby_attributes if attr not in df.columns]
        if missing_attrs:
            raise ValueError(f"Missing attributes in data: {missing_attrs}")
        
        # Validate primary key
        if config.primary_key not in df.columns:
            raise ValueError(f"Primary key {config.primary_key} not found in data")
        
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
            'primary_key': config.primary_key,
            'primary_key_unique_values': int(df[config.primary_key].nunique()),
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
        logger.info(f"Primary key {config.primary_key} has {data_quality['primary_key_unique_values']} unique values")
        
        return group_stats, data_quality
    
    @staticmethod
    def _remove_outliers(df: pd.DataFrame, config: SegmentationConfig) -> pd.DataFrame:
        """Remove statistical outliers from the dataset"""
        initial_count = len(df)
        
        df_clean = df.copy()
        
        def remove_group_outliers(group):
            if len(group) < 3:  # Need at least 3 points for z-score
                return group
            z_scores = np.abs(stats.zscore(group['ACTUALSQTY']))
            return group[z_scores < config.outlier_threshold]
        
        df_clean = df_clean.groupby(config.groupby_attributes).apply(
            remove_group_outliers
        ).reset_index(drop=True)
        
        removed_count = initial_count - len(df_clean)
        logger.info(f"Removed {removed_count} outlier records ({removed_count/initial_count*100:.2f}%)")
        
        return df_clean