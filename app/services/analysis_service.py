import pandas as pd
import numpy as np
from typing import Tuple
from app.utils.logger import get_logger

logger = get_logger(__name__)


class AnalysisService:
    """Service for performing XYZ analysis"""
    
    @staticmethod
    def calculate_xyz_segmentation(
        df: pd.DataFrame,
        x_threshold: float,
        y_threshold: float
    ) -> pd.DataFrame:
        """
        Perform XYZ segmentation based on coefficient of variation
        
        Args:
            df: DataFrame with PRDID and ACTUALSQTY columns
            x_threshold: CV threshold for X segment (stable)
            y_threshold: CV threshold for Y segment (moderate)
            
        Returns:
            DataFrame with product statistics and segments
        """
        logger.info(f"Starting XYZ analysis with thresholds X={x_threshold}, Y={y_threshold}")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for analysis")
            return pd.DataFrame()
        
        # Calculate statistics by product
        logger.debug("Calculating product statistics")
        product_stats = df.groupby('PRDID')['ACTUALSQTY'].agg(['mean', 'std']).reset_index()
        
        # Calculate Coefficient of Variation
        product_stats['CV'] = (product_stats['std'] / product_stats['mean']) * 100
        
        # Handle cases where std is 0 (CV would be 0)
        product_stats['CV'] = product_stats['CV'].fillna(0)
        
        # Apply segmentation logic
        conditions = [
            (product_stats['CV'] <= x_threshold),
            ((product_stats['CV'] > x_threshold) & (product_stats['CV'] <= y_threshold)),
            (product_stats['CV'] > y_threshold)
        ]
        choices = ['X', 'Y', 'Z']
        
        product_stats['XYZ_Segment'] = np.select(conditions, choices, default='Unknown')
        
        # Log segment distribution
        segment_counts = product_stats['XYZ_Segment'].value_counts().to_dict()
        logger.info(f"Segmentation complete: {segment_counts}")
        
        return product_stats
    
    @staticmethod
    def get_segment_summary(df: pd.DataFrame) -> dict:
        """Get summary statistics for each segment"""
        summary = {}
        
        for segment in ['X', 'Y', 'Z']:
            segment_data = df[df['XYZ_Segment'] == segment]
            
            if not segment_data.empty:
                summary[segment] = {
                    'count': len(segment_data),
                    'avg_cv': float(segment_data['CV'].mean()),
                    'avg_mean': float(segment_data['mean'].mean()),
                    'products': segment_data['PRDID'].tolist()
                }
        
        return summary