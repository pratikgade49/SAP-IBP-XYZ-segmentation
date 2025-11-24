"""
Service for XYZ Segmentation Analysis
Classifies planning objects based on demand volatility using Coefficient of Variation (CV)
"""
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
from sklearn.cluster import KMeans
from utils.logger import get_logger

logger = get_logger(__name__)


class SegmentationType(Enum):
    """XYZ segment types"""
    X = "X"  # Low variation - highly predictable
    Y = "Y"  # Moderate variation - somewhat predictable
    Z = "Z"  # High variation - difficult to forecast


class CalculationStrategy(Enum):
    """Segmentation calculation strategies"""
    CALCULATE_VARIATION = "calculate_variation"  # Calculate CV during segmentation
    AGGREGATE_PERIODS = "aggregate_periods"      # Use pre-calculated metrics


@dataclass
class SegmentationThresholds:
    """Thresholds for XYZ segmentation"""
    x_threshold: float  # CV <= x_threshold = X segment
    y_threshold: float  # x_threshold < CV <= y_threshold = Y segment
    # CV > y_threshold = Z segment
    
    def __post_init__(self):
        if self.x_threshold >= self.y_threshold:
            raise ValueError("x_threshold must be less than y_threshold")


@dataclass
class SegmentationResult:
    """Result of XYZ segmentation for a single item"""
    item_id: str
    segment: SegmentationType
    coefficient_of_variation: float
    mean_demand: float
    std_deviation: float
    data_points: int


class XYZSegmentationService:
    """Handle XYZ segmentation analysis for demand planning"""
    
    def __init__(self):
        # Default thresholds (can be customized per use case)
        self.default_thresholds = SegmentationThresholds(
            x_threshold=0.5,   # X: CV <= 0.5 (50%)
            y_threshold=1.0    # Y: 0.5 < CV <= 1.0 (100%)
        )
    
    def segment_items_cv(
        self,
        demand_data: Dict[str, List[float]],
        thresholds: Optional[SegmentationThresholds] = None,
        remove_zeros: bool = True,
        use_cv_squared: bool = False
    ) -> Dict[str, SegmentationResult]:
        """
        Segment items using Coefficient of Variation (CV) calculation strategy
        
        Args:
            demand_data: Dictionary mapping item_id to list of historical demand values
            thresholds: Custom segmentation thresholds (uses default if None)
            remove_zeros: Whether to exclude zero demand periods from calculation
            use_cv_squared: Use CV^2 instead of CV for classification
            
        Returns:
            Dictionary mapping item_id to SegmentationResult
        """
        logger.info(f"Starting CV-based XYZ segmentation for {len(demand_data)} items")
        
        if thresholds is None:
            thresholds = self.default_thresholds
        
        results = {}
        
        for item_id, demand_values in demand_data.items():
            try:
                # Filter demand values
                filtered_demand = self._filter_demand_values(demand_values, remove_zeros)
                
                if len(filtered_demand) < 2:
                    logger.warning(f"Item {item_id}: Insufficient data points ({len(filtered_demand)})")
                    continue
                
                # Calculate CV
                mean_demand = np.mean(filtered_demand)
                std_dev = np.std(filtered_demand, ddof=1)  # Sample std deviation
                
                if mean_demand == 0:
                    logger.warning(f"Item {item_id}: Mean demand is zero, skipping")
                    continue
                
                cv = std_dev / mean_demand
                
                # Use CV squared if specified
                comparison_value = cv ** 2 if use_cv_squared else cv
                
                # Determine segment
                segment = self._classify_segment(comparison_value, thresholds)
                
                results[item_id] = SegmentationResult(
                    item_id=item_id,
                    segment=segment,
                    coefficient_of_variation=cv,
                    mean_demand=mean_demand,
                    std_deviation=std_dev,
                    data_points=len(filtered_demand)
                )
                
            except Exception as e:
                logger.error(f"Error segmenting item {item_id}: {str(e)}")
                continue
        
        logger.info(f"Segmentation completed. Processed {len(results)} items")
        self._log_segment_distribution(results)
        
        return results
    
    def segment_items_aggregated(
        self,
        metric_data: Dict[str, List[float]],
        thresholds: Optional[SegmentationThresholds] = None,
        aggregation_method: str = "average"
    ) -> Dict[str, SegmentationResult]:
        """
        Segment items using pre-calculated metrics (e.g., MAPE) - Aggregate over Periods strategy
        
        Args:
            metric_data: Dictionary mapping item_id to list of pre-calculated metric values
                        (e.g., MAPE values from Manage Forecast Error Calculations)
            thresholds: Custom segmentation thresholds
            aggregation_method: Method to aggregate metrics ('average', 'sum', 'min', 'max')
            
        Returns:
            Dictionary mapping item_id to SegmentationResult
        """
        logger.info(f"Starting aggregated metric XYZ segmentation for {len(metric_data)} items")
        
        if thresholds is None:
            thresholds = self.default_thresholds
        
        results = {}
        
        for item_id, metric_values in metric_data.items():
            try:
                if len(metric_values) == 0:
                    logger.warning(f"Item {item_id}: No metric values provided")
                    continue
                
                # Aggregate metric values
                if aggregation_method == "average":
                    aggregated_value = np.mean(metric_values)
                elif aggregation_method == "sum":
                    aggregated_value = np.sum(metric_values)
                elif aggregation_method == "min":
                    aggregated_value = np.min(metric_values)
                elif aggregation_method == "max":
                    aggregated_value = np.max(metric_values)
                else:
                    raise ValueError(f"Invalid aggregation method: {aggregation_method}")
                
                # Determine segment
                segment = self._classify_segment(aggregated_value, thresholds)
                
                results[item_id] = SegmentationResult(
                    item_id=item_id,
                    segment=segment,
                    coefficient_of_variation=aggregated_value,  # Using metric value
                    mean_demand=np.mean(metric_values),
                    std_deviation=np.std(metric_values, ddof=1),
                    data_points=len(metric_values)
                )
                
            except Exception as e:
                logger.error(f"Error segmenting item {item_id}: {str(e)}")
                continue
        
        logger.info(f"Aggregated segmentation completed. Processed {len(results)} items")
        self._log_segment_distribution(results)
        
        return results
    
    def segment_items_kmeans(
        self,
        demand_data: Dict[str, List[float]],
        n_clusters: int = 3,
        remove_zeros: bool = True
    ) -> Dict[str, SegmentationResult]:
        """
        Segment items using K-means clustering machine learning method
        Useful when thresholds are unknown
        
        Args:
            demand_data: Dictionary mapping item_id to list of historical demand values
            n_clusters: Number of clusters (default: 3 for X, Y, Z)
            remove_zeros: Whether to exclude zero demand periods
            
        Returns:
            Dictionary mapping item_id to SegmentationResult
        """
        logger.info(f"Starting K-means XYZ segmentation for {len(demand_data)} items")
        
        # Calculate CV for all items
        cv_values = []
        item_ids = []
        item_stats = {}
        
        for item_id, demand_values in demand_data.items():
            try:
                filtered_demand = self._filter_demand_values(demand_values, remove_zeros)
                
                if len(filtered_demand) < 2:
                    continue
                
                mean_demand = np.mean(filtered_demand)
                std_dev = np.std(filtered_demand, ddof=1)
                
                if mean_demand == 0:
                    continue
                
                cv = std_dev / mean_demand
                cv_values.append(cv)
                item_ids.append(item_id)
                item_stats[item_id] = {
                    'cv': cv,
                    'mean': mean_demand,
                    'std': std_dev,
                    'count': len(filtered_demand)
                }
                
            except Exception as e:
                logger.error(f"Error calculating CV for item {item_id}: {str(e)}")
                continue
        
        if len(cv_values) < n_clusters:
            raise ValueError(f"Insufficient items ({len(cv_values)}) for {n_clusters} clusters")
        
        # Perform K-means clustering
        cv_array = np.array(cv_values).reshape(-1, 1)
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(cv_array)
        
        # Sort clusters by centroid value (lowest = X, highest = Z)
        cluster_centers = kmeans.cluster_centers_.flatten()
        sorted_cluster_indices = np.argsort(cluster_centers)
        
        # Map clusters to segments
        segment_mapping = {
            sorted_cluster_indices[0]: SegmentationType.X,
            sorted_cluster_indices[1]: SegmentationType.Y,
            sorted_cluster_indices[2]: SegmentationType.Z
        }
        
        # Create results
        results = {}
        for i, item_id in enumerate(item_ids):
            cluster = cluster_labels[i]
            segment = segment_mapping[cluster]
            stats = item_stats[item_id]
            
            results[item_id] = SegmentationResult(
                item_id=item_id,
                segment=segment,
                coefficient_of_variation=stats['cv'],
                mean_demand=stats['mean'],
                std_deviation=stats['std'],
                data_points=stats['count']
            )
        
        logger.info(f"K-means segmentation completed. Processed {len(results)} items")
        logger.info(f"Cluster centers: X={cluster_centers[sorted_cluster_indices[0]]:.3f}, "
                   f"Y={cluster_centers[sorted_cluster_indices[1]]:.3f}, "
                   f"Z={cluster_centers[sorted_cluster_indices[2]]:.3f}")
        self._log_segment_distribution(results)
        
        return results
    
    def calculate_deseasonalized_cv(
        self,
        demand_data: Dict[str, List[float]],
        seasonal_period: int = 12,
        thresholds: Optional[SegmentationThresholds] = None
    ) -> Dict[str, SegmentationResult]:
        """
        Calculate CV after removing seasonality (simulates time-series analysis integration)
        
        Args:
            demand_data: Dictionary mapping item_id to list of historical demand values
            seasonal_period: Period length for seasonality (e.g., 12 for monthly data)
            thresholds: Custom segmentation thresholds
            
        Returns:
            Dictionary mapping item_id to SegmentationResult
        """
        logger.info(f"Starting deseasonalized CV segmentation for {len(demand_data)} items")
        
        deseasonalized_data = {}
        
        for item_id, demand_values in demand_data.items():
            try:
                if len(demand_values) < seasonal_period * 2:
                    logger.warning(f"Item {item_id}: Insufficient data for deseasonalization")
                    deseasonalized_data[item_id] = demand_values
                    continue
                
                # Simple seasonal decomposition (moving average method)
                deseasonalized = self._remove_seasonality(
                    demand_values, 
                    seasonal_period
                )
                deseasonalized_data[item_id] = deseasonalized
                
            except Exception as e:
                logger.error(f"Error deseasonalizing item {item_id}: {str(e)}")
                deseasonalized_data[item_id] = demand_values
        
        # Calculate CV on deseasonalized data
        return self.segment_items_cv(
            deseasonalized_data,
            thresholds=thresholds,
            remove_zeros=True
        )
    
    def export_to_sap_ibp_format(
        self,
        results: Dict[str, SegmentationResult],
        planning_area_id: str,
        version_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Export segmentation results in SAP IBP import format
        
        Args:
            results: Segmentation results from any segmentation method
            planning_area_id: Planning area ID for IBP
            version_id: Version ID (optional)
            
        Returns:
            List of dictionaries ready for IBP master data import
        """
        logger.info(f"Exporting {len(results)} segmentation results to SAP IBP format")
        
        export_data = []
        
        for item_id, result in results.items():
            record = {
                "PRDID": item_id,  # Assuming product-based segmentation
                "XYZ_SEGMENT": result.segment.value,
                "CV_VALUE": round(result.coefficient_of_variation, 4),
                "MEAN_DEMAND": round(result.mean_demand, 2),
                "STD_DEV": round(result.std_deviation, 2)
            }
            
            export_data.append(record)
        
        logger.info("Export to SAP IBP format completed")
        return export_data
    
    def get_segment_summary(
        self,
        results: Dict[str, SegmentationResult]
    ) -> Dict[str, Any]:
        """
        Get summary statistics for segmentation results
        
        Args:
            results: Segmentation results
            
        Returns:
            Dictionary with summary statistics
        """
        segments = {seg: [] for seg in SegmentationType}
        
        for result in results.values():
            segments[result.segment].append(result)
        
        summary = {
            "total_items": len(results),
            "segments": {}
        }
        
        for segment_type in SegmentationType:
            items = segments[segment_type]
            count = len(items)
            
            if count > 0:
                cvs = [item.coefficient_of_variation for item in items]
                summary["segments"][segment_type.value] = {
                    "count": count,
                    "percentage": round(count / len(results) * 100, 2),
                    "avg_cv": round(np.mean(cvs), 4),
                    "min_cv": round(np.min(cvs), 4),
                    "max_cv": round(np.max(cvs), 4)
                }
            else:
                summary["segments"][segment_type.value] = {
                    "count": 0,
                    "percentage": 0.0
                }
        
        return summary
    
    # ==================== HELPER METHODS ====================
    
    def _filter_demand_values(
        self,
        demand_values: List[float],
        remove_zeros: bool
    ) -> List[float]:
        """Filter demand values based on criteria"""
        filtered = [v for v in demand_values if v is not None and not np.isnan(v)]
        
        if remove_zeros:
            filtered = [v for v in filtered if v != 0]
        
        return filtered
    
    def _classify_segment(
        self,
        value: float,
        thresholds: SegmentationThresholds
    ) -> SegmentationType:
        """Classify value into X, Y, or Z segment"""
        if value <= thresholds.x_threshold:
            return SegmentationType.X
        elif value <= thresholds.y_threshold:
            return SegmentationType.Y
        else:
            return SegmentationType.Z
    
    def _remove_seasonality(
        self,
        demand_values: List[float],
        period: int
    ) -> List[float]:
        """
        Simple seasonal decomposition using moving average
        Returns deseasonalized demand values
        """
        demand_array = np.array(demand_values)
        
        # Calculate seasonal indices using centered moving average
        seasonal_indices = np.ones(period)
        
        for i in range(period):
            period_values = demand_array[i::period]
            if len(period_values) > 0 and np.mean(demand_array) > 0:
                seasonal_indices[i] = np.mean(period_values) / np.mean(demand_array)
        
        # Deseasonalize
        deseasonalized = []
        for i, value in enumerate(demand_array):
            seasonal_idx = i % period
            deseasonalized.append(value / seasonal_indices[seasonal_idx])
        
        return deseasonalized
    
    def _log_segment_distribution(
        self,
        results: Dict[str, SegmentationResult]
    ) -> None:
        """Log distribution of segments"""
        summary = self.get_segment_summary(results)
        
        logger.info("=" * 50)
        logger.info("XYZ Segmentation Distribution:")
        for segment, stats in summary["segments"].items():
            logger.info(f"  {segment}: {stats['count']} items ({stats['percentage']}%) - "
                       f"Avg CV: {stats.get('avg_cv', 0):.4f}")
        logger.info("=" * 50)