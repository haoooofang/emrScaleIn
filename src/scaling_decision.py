import logging
import math

logger = logging.getLogger('emr_scaling')

class ScalingDecisionMaker:
    """
    Makes scaling decisions based on resource utilization data.
    """
    
    def __init__(self, config):
        """
        Initialize the ScalingDecisionMaker.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.low_threshold = config['thresholds']['low_utilization']
        self.high_threshold = config['thresholds']['high_utilization']
        self.target_utilization = config['thresholds']['target_utilization']
        self.decay_factor = config['weights']['decay_factor']
        self.threshold_periods = config['monitoring']['threshold_periods']
        self.min_capacity = config['monitoring'].get('min_capacity', 1)
        
        logger.info(f"Initializing Scaling Decision Maker with thresholds: "
                   f"low={self.low_threshold:.2f}, high={self.high_threshold:.2f}, "
                   f"target={self.target_utilization:.2f}")
    
    def should_scale_down(self, samples):
        """
        Determine if the cluster should be scaled down based on weighted
        count of periods below the utilization threshold.
        
        Args:
            samples: List of (timestamp, utilization) tuples
        
        Returns:
            bool: True if cluster should be scaled down
        """
        if len(samples) < self.threshold_periods:
            logger.info(f"Not enough samples ({len(samples)}/{self.threshold_periods}) to make scaling decision")
            return False
        
        # Sort samples by timestamp
        sorted_samples = sorted(samples, key=lambda x: x[0])
        utilization_values = [sample[1] for sample in sorted_samples]
        
        # Calculate weighted count of periods below threshold
        weighted_count = 0.0
        
        for i, utilization in enumerate(reversed(utilization_values)):
            if utilization < self.low_threshold:
                weight = self.decay_factor ** i
                weighted_count += weight
                logger.debug(f"Sample {i} is below threshold: {utilization:.2f} < {self.low_threshold:.2f}, weight: {weight:.2f}")
        
        # Calculate threshold weight sum (sum of weights for threshold_periods)
        threshold_weight_sum = sum(self.decay_factor ** i for i in range(self.threshold_periods))
        
        # Determine if enough periods are below threshold (using 80% of possible weight)
        required_weight = threshold_weight_sum * 0.8
        should_scale = weighted_count >= required_weight
        
        logger.info(f"Scaling decision: {should_scale} (weighted count: {weighted_count:.2f}, required: {required_weight:.2f})")
        return should_scale
    
    def calculate_target_capacity(self, current_utilization, current_capacity):
        """
        Calculate the optimal target capacity to achieve target utilization.
        
        Args:
            current_utilization: Current resource utilization (0-1)
            current_capacity: Current cluster capacity units
        
        Returns:
            int: Target capacity units
        """
        # Avoid division by zero
        if current_utilization <= 0.01:
            logger.warning("Current utilization is too low, using minimum capacity")
            return self.min_capacity
        
        # Calculate capacity needed to achieve target utilization
        # Formula: new_capacity = current_capacity * (current_util / target_util)
        raw_target_capacity = current_capacity * (current_utilization / self.target_utilization)
        
        # Round to nearest integer and apply minimum capacity constraint
        target_capacity = max(self.min_capacity, round(raw_target_capacity))
        
        # Log the calculation
        logger.info(f"Capacity calculation: current_utilization={current_utilization:.2f}, "
                   f"current_capacity={current_capacity}, target_utilization={self.target_utilization:.2f}, "
                   f"calculated_capacity={target_capacity}")
        
        return target_capacity
    
    def should_restore_capacity(self, current_utilization):
        """
        Determine if the original capacity should be restored.
        
        Args:
            current_utilization: Current resource utilization (0-1)
            
        Returns:
            bool: True if capacity should be restored
        """
        should_restore = current_utilization > self.high_threshold
        
        if should_restore:
            logger.info(f"Should restore capacity: utilization {current_utilization:.2f} > threshold {self.high_threshold:.2f}")
        
        return should_restore
    
    def get_scaling_summary(self, samples):
        """
        Get a summary of the scaling decision factors.
        
        Args:
            samples: List of (timestamp, utilization) tuples
            
        Returns:
            dict: Summary of scaling decision factors
        """
        if not samples:
            return {
                'sample_count': 0,
                'avg_utilization': 0.0,
                'weighted_avg': 0.0,
                'below_threshold_count': 0,
                'weighted_below_count': 0.0,
                'should_scale_down': False
            }
        
        # Sort samples by timestamp
        sorted_samples = sorted(samples, key=lambda x: x[0])
        utilization_values = [sample[1] for sample in sorted_samples]
        
        # Calculate average utilization
        avg_utilization = sum(utilization_values) / len(utilization_values)
        
        # Calculate weighted average
        total_weight = 0.0
        weighted_sum = 0.0
        
        for i, utilization in enumerate(reversed(utilization_values)):
            weight = self.decay_factor ** i
            weighted_sum += utilization * weight
            total_weight += weight
        
        weighted_avg = weighted_sum / total_weight
        
        # Count samples below threshold
        below_threshold_count = sum(1 for u in utilization_values if u < self.low_threshold)
        
        # Calculate weighted count of samples below threshold
        weighted_below_count = 0.0
        
        for i, utilization in enumerate(reversed(utilization_values)):
            if utilization < self.low_threshold:
                weighted_below_count += self.decay_factor ** i
        
        # Determine if should scale down
        should_scale_down = self.should_scale_down(samples)
        
        return {
            'sample_count': len(samples),
            'avg_utilization': avg_utilization,
            'weighted_avg': weighted_avg,
            'below_threshold_count': below_threshold_count,
            'weighted_below_count': weighted_below_count,
            'should_scale_down': should_scale_down
        }
