import logging
from datetime import datetime, timedelta
from src.utils.aws_utils import create_aws_client, retry_aws_api

logger = logging.getLogger('emr_scaling')

class ResourceMonitor:
    """
    Monitors EMR cluster resource utilization metrics.
    """
    
    def __init__(self, config, emr_manager):
        """
        Initialize the ResourceMonitor.
        
        Args:
            config: Configuration dictionary
            emr_manager: EMRClusterManager instance
        """
        self.config = config
        self.emr_manager = emr_manager
        self.cluster_id = config['emr']['cluster_id']
        self.region = config['emr']['region']
        self.history_periods = config['monitoring']['history_periods']
        
        logger.info(f"Initializing Resource Monitor for cluster {self.cluster_id}")
        
        # Create CloudWatch client
        self.cloudwatch = create_aws_client('cloudwatch', self.region)
        
        # Initialize samples list to store (timestamp, utilization) tuples
        self.samples = []
    
    @retry_aws_api
    def get_current_utilization(self):
        """
        Get the current YARN memory utilization from CloudWatch.
        
        Returns:
            float: Current utilization as a value between 0 and 1
        """
        logger.debug("Getting current YARN memory utilization")
        
        try:
            # Check if cluster is active
            if not self.emr_manager.is_cluster_active():
                logger.warning("Cluster is not active, cannot get utilization")
                return 0.0
            
            # Get YARN memory available percentage from CloudWatch
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=10)
            
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/ElasticMapReduce',
                MetricName='YARNMemoryAvailablePercentage',
                Dimensions=[
                    {
                        'Name': 'JobFlowId',
                        'Value': self.cluster_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=300,  # 5 minutes
                Statistics=['Average']
            )
            
            # Check if we got any datapoints
            if not response['Datapoints']:
                logger.warning("No datapoints returned for YARNMemoryAvailablePercentage")
                return 0.0
            
            # Sort datapoints by timestamp and get the most recent one
            datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            latest_datapoint = datapoints[-1]
            
            # Convert available percentage to utilization (1 - available percentage)
            available_percentage = latest_datapoint['Average']
            utilization = 1.0 - (available_percentage / 100.0)
            
            logger.info(f"Current YARN memory utilization: {utilization:.2f} (available: {available_percentage:.2f}%)")
            return utilization
        
        except Exception as e:
            logger.error(f"Failed to get current utilization: {str(e)}")
            return 0.0
    
    def add_sample(self):
        """
        Get current utilization and add to samples.
        
        Returns:
            float: Current utilization
        """
        # Get current utilization
        utilization = self.get_current_utilization()
        
        # Add sample with timestamp
        timestamp = datetime.now()
        self.samples.append((timestamp, utilization))
        
        # Keep only the configured number of historical periods
        if len(self.samples) > self.history_periods:
            self.samples.pop(0)
        
        logger.debug(f"Added utilization sample: {utilization:.2f} at {timestamp}")
        return utilization
    
    def get_samples(self):
        """
        Get all samples.
        
        Returns:
            list: List of (timestamp, utilization) tuples
        """
        return self.samples
    
    def get_weighted_average(self):
        """
        Calculate weighted average of utilization samples.
        More recent samples have higher weights.
        
        Returns:
            float: Weighted average utilization
        """
        if not self.samples:
            return 0.0
        
        total_weight = 0.0
        weighted_sum = 0.0
        decay_factor = self.config['weights']['decay_factor']
        
        # Sort samples by timestamp to ensure correct ordering
        sorted_samples = sorted(self.samples, key=lambda x: x[0])
        utilization_values = [sample[1] for sample in sorted_samples]
        
        # Apply exponential decay weights (most recent has highest weight)
        for i, utilization in enumerate(reversed(utilization_values)):
            weight = decay_factor ** i  # Most recent sample has weight = 1
            weighted_sum += utilization * weight
            total_weight += weight
        
        weighted_avg = weighted_sum / total_weight
        logger.debug(f"Weighted average utilization: {weighted_avg:.2f}")
        return weighted_avg
    
    def get_sample_count(self):
        """
        Get the number of samples.
        
        Returns:
            int: Number of samples
        """
        return len(self.samples)
    
    def clear_samples(self):
        """
        Clear all samples.
        """
        self.samples = []
        logger.debug("Cleared all utilization samples")
