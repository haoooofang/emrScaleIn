import logging
import copy

import boto3

from src.utils.aws_utils import create_aws_client, retry_aws_api

logger = logging.getLogger('emr_scaling')

class EMRClusterManager:
    """
    Manages interactions with the AWS EMR API for cluster scaling operations.
    """
    
    def __init__(self, config):
        """
        Initialize the EMRClusterManager.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.cluster_id = config['emr']['cluster_id']
        self.region = config['emr']['region']
        
        logger.info(f"Initializing EMR Cluster Manager for cluster {self.cluster_id} in {self.region}")
        
        # Create EMR client
        self.emr_client = create_aws_client('emr', self.region)
        
        # Store original scaling configuration
        self.original_scaling_config = None
        self.current_scaling_config = None
        self.save_original_config()
    
    @retry_aws_api
    def save_original_config(self):
        """
        Get and store the original scaling configuration.
        """
        logger.info(f"Retrieving original scaling configuration for cluster {self.cluster_id}")
        
        try:
            response = self.emr_client.get_managed_scaling_policy(ClusterId=self.cluster_id)

            if 'ManagedScalingPolicy' not in response:
                logger.warning(f"Cluster {self.cluster_id} does not have a managed scaling policy")
                self.original_scaling_config = None
                self.current_scaling_config = None
                return
            
            self.original_scaling_config = response['ManagedScalingPolicy']
            self.current_scaling_config = copy.deepcopy(self.original_scaling_config)
            
            logger.info(f"Original scaling configuration: {self.original_scaling_config}")
        except Exception as e:
            logger.error(f"Failed to retrieve original scaling configuration: {str(e)}")
            raise
    
    @retry_aws_api
    def get_current_capacity(self):
        """
        Get the current maximum capacity of the cluster.
        
        Returns:
            int: Current maximum capacity units
        """
        if not self.current_scaling_config:
            logger.warning("No scaling configuration available")
            return 0
        
        try:
            max_capacity = self.current_scaling_config['ComputeLimits']['MaximumCapacityUnits']
            logger.debug(f"Current maximum capacity: {max_capacity}")
            return max_capacity
        except KeyError:
            logger.warning("MaximumCapacityUnits not found in scaling configuration")
            return 0
    
    @retry_aws_api
    def update_max_capacity(self, max_capacity):
        """
        Update the maximum capacity of the cluster.
        
        Args:
            max_capacity: New maximum capacity units
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.current_scaling_config:
            logger.error("Cannot update capacity: No scaling configuration available")
            return False
        
        logger.info(f"Updating maximum capacity to {max_capacity}")
        
        try:
            # Create a deep copy of the current configuration
            new_config = copy.deepcopy(self.current_scaling_config)
            
            # Update the maximum capacity
            new_config['ComputeLimits']['MaximumCapacityUnits'] = max_capacity
            new_config['ComputeLimits']['MaximumOnDemandCapacityUnits'] = max_capacity
            
            # Update the scaling policy
            self.emr_client.put_managed_scaling_policy(
                ClusterId=self.cluster_id,
                ManagedScalingPolicy=new_config
            )
            
            # Update the current configuration
            self.current_scaling_config = new_config
            
            logger.info(f"Successfully updated maximum capacity to {max_capacity}")
            return True
        except Exception as e:
            logger.error(f"Failed to update maximum capacity: {str(e)}")
            return False
    
    @retry_aws_api
    def restore_original_capacity(self):
        """
        Restore the original scaling configuration.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.original_scaling_config:
            logger.warning("No original scaling configuration to restore")
            return False
        
        logger.info("Restoring original scaling configuration")
        
        try:
            # Update the scaling policy
            self.emr_client.put_managed_scaling_policy(
                ClusterId=self.cluster_id,
                ManagedScalingPolicy=self.original_scaling_config
            )
            
            # Update the current configuration
            self.current_scaling_config = copy.deepcopy(self.original_scaling_config)
            
            logger.info("Successfully restored original scaling configuration")
            return True
        except Exception as e:
            logger.error(f"Failed to restore original scaling configuration: {str(e)}")
            return False
    
    @retry_aws_api
    def get_cluster_state(self):
        """
        Get the current state of the cluster.
        
        Returns:
            str: Cluster state
        """
        try:
            response = self.emr_client.describe_cluster(ClusterId=self.cluster_id)
            
            if 'Cluster' not in response or 'Status' not in response['Cluster']:
                logger.error(f"Invalid response from describe_cluster: {response}")
                return "UNKNOWN"
            
            state = response['Cluster']['Status']['State']
            logger.debug(f"Cluster state: {state}")
            return state
        except Exception as e:
            logger.error(f"Failed to get cluster state: {str(e)}")
            return "UNKNOWN"
    
    def is_cluster_active(self):
        """
        Check if the cluster is in an active state.
        
        Returns:
            bool: True if the cluster is active, False otherwise
        """
        active_states = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        state = self.get_cluster_state()
        
        is_active = state in active_states
        logger.debug(f"Cluster active: {is_active} (state: {state})")
        return is_active
