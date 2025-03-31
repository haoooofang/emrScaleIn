import logging
import signal
import time
import os
from datetime import datetime

from src.config_manager import ConfigurationManager
from src.emr_manager import EMRClusterManager
from src.resource_monitor import ResourceMonitor
from src.scaling_decision import ScalingDecisionMaker
from src.utils.logging_utils import setup_logging

class EMRScalingService:
    """
    Main service class for EMR auto-scaling.
    """
    
    def __init__(self, config_path):
        """
        Initialize the EMR scaling service.
        
        Args:
            config_path: Path to the configuration file
        """
        # Set up logging
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        log_file = os.path.join(log_dir, 'emr_scaling.log')
        self.logger = setup_logging(log_level=logging.INFO, log_file=log_file)
        
        self.logger.info("Initializing EMR Scaling Service")
        self.logger.info(f"Using configuration file: {config_path}")
        
        # Initialize components
        self.config_path = config_path
        self.config_manager = ConfigurationManager(config_path)
        self.config = self.config_manager.get_config()
        
        self.emr_manager = EMRClusterManager(self.config)
        self.resource_monitor = ResourceMonitor(self.config, self.emr_manager)
        self.decision_maker = ScalingDecisionMaker(self.config)
        
        # Service state
        self.running = False
        self.is_downscaled = False
        self.last_scaling_time = None
        self.last_status_log_time = None
        
        # Set up signal handlers
        self.setup_signal_handlers()
        
        self.logger.info("EMR Scaling Service initialized")
    
    def setup_signal_handlers(self):
        """
        Set up signal handlers for graceful shutdown.
        """
        self.logger.info("Setting up signal handlers")
        
        def handle_signal(signum, frame):
            sig_name = signal.Signals(signum).name
            self.logger.info(f"Received signal {sig_name} ({signum})")
            
            if signum in (signal.SIGINT, signal.SIGTERM):
                self.logger.info("Shutting down...")
                self.stop()
            elif signum == signal.SIGHUP:
                self.logger.info("Reloading configuration...")
                self.reload_config()
        
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGHUP, handle_signal)
    
    def reload_config(self):
        """
        Reload the configuration file.
        """
        self.logger.info(f"Reloading configuration from {self.config_path}")
        
        try:
            self.config_manager = ConfigurationManager(self.config_path)
            self.config = self.config_manager.get_config()
            
            # Update components with new configuration
            self.decision_maker = ScalingDecisionMaker(self.config)
            
            self.logger.info("Configuration reloaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to reload configuration: {str(e)}")
    
    def run(self):
        """
        Run the service main loop.
        """
        self.logger.info("Starting EMR Scaling Service")
        self.running = True
        
        # Main service loop
        while self.running:
            try:
                self.monitoring_cycle()
                
                # Sleep until next sampling interval
                interval = self.config['monitoring']['sampling_interval']
                self.logger.debug(f"Sleeping for {interval} seconds until next monitoring cycle")
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring cycle: {str(e)}", exc_info=True)
                # Implement backoff/retry logic
                self.logger.info("Retrying after 60 seconds...")
                time.sleep(60)  # Retry after a minute
        
        self.logger.info("EMR Scaling Service stopped")
    
    def monitoring_cycle(self):
        """
        Run a single monitoring cycle.
        """
        self.logger.debug("Starting monitoring cycle")
        
        # Check if cluster is active
        if not self.emr_manager.is_cluster_active():
            self.logger.warning("Cluster is not active, skipping monitoring cycle")
            return
        
        # Monitor resources
        current_utilization = self.resource_monitor.add_sample()
        
        # Log status periodically (every hour)
        current_time = datetime.now()
        if (self.last_status_log_time is None or 
            (current_time - self.last_status_log_time).total_seconds() > 3600):
            self.log_status()
            self.last_status_log_time = current_time
        
        # Check if we're in downscaled state
        if self.is_downscaled:
            self.logger.debug("Cluster is in downscaled state")
            
            # Check if we should restore capacity
            if self.decision_maker.should_restore_capacity(current_utilization):
                self.logger.info(f"High utilization detected ({current_utilization:.2f}), restoring original capacity")
                
                if self.emr_manager.restore_original_capacity():
                    self.is_downscaled = False
                    self.last_scaling_time = datetime.now()
                    self.logger.info("Original capacity restored successfully")
                else:
                    self.logger.error("Failed to restore original capacity")
        else:
            self.logger.debug("Cluster is in normal state")
            
            # Check if we should scale down
            samples = self.resource_monitor.get_samples()
            if self.decision_maker.should_scale_down(samples):
                self.logger.info("Low utilization detected, calculating target capacity")
                
                current_capacity = self.emr_manager.get_current_capacity()
                target_capacity = self.decision_maker.calculate_target_capacity(
                    current_utilization, current_capacity)
                
                if target_capacity < current_capacity:
                    self.logger.info(f"Scaling down from {current_capacity} to {target_capacity} units")
                    
                    if self.emr_manager.update_max_capacity(target_capacity):
                        self.is_downscaled = True
                        self.last_scaling_time = datetime.now()
                        self.logger.info(f"Capacity updated successfully to {target_capacity} units")
                    else:
                        self.logger.error("Failed to update capacity")
                else:
                    self.logger.info(f"Calculated target capacity ({target_capacity}) is not lower than current capacity ({current_capacity}), not scaling down")
    
    def log_status(self):
        """
        Log the current status of the service.
        """
        samples = self.resource_monitor.get_samples()
        summary = self.decision_maker.get_scaling_summary(samples)
        
        self.logger.info("=== EMR Scaling Service Status ===")
        self.logger.info(f"Cluster ID: {self.config['emr']['cluster_id']}")
        self.logger.info(f"Cluster State: {self.emr_manager.get_cluster_state()}")
        self.logger.info(f"Current Capacity: {self.emr_manager.get_current_capacity()} units")
        self.logger.info(f"Downscaled: {self.is_downscaled}")
        
        if self.last_scaling_time:
            self.logger.info(f"Last Scaling Action: {self.last_scaling_time}")
        
        self.logger.info(f"Sample Count: {summary['sample_count']}")
        self.logger.info(f"Average Utilization: {summary['avg_utilization']:.2f}")
        self.logger.info(f"Weighted Average Utilization: {summary['weighted_avg']:.2f}")
        self.logger.info(f"Samples Below Threshold: {summary['below_threshold_count']}/{summary['sample_count']}")
        self.logger.info(f"Should Scale Down: {summary['should_scale_down']}")
        self.logger.info("===================================")
    
    def stop(self):
        """
        Stop the service.
        """
        self.logger.info("Stopping EMR Scaling Service")
        
        # Restore original capacity if downscaled
        if self.is_downscaled:
            self.logger.info("Restoring original capacity before stopping")
            if self.emr_manager.restore_original_capacity():
                self.is_downscaled = False
                self.logger.info("Original capacity restored successfully")
            else:
                self.logger.error("Failed to restore original capacity")
        
        self.running = False
