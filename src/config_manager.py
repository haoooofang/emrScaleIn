import yaml
import logging
import os

logger = logging.getLogger('emr_scaling')

class ConfigurationManager:
    """
    Manages loading and validating configuration from YAML files.
    """
    
    def __init__(self, config_path):
        """
        Initialize the ConfigurationManager.
        
        Args:
            config_path: Path to the configuration file
        """
        self.config_path = config_path
        self.config = None
        self.load_config()
        
    def load_config(self):
        """
        Load configuration from YAML file.
        
        Raises:
            FileNotFoundError: If the configuration file is not found
            yaml.YAMLError: If the configuration file is not valid YAML
        """
        logger.info(f"Loading configuration from {self.config_path}")
        
        if not os.path.exists(self.config_path):
            logger.error(f"Configuration file not found: {self.config_path}")
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            
            logger.info("Configuration loaded successfully")
            self.validate_config()
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {str(e)}")
            raise
        
    def validate_config(self):
        """
        Validate the configuration.
        
        Raises:
            ValueError: If the configuration is invalid
        """
        # Required configuration sections and parameters
        required_sections = ['emr', 'monitoring', 'thresholds', 'weights']
        required_params = {
            'emr': ['cluster_id', 'region'],
            'monitoring': ['sampling_interval', 'history_periods', 'threshold_periods'],
            'thresholds': ['low_utilization', 'high_utilization', 'target_utilization'],
            'weights': ['decay_factor']
        }
        
        # Check if all required sections exist
        for section in required_sections:
            if section not in self.config:
                logger.error(f"Missing required configuration section: {section}")
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Check if all required parameters exist in each section
        for section, params in required_params.items():
            for param in params:
                if param not in self.config[section]:
                    logger.error(f"Missing required configuration parameter: {section}.{param}")
                    raise ValueError(f"Missing required configuration parameter: {section}.{param}")
        
        # Validate specific parameter values
        self._validate_numeric_range('monitoring.sampling_interval', 60, 3600)
        self._validate_numeric_range('monitoring.history_periods', 2, 100)
        self._validate_numeric_range('monitoring.threshold_periods', 1, 
                                    self.config['monitoring']['history_periods'])
        self._validate_numeric_range('thresholds.low_utilization', 0.0, 1.0)
        self._validate_numeric_range('thresholds.high_utilization', 
                                    self.config['thresholds']['low_utilization'], 1.0)
        self._validate_numeric_range('thresholds.target_utilization', 
                                    self.config['thresholds']['low_utilization'], 
                                    self.config['thresholds']['high_utilization'])
        self._validate_numeric_range('weights.decay_factor', 0.0, 1.0)
        
        # Set default values for optional parameters
        if 'min_capacity' not in self.config['monitoring']:
            self.config['monitoring']['min_capacity'] = 1
            logger.info("Using default value for monitoring.min_capacity: 1")
        
        logger.info("Configuration validation successful")
    
    def _validate_numeric_range(self, param_path, min_val, max_val):
        """
        Validate that a numeric parameter is within the specified range.
        
        Args:
            param_path: Parameter path in dot notation (e.g., 'monitoring.sampling_interval')
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Raises:
            ValueError: If the parameter is not within the specified range
        """
        section, param = param_path.split('.')
        value = self.config[section][param]
        
        if not isinstance(value, (int, float)):
            logger.error(f"Parameter {param_path} must be numeric, got {type(value)}")
            raise ValueError(f"Parameter {param_path} must be numeric, got {type(value)}")
        
        if value < min_val or value > max_val:
            logger.error(f"Parameter {param_path} must be between {min_val} and {max_val}, got {value}")
            raise ValueError(f"Parameter {param_path} must be between {min_val} and {max_val}, got {value}")
    
    def get_config(self):
        """
        Get the configuration.
        
        Returns:
            dict: The configuration
        """
        return self.config
    
    def get_value(self, param_path, default=None):
        """
        Get a configuration value by path.
        
        Args:
            param_path: Parameter path in dot notation (e.g., 'monitoring.sampling_interval')
            default: Default value to return if the parameter is not found
            
        Returns:
            The configuration value, or the default value if not found
        """
        try:
            parts = param_path.split('.')
            value = self.config
            for part in parts:
                value = value[part]
            return value
        except (KeyError, TypeError):
            logger.warning(f"Configuration parameter not found: {param_path}, using default: {default}")
            return default
