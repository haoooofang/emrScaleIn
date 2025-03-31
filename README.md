# EMR Auto-Scaling System

A daemon service that monitors EMR cluster resource utilization and automatically triggers scaling operations based on configured thresholds.

## Overview

The EMR Auto-Scaling System observes YARN memory utilization metrics, performs downscaling when resources are consistently underutilized, and restores capacity when needed. It uses a weighted algorithm to evaluate resource utilization over time, giving more importance to recent metrics while still considering the overall trend.

## Features

- Periodically collects YARN memory utilization metrics from the EMR cluster
- Tracks and stores metrics for the past n sampling periods
- Evaluates resource utilization using a weighted algorithm (recent periods have higher weights)
- Automatically downscales when resources are consistently underutilized
- Calculates optimal target capacity to achieve utilization just above the threshold after downscaling
- Restores original capacity when resource utilization exceeds the upper threshold
- Configurable thresholds, sampling periods, and weights

## Architecture

The system consists of the following components:

- **ConfigurationManager**: Loads and validates configuration from YAML files
- **EMRClusterManager**: Interfaces with AWS EMR API to get/set scaling configurations
- **ResourceMonitor**: Collects and tracks YARN memory utilization metrics
- **ScalingDecisionMaker**: Implements the weighted algorithm for scaling decisions
- **EMRScalingService**: Orchestrates the monitoring and scaling operations

## Requirements

- Python 3.6+
- boto3
- pyyaml
- AWS credentials with permissions to:
  - Describe EMR clusters
  - Get CloudWatch metrics
  - Update EMR managed scaling policies

## Installation

### Using the Installation Script

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/emr-scaling.git
   cd emr-scaling
   ```

2. Update the configuration file (`config.yaml`) with your EMR cluster ID and region.

3. Run the installation script as root:
   ```
   sudo ./install.sh
   ```

4. Start the service:
   ```
   sudo systemctl start emr-scaling
   ```

5. Enable the service to start on boot:
   ```
   sudo systemctl enable emr-scaling
   ```

### Manual Installation

1. Install dependencies:
   ```
   pip3 install boto3 pyyaml
   ```

2. Create directories:
   ```
   sudo mkdir -p /opt/emr-scaling
   sudo mkdir -p /etc/emr-scaling
   sudo mkdir -p /opt/emr-scaling/logs
   ```

3. Copy files:
   ```
   sudo cp -r ./src /opt/emr-scaling/
   sudo cp ./main.py /opt/emr-scaling/
   sudo cp ./requirements.txt /opt/emr-scaling/
   sudo cp ./config.yaml /etc/emr-scaling/
   ```

4. Set permissions:
   ```
   sudo chmod +x /opt/emr-scaling/main.py
   sudo chown -R ec2-user:ec2-user /opt/emr-scaling
   sudo chown -R ec2-user:ec2-user /etc/emr-scaling
   ```

5. Install systemd service:
   ```
   sudo cp ./emr-scaling.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl start emr-scaling
   sudo systemctl enable emr-scaling
   ```

## Configuration

The system is configured through a YAML file (`config.yaml`). Here's an example configuration:

```yaml
emr:
  cluster_id: "j-XXXXXXXXXXXX"  # EMR cluster ID
  region: "us-east-1"           # AWS region

monitoring:
  sampling_interval: 300        # Sampling interval in seconds
  history_periods: 12           # Number of historical sampling periods to retain (n)
  threshold_periods: 8          # Number of periods required to trigger downscaling (m)
  min_capacity: 1               # Minimum capacity units
  
thresholds:
  low_utilization: 0.4          # Lower threshold, consider downscaling below this value
  high_utilization: 0.7         # Upper threshold, restore capacity above this value
  target_utilization: 0.5       # Target utilization after downscaling

weights:
  decay_factor: 0.9             # Weight decay factor for historical samples
```

### Configuration Parameters

- **emr.cluster_id**: The ID of the EMR cluster to monitor and scale
- **emr.region**: The AWS region where the EMR cluster is located
- **monitoring.sampling_interval**: How often to collect metrics (in seconds)
- **monitoring.history_periods**: Number of historical sampling periods to retain
- **monitoring.threshold_periods**: Number of periods required to trigger downscaling
- **monitoring.min_capacity**: Minimum capacity units to maintain
- **thresholds.low_utilization**: Lower threshold, consider downscaling below this value
- **thresholds.high_utilization**: Upper threshold, restore capacity above this value
- **thresholds.target_utilization**: Target utilization after downscaling
- **weights.decay_factor**: Weight decay factor for historical samples (0-1)

## Usage

### Starting the Service

```
sudo systemctl start emr-scaling
```

### Checking Service Status

```
sudo systemctl status emr-scaling
```

### Viewing Logs

```
sudo journalctl -u emr-scaling
```

### Reloading Configuration

To reload the configuration without restarting the service:

```
sudo systemctl reload emr-scaling
```

### Running Manually

You can also run the service manually for testing:

```
python3 main.py --config config.yaml --log-level DEBUG
```

## Weighted Algorithm

The system uses a weighted algorithm to evaluate resource utilization over time. The algorithm gives more importance to recent utilization metrics while still considering the overall trend. This prevents premature scaling decisions based on temporary fluctuations.

The weighted average utilization is calculated using an exponential decay function:

```python
def calculate_weighted_average(samples):
    if not samples:
        return 0.0
        
    total_weight = 0.0
    weighted_sum = 0.0
    decay_factor = config['weights']['decay_factor']
    
    # Apply exponential decay weights (most recent has highest weight)
    for i, utilization in enumerate(reversed(utilization_values)):
        weight = decay_factor ** i  # Most recent sample has weight = 1
        weighted_sum += utilization * weight
        total_weight += weight
        
    return weighted_sum / total_weight
```

The decision to scale down is based on a weighted count of periods below the threshold:

```python
def should_scale_down(samples):
    # Calculate weighted count of periods below threshold
    weighted_count = 0.0
    
    for i, utilization in enumerate(reversed(utilization_values)):
        if utilization < low_threshold:
            weighted_count += decay_factor ** i
    
    # Calculate threshold weight sum
    threshold_weight_sum = sum(decay_factor ** i for i in range(threshold_periods))
    
    # Determine if enough periods are below threshold (using 80% of possible weight)
    required_weight = threshold_weight_sum * 0.8
    
    return weighted_count >= required_weight
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
