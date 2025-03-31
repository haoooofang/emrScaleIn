#!/usr/bin/env python3
"""
Test script for EMR Auto-Scaling System

This script simulates resource utilization patterns and tests the scaling decision logic
without actually modifying an EMR cluster.
"""

import argparse
import logging
import os
import sys
import time
import yaml
from datetime import datetime, timedelta

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.scaling_decision import ScalingDecisionMaker
from src.utils.logging_utils import setup_logging

def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Test EMR Auto-Scaling System')
    
    parser.add_argument(
        '-c', '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '-p', '--pattern',
        choices=['decreasing', 'increasing', 'fluctuating', 'stable_low', 'stable_high'],
        default='decreasing',
        help='Utilization pattern to simulate (default: decreasing)'
    )
    
    parser.add_argument(
        '-s', '--samples',
        type=int,
        default=15,
        help='Number of samples to generate (default: 15)'
    )
    
    parser.add_argument(
        '--initial-utilization',
        type=float,
        default=0.7,
        help='Initial utilization value (default: 0.7)'
    )
    
    parser.add_argument(
        '--final-utilization',
        type=float,
        default=0.2,
        help='Final utilization value for decreasing/increasing patterns (default: 0.2)'
    )
    
    parser.add_argument(
        '--capacity',
        type=int,
        default=20,
        help='Current cluster capacity units (default: 20)'
    )
    
    return parser.parse_args()

def generate_samples(pattern, num_samples, initial_util, final_util):
    """
    Generate sample utilization data based on the specified pattern.
    
    Args:
        pattern: Utilization pattern ('decreasing', 'increasing', 'fluctuating', 'stable_low', 'stable_high')
        num_samples: Number of samples to generate
        initial_util: Initial utilization value
        final_util: Final utilization value for decreasing/increasing patterns
        
    Returns:
        list: List of (timestamp, utilization) tuples
    """
    samples = []
    now = datetime.now()
    
    if pattern == 'decreasing':
        # Linearly decreasing utilization
        for i in range(num_samples):
            timestamp = now - timedelta(minutes=(num_samples - i) * 5)
            utilization = initial_util - (i / (num_samples - 1)) * (initial_util - final_util)
            samples.append((timestamp, utilization))
    
    elif pattern == 'increasing':
        # Linearly increasing utilization
        for i in range(num_samples):
            timestamp = now - timedelta(minutes=(num_samples - i) * 5)
            utilization = final_util + (i / (num_samples - 1)) * (initial_util - final_util)
            samples.append((timestamp, utilization))
    
    elif pattern == 'fluctuating':
        # Fluctuating utilization around a mean
        import math
        mean_util = (initial_util + final_util) / 2
        amplitude = (initial_util - final_util) / 2
        
        for i in range(num_samples):
            timestamp = now - timedelta(minutes=(num_samples - i) * 5)
            # Sine wave fluctuation
            utilization = mean_util + amplitude * math.sin(i * math.pi / 4)
            samples.append((timestamp, utilization))
    
    elif pattern == 'stable_low':
        # Stable low utilization
        for i in range(num_samples):
            timestamp = now - timedelta(minutes=(num_samples - i) * 5)
            # Add small random variation
            utilization = final_util + (0.05 * (i % 3 - 1))
            samples.append((timestamp, max(0, min(1, utilization))))
    
    elif pattern == 'stable_high':
        # Stable high utilization
        for i in range(num_samples):
            timestamp = now - timedelta(minutes=(num_samples - i) * 5)
            # Add small random variation
            utilization = initial_util + (0.05 * (i % 3 - 1))
            samples.append((timestamp, max(0, min(1, utilization))))
    
    return samples

def print_samples(samples):
    """
    Print the generated samples in a readable format.
    
    Args:
        samples: List of (timestamp, utilization) tuples
    """
    print("\nGenerated Utilization Samples:")
    print("------------------------------")
    print("Timestamp               | Utilization")
    print("------------------------|------------")
    
    for timestamp, utilization in samples:
        print(f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} | {utilization:.2f}")

def main():
    """
    Main entry point for the test script.
    """
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    logger = setup_logging(log_level=logging.INFO)
    
    # Load configuration
    try:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        return 1
    
    # Generate sample data
    samples = generate_samples(
        args.pattern,
        args.samples,
        args.initial_utilization,
        args.final_utilization
    )
    
    # Print the samples
    print_samples(samples)
    
    # Create scaling decision maker
    decision_maker = ScalingDecisionMaker(config)
    
    # Get scaling decision
    should_scale = decision_maker.should_scale_down(samples)
    
    # Calculate target capacity if scaling down
    current_utilization = samples[-1][1]  # Most recent utilization
    target_capacity = decision_maker.calculate_target_capacity(
        current_utilization, args.capacity)
    
    # Get scaling summary
    summary = decision_maker.get_scaling_summary(samples)
    
    # Print results
    print("\nScaling Decision Results:")
    print("------------------------")
    print(f"Pattern: {args.pattern}")
    print(f"Current Capacity: {args.capacity} units")
    print(f"Current Utilization: {current_utilization:.2f}")
    print(f"Average Utilization: {summary['avg_utilization']:.2f}")
    print(f"Weighted Average: {summary['weighted_avg']:.2f}")
    print(f"Samples Below Threshold: {summary['below_threshold_count']}/{summary['sample_count']}")
    print(f"Should Scale Down: {should_scale}")
    
    if should_scale:
        print(f"Target Capacity: {target_capacity} units")
        print(f"Capacity Reduction: {args.capacity - target_capacity} units ({(args.capacity - target_capacity) / args.capacity * 100:.1f}%)")
    
    return 0

if __name__ == '__main__':
    sys.exit(main())
