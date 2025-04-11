#!/usr/bin/env python3
"""
EMR Auto-Scaling Service

This service monitors EMR cluster resource utilization and automatically
triggers scaling operations based on configured thresholds.
"""

import os
import sys
import argparse
import logging

from src.service import EMRScalingService
from src.utils.logging_utils import setup_logging

def parse_args():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='EMR Auto-Scaling Service')
    
    parser.add_argument(
        '-c', '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    parser.add_argument(
        '-l', '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        help='Path to log file (default: logs/emr_scaling.log)'
    )
    
    parser.add_argument(
        '--no-console',
        action='store_true',
        help='Disable console logging'
    )
    
    return parser.parse_args()

def main():
    """
    Main entry point for the service.
    """
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    log_level = getattr(logging, args.log_level)
    log_file = args.log_file
    log_to_console = not args.no_console
    
    if not log_file:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
        log_file = os.path.join(log_dir, 'emr_scaling.log')
    
    logger = setup_logging(log_level=log_level, log_file=log_file)
    
    # Log startup information
    logger.info("Starting EMR Auto-Scaling Service")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Configuration file: {args.config}")
    
    try:
        # Create and run the service
        service = EMRScalingService(args.config)
        service.run()
    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.critical(f"Service failed: {str(e)}", exc_info=True)
        return 1
    
    logger.info("Service shutdown complete")
    return 0

if __name__ == '__main__':
    sys.exit(main())
