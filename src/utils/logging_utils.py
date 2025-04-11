import logging
import os
from logging.handlers import RotatingFileHandler
import sys

def setup_logging(log_level=logging.INFO, log_file=None):
    logger = logging.getLogger('emr_scaling')

    # Clear any existing handlers to prevent duplication
    logger.handlers.clear()

    # Prevent propagation to root logger
    logger.propagate = False

    # Set log level
    logger.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add file handler if log_file is specified
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Add stream handler for console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
