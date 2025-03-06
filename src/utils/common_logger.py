"""
This module provides a function to obtain a pre-configured logger.
It sets up a rotating file handler and a stream handler for console logging.
It also ensures that the directory for the log file exists.
"""

import logging
import logging.handlers
import os

def setup_logger(name, log_file='logs/app.log', level=logging.INFO,
                 max_bytes=10 * 1024 * 1024, backup_count=5):
    """
    Set up and return a logger with the provided configuration.

    Parameters:
        name (str): Name for the logger (typically __name__).
        log_file (str): File path to write the log.
        level (int): Logging level threshold.
        max_bytes (int): Maximum size in bytes for the log file before rotation.
        backup_count (int): Number of rotated log files to keep.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        # Ensure log file directory exists before creating the file handler
        log_directory = os.path.dirname(log_file)
        if log_directory and not os.path.exists(log_directory):
            os.makedirs(log_directory, exist_ok=True)  # Creates directory if it doesn't exist [5]

        # Create a rotating file handler for persistent logging
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(level)

        # Create a stream handler to output logs to the console
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Define a common formatter for both handlers
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
