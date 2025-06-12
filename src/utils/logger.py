import logging
import os
from datetime import datetime


def setup_logger(name: str=None, log_to_file: bool = True, level: str = 'INFO'):
    """
    Create and configure logger

    Args:
        name: name of the logger
        log_to_file: whether to save logs to file
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
         A configured logger instance
    """
    # Setup logger name
    logger_name = name or "data_ingestion_cleaning"

    # Create a logger
    logger = logging.getLogger(logger_name)

    # Remove existing handlers
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    # Setup log levels
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    logger.setLevel(log_levels.get(level.upper(), logging.INFO))

    # Configure logging format
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] - %(message)s')

    # Adding Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Create log file if required
    if log_to_file:
        # Create a logs directory if not exists
        log_dir = os.path.join(os.getcwd(), 'logs')
        os.makedirs(log_dir, exist_ok=True)

        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"{logger_name}_{timestamp}.log")

        # Create a file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger