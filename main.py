# This is the main function for running data ingestion and data cleaning

import sys
import os
from src.bronze_processor.bronze_module import BronzeModule
from src.utils.common_logger import setup_logger


def main():
    logger = setup_logger(__name__)
    config_path = os.path.join('config', 'config.yaml')
    logger.info(f"Starting data ingestion and bronze processing with config: {config_path}")
    try:
        bronze = BronzeModule(config_path)
        bronze.process_all()
        logger.info("Bronze layer processing completed successfully.")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

