"""
This module provides functionality to load and process a YAML configuration
file. The configuration file is expected to contain sections like "input_data"
and "bronze_data" that the application will consume.
"""

import os
import yaml
from src.utils.common_logger import setup_logger

logger = setup_logger(__name__)


class YAMLProcessorError(Exception):
    """Custom exception for YAMLProcessor-related errors."""
    pass


class YAMLProcessor:
    """
    A processor class to load and process a YAML configuration file.
    """

    def __init__(self, config_path):
        """
        Initialize the processor with the path to the YAML config file.

        Parameters:
            config_path (str): Path to the YAML configuration file.

        Raises:
            YAMLProcessorError: If the configuration file does not exist.
        """
        if not os.path.exists(config_path):
            logger.error("Configuration file does not exist: %s", config_path)
            raise YAMLProcessorError("Configuration file does not exist: "
                                     "{}".format(config_path))
        self.config_path = config_path
        self.config = {}

    def load_config(self):
        """
        Load the YAML configuration file and store its content.

        Raises:
            YAMLProcessorError: If an error occurs while reading or parsing the file.
        """
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config = yaml.safe_load(file)
            logger.info("Loaded YAML configuration from: %s", self.config_path)
        except yaml.YAMLError as exc:
            logger.error("Failed to parse YAML file: %s", exc)
            raise YAMLProcessorError("Failed to parse YAML file: {}".format(exc))
        except Exception as exc:
            logger.error("Error loading configuration file: %s", exc)
            raise YAMLProcessorError("Error loading configuration file: {}".format(exc))

    def get_section(self, section_name):
        """
        Retrieve a specified section from the loaded configuration.

        Parameters:
            section_name (str): The name of the section to retrieve.

        Returns:
            dict: The configuration data of the given section.

        Raises:
            YAMLProcessorError: If the configuration has not yet been loaded or the
                                section is missing.
        """
        if not self.config:
            logger.error("Configuration not loaded. Call load_config() first.")
            raise YAMLProcessorError("Configuration not loaded. "
                                     "Call load_config() before accessing sections.")
        section = self.config.get(section_name)
        if section is None:
            logger.error("Section '%s' not found in the configuration.", section_name)
            raise YAMLProcessorError("Section '{}' not found in configuration."
                                     .format(section_name))
        logger.info("Section '%s' retrieved successfully.", section_name)
        return section

    def get_input_data(self):
        """
        Retrieve the 'input_data' section from the configuration.

        Returns:
            dict: The input_data configuration.

        Raises:
            YAMLProcessorError: If the 'input_data' section is missing.
        """
        return self.get_section('input_data')

    def get_bronze_data(self):
        """
        Retrieve the 'bronze_data' section from the configuration.

        Returns:
            dict: The bronze_data configuration.

        Raises:
            YAMLProcessorError: If the 'bronze_data' section is missing.
        """
        return self.get_section('bronze_data')

