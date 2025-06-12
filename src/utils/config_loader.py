import yaml
import os


def load_config(config_path: str = "config/config.yaml"):
    """
    Load the YAML configuration file

    Args:
        config_path: Path to the config file

    Returns:
        dict: Configuration as a dictionary
    """
    # Check if the file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # Try to read config file
    try:
        with open(config_path, 'r') as config_file:
            return yaml.safe_load(config_file)
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML Config file: {e}")
