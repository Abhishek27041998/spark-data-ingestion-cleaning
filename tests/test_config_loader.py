import os
import tempfile
from unittest.mock import patch, mock_open

import yaml
import pytest


# Import config loader
from src.utils.config_loader import load_config


@pytest.fixture
def test_config_file():
    """Create a temporary config file for testing"""
    test_config = {
        "paths": {
            "raw": {
                "movies_csv": "data/raw/ml-latest-small/movies.csv"
            },
            "bronze": {
                "movies": "data/bronze/movies"
            }
        }
    }

    # Create a temporary file
    temp_dir = tempfile.TemporaryDirectory()
    config_path = os.path.join(temp_dir.name, "test_config.yaml")

    with open(config_path, 'w') as f:
        yaml.dump(test_config, f)

    yield config_path, test_config

    temp_dir.cleanup()


def test_load_existing_config(test_config_file):
    """Test loading of valid config file"""
    config_path, expected_config = test_config_file

    config = load_config(config_path)

    assert config['paths']['raw']['movies_csv'] == "data/raw/ml-latest-small/movies.csv"
    assert config['paths']['bronze']['movies'] == "data/bronze/movies"


def test_file_not_found():
    """Test handling of non-existent file"""
    non_existent_path = "file_not_exists.yaml"

    with pytest.raises(FileNotFoundError):
        load_config(non_existent_path)


def test_invalid_yaml():
    """Test handling of invalid YAML"""
    invalid_yaml = "invalid : yaml : : content"

    # Mock open to return invalid YAML content
    with patch("builtins.open", mock_open(read_data=invalid_yaml)):
        with patch("os.path.exists", return_value=True):
            with pytest.raises(ValueError):
                load_config("fake_path.yaml")

