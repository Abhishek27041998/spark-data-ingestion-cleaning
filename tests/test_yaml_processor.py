"""
Unit tests for yaml_processor.py using pytest.
"""

import os
import tempfile
import yaml
import pytest
from src.utils.yaml_processor import YAMLProcessor, YAMLProcessorError


@pytest.fixture
def yaml_config_file():
    """
    Create a temporary YAML configuration file for testing.
    """
    config = {
        'input_data': {
            'parent_dir': "data/raw/ml-latest-small",
            'movies_file_name': "movies.csv",
            'ratings_file_name': "ratings.csv",
            'links_file_name': "links.csv",
            'tags_file_name': "movies.csv"
        },
        'bronze_data': {
            'parent_dir': "data/bronze",
            'movies_delta_dir': "movies",
            'ratings_delta_dir': "ratings",
            'links_delta_dir': "links",
            'tags_dir': "tags"
        }
    }
    with tempfile.NamedTemporaryFile(delete=False, suffix='.yaml', mode='w', encoding='utf-8') as tmp:
        yaml.dump(config, tmp)
        temp_filepath = tmp.name
    yield temp_filepath
    os.remove(temp_filepath)


class TestYAMLProcessor:
    """
    Test cases for the YAMLProcessor class.
    """

    def test_load_config_success(self, yaml_config_file):
        """
        Test that loading a valid config file succeeds.
        """
        processor = YAMLProcessor(yaml_config_file)
        processor.load_config()
        assert isinstance(processor.config, dict)
        assert 'input_data' in processor.config
        assert 'bronze_data' in processor.config

    def test_get_section_success(self, yaml_config_file):
        """
        Test that retrieving an existing section returns a dictionary.
        """
        processor = YAMLProcessor(yaml_config_file)
        processor.load_config()
        input_data = processor.get_section('input_data')
        assert isinstance(input_data, dict)
        bronze_data = processor.get_section('bronze_data')
        assert isinstance(bronze_data, dict)

    def test_get_section_failure(self, yaml_config_file):
        """
        Test that trying to retrieve a missing section raises an error.
        """
        processor = YAMLProcessor(yaml_config_file)
        processor.load_config()
        with pytest.raises(YAMLProcessorError):
            processor.get_section('non_existing_section')

    def test_get_input_data(self, yaml_config_file):
        """
        Test that the get_input_data() method returns correct configuration.
        """
        processor = YAMLProcessor(yaml_config_file)
        processor.load_config()
        input_data = processor.get_input_data()
        assert input_data.get('parent_dir') == "data/raw/ml-latest-small"

    def test_get_bronze_data(self, yaml_config_file):
        """
        Test that the get_bronze_data() method returns correct configuration.
        """
        processor = YAMLProcessor(yaml_config_file)
        processor.load_config()
        bronze_data = processor.get_bronze_data()
        assert bronze_data.get('parent_dir') == "data/bronze"

    def test_missing_config_file(self):
        """
        Test that providing a non-existent file raises an error.
        """
        with pytest.raises(YAMLProcessorError):
            YAMLProcessor("non_existent_file.yaml")
