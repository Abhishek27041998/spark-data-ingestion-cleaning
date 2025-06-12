"""
This module is responsible for testing the bronze module.
"""

import os
import tempfile
import yaml
import pytest
from unittest.mock import patch, MagicMock
from src.bronze_processor.bronze_module import BronzeModule

@pytest.fixture
def bronze_yaml_config_file():
    config = {
        'input_data': {
            'parent_dir': 'input_dir',
            'movies_file_name': 'movies.csv',
            'ratings_file_name': 'ratings.csv',
            'links_file_name': 'links.csv',
            'tags_file_name': 'tags.csv'
        },
        'bronze_data': {
            'parent_dir': 'bronze_dir',
            'movies_delta_dir': 'movies_delta',
            'ratings_delta_dir': 'ratings_delta',
            'links_delta_dir': 'links_delta',
            'tags_delta_dir': 'tags_delta'
        }
    }
    with tempfile.NamedTemporaryFile(delete=False, suffix='.yaml', mode='w', encoding='utf-8') as tmp:
        yaml.dump(config, tmp)
        temp_filepath = tmp.name
    yield temp_filepath
    os.remove(temp_filepath)

@patch('src.bronze_processor.bronze_module.spark')
def test_process_movies(mock_spark, bronze_yaml_config_file):
    mock_df = MagicMock(name='DataFrame')
    mock_write = MagicMock(name='write')
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.save.return_value = None
    mock_df.write = mock_write
    mock_spark.read.csv.return_value = mock_df
    bronze = BronzeModule(bronze_yaml_config_file)
    bronze.process_movies()
    movies_path = os.path.join('input_dir', 'movies.csv')
    delta_path = os.path.join('bronze_dir', 'movies_delta')
    mock_spark.read.csv.assert_called_once_with(movies_path, header=True, inferSchema=True)
    mock_write.format.assert_called_once_with('parquet')
    mock_write.mode.assert_called_once_with('overwrite')
    mock_write.save.assert_called_once_with(delta_path)

@patch('src.bronze_processor.bronze_module.spark')
def test_process_ratings(mock_spark, bronze_yaml_config_file):
    mock_df = MagicMock(name='DataFrame')
    mock_write = MagicMock(name='write')
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.save.return_value = None
    mock_df.write = mock_write
    mock_spark.read.csv.return_value = mock_df
    bronze = BronzeModule(bronze_yaml_config_file)
    bronze.process_ratings()
    ratings_path = os.path.join('input_dir', 'ratings.csv')
    delta_path = os.path.join('bronze_dir', 'ratings_delta')
    mock_spark.read.csv.assert_called_once_with(ratings_path, header=True, inferSchema=True)
    mock_write.format.assert_called_once_with('parquet')
    mock_write.mode.assert_called_once_with('overwrite')
    mock_write.save.assert_called_once_with(delta_path)

@patch('src.bronze_processor.bronze_module.spark')
def test_process_links(mock_spark, bronze_yaml_config_file):
    mock_df = MagicMock(name='DataFrame')
    mock_write = MagicMock(name='write')
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.save.return_value = None
    mock_df.write = mock_write
    mock_spark.read.csv.return_value = mock_df
    bronze = BronzeModule(bronze_yaml_config_file)
    bronze.process_links()
    links_path = os.path.join('input_dir', 'links.csv')
    delta_path = os.path.join('bronze_dir', 'links_delta')
    mock_spark.read.csv.assert_called_once_with(links_path, header=True, inferSchema=True)
    mock_write.format.assert_called_once_with('parquet')
    mock_write.mode.assert_called_once_with('overwrite')
    mock_write.save.assert_called_once_with(delta_path)

@patch('src.bronze_processor.bronze_module.spark')
def test_process_tags(mock_spark, bronze_yaml_config_file):
    mock_df = MagicMock(name='DataFrame')
    mock_write = MagicMock(name='write')
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.save.return_value = None
    mock_df.write = mock_write
    mock_spark.read.csv.return_value = mock_df
    bronze = BronzeModule(bronze_yaml_config_file)
    bronze.process_tags()
    tags_path = os.path.join('input_dir', 'tags.csv')
    delta_path = os.path.join('bronze_dir', 'tags_delta')
    mock_spark.read.csv.assert_called_once_with(tags_path, header=True, inferSchema=True)
    mock_write.format.assert_called_once_with('parquet')
    mock_write.mode.assert_called_once_with('overwrite')
    mock_write.save.assert_called_once_with(delta_path)

@patch('src.bronze_processor.bronze_module.spark')
def test_process_all(mock_spark, bronze_yaml_config_file):
    mock_df = MagicMock(name='DataFrame')
    mock_write = MagicMock(name='write')
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.save.return_value = None
    mock_df.write = mock_write
    mock_spark.read.csv.return_value = mock_df
    bronze = BronzeModule(bronze_yaml_config_file)
    bronze.process_all()
    assert mock_spark.read.csv.call_count == 4
    assert mock_write.format.call_count == 4
    assert mock_write.mode.call_count == 4
    assert mock_write.save.call_count == 4




