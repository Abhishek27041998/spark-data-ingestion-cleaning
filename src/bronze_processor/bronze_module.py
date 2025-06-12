"""
This module is responsible for processing the raw data and saving it in the bronze layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
from src.utils.yaml_processor import YAMLProcessor
from src.utils.spark_utils import spark


class BronzeModule:
    """
    This class is responsible for processing the raw data and saving it in the bronze layer.
    """
    def __init__(self, config_path):
        processor = YAMLProcessor(config_path)
        processor.load_config()
        self.config = processor.config

    def process_movies(self):
        """
        Process the movies data and save it in the bronze layer.
        """
        movies_path = os.path.join(self.config['input_data']['parent_dir'], self.config['input_data']['movies_file_name'])
        bronze_movies_dir = os.path.join(self.config['bronze_data']['parent_dir'], self.config['bronze_data']['movies_delta_dir'])
        if not os.path.exists(bronze_movies_dir):
            os.makedirs(bronze_movies_dir, exist_ok=True)
        movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
        movies_df.write.format("parquet").mode("overwrite").save(bronze_movies_dir)

    def process_ratings(self):
        """
        Process the ratings data and save it in the bronze layer.
        """
        ratings_path = os.path.join(self.config['input_data']['parent_dir'], self.config['input_data']['ratings_file_name'])
        bronze_ratings_dir = os.path.join(self.config['bronze_data']['parent_dir'], self.config['bronze_data']['ratings_delta_dir'])
        if not os.path.exists(bronze_ratings_dir):
            os.makedirs(bronze_ratings_dir, exist_ok=True)
        ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
        ratings_df.write.format("parquet").mode("overwrite").save(bronze_ratings_dir)

    def process_links(self):        
        """
        Process the links data and save it in the bronze layer.
        """
        links_path = os.path.join(self.config['input_data']['parent_dir'], self.config['input_data']['links_file_name'])
        bronze_links_dir = os.path.join(self.config['bronze_data']['parent_dir'], self.config['bronze_data']['links_delta_dir'])
        if not os.path.exists(bronze_links_dir):
            os.makedirs(bronze_links_dir, exist_ok=True)
        links_df = spark.read.csv(links_path, header=True, inferSchema=True)
        links_df.write.format("parquet").mode("overwrite").save(bronze_links_dir)

    def process_tags(self):
        """
        Process the tags data and save it in the bronze layer.
        """
        tags_path = os.path.join(self.config['input_data']['parent_dir'], self.config['input_data']['tags_file_name'])
        bronze_tags_dir = os.path.join(self.config['bronze_data']['parent_dir'], self.config['bronze_data']['tags_delta_dir'])
        if not os.path.exists(bronze_tags_dir):
            os.makedirs(bronze_tags_dir, exist_ok=True)
        tags_df = spark.read.csv(tags_path, header=True, inferSchema=True)
        tags_df.write.format("parquet").mode("overwrite").save(bronze_tags_dir)

    def process_all(self):
        """ 
        Process all the data and save it in the bronze layer.
        """
        self.process_movies()
        self.process_ratings()
        self.process_links()
        self.process_tags()
