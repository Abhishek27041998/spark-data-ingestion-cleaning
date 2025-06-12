"""
This module contains utility functions for the Spark application.
"""

from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create a Spark session.
    """
    return SparkSession.builder.appName("SparkApplication").getOrCreate()


spark = create_spark_session()
