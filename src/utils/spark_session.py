from pyspark.sql import SparkSession


def get_spark_session(app_name: str='Medallion ETL') -> SparkSession:
    """
    Create a spark session configured for local development

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    # Create a SparkSession with appropriate configuration

    spark = SparkSession.builder \
                        .appName(app_name) \
                        .master("local[4]") \
                        .config("spark.sql.shuffle.partitions", "4") \
                        .config("spark.default.parallelism", "4") \
                        .config("spark.drive.memory", "2g") \
                        .config("spark.executor.memory", "2g") \
                        .config("spark.sql.warehouse.dir", "spark-warehouse") \
                        .getOrCreate()

    return spark
