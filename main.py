# This is the main function for running data ingestion and data cleaning
from src.utils.logger import setup_logger
from src.utils.config_loader import load_config
from src.utils.spark_session import get_spark_session


def main():
    logger = setup_logger(name='data_ingestion_cleaning')
    logger.info("Starting Ingestion of Movies Dataset")

    config = load_config()
    logger.info(f"Read config file: {config}")

    spark = get_spark_session("Medallion ETL")
    logger.info("Initialized a Spark session")


if __name__ == '__main__':
    main()
