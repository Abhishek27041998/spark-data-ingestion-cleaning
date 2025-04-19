# This is the main function for running data ingestion and data cleaning
from src.utils.logger import setup_logger


def main():
    logger = setup_logger(name='data_ingestion_cleaning')

    logger.info("Starting Ingestion of Movies Dataset")


if __name__ == '__main__':
    main()
