# spark-data-ingestion-cleaning
This repo demonstrates basic functionalities of Spark (PySpark) to perform data ingestion and cleaning of a public dataset

## Overview
This project implements a data pipeline using PySpark to ingest and process movie-related data from public datasets. The pipeline includes data cleaning, transformation, and storage in a structured format.

## Features
- Data ingestion from multiple sources (movies, ratings, links, tags)
- Data cleaning and transformation using PySpark
- Delta Lake format for efficient data storage
- Modular architecture with separate bronze layer processing

## Project Structure
```
.
├── config/             # Configuration files
├── data/              # Data storage directory
├── logs/              # Application logs
├── notebooks/         # Jupyter notebooks for analysis
├── src/               # Source code
│   ├── bronze_processor/  # Bronze layer data processing
│   └── utils/            # Utility functions
├── tests/             # Test files
├── main.py            # Main application entry point
└── requirements.txt   # Project dependencies
```

## Prerequisites
- Python 3.8+
- Apache Spark
- Virtual environment (recommended)

## Installation
1. Clone the repository:
```bash
git clone https://github.com/yourusername/spark-data-ingestion-cleaning.git
cd spark-data-ingestion-cleaning
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage
1. Configure your data sources in the `config/` directory
2. Run the main pipeline:
```bash
python main.py
```

## Development
- Source code is organized in the `src/` directory
- Bronze layer processing handles initial data ingestion and cleaning
- Utility functions are available in `src/utils/`
- Tests can be found in the `tests/` directory

## License
This project is licensed under the terms of the license included in the repository.
