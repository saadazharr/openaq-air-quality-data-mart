# OpenAQ Air Quality Data Mart

## Project Overview

This project is a data engineering assignment based on the OpenAQ air quality dataset. The main goal is to design and build a data mart that allows business analysts to analyze historical air quality measurements efficiently.

The solution includes Python ETL scripts, SQL scripts, data quality handling, database layers, architecture documentation, and an initial Airflow orchestration structure.

## Dataset

Dataset cannot be shared as it was not allowed. 

## Folder Structure

- SQL
- ETL
- Airflow
- SQLScripts
- README.md

## Folder Details

### SQL

This folder contains the project architecture, ETL diagram, database schema, and full documentation explaining how the project was designed and implemented.

### ETL

This folder contains the Python code used to download the OpenAQ data, process the file, clean the data, and load it into the database.

### Airflow

This folder contains the initial Airflow files for pipeline orchestration. This part was started but could not be fully completed due to time constraints.

### SQLScripts

This folder contains SQL scripts for creating the raw, staging, and production layers. It also includes analytical queries for business analysts to verify and analyze the data.

## Data Architecture

The project follows a layered data architecture.

### Raw Layer

The raw layer stores the original source data with minimal transformation. This helps preserve the original dataset and supports auditability.

### Staging Layer

The staging layer cleans, validates, and transforms the raw data before loading it into the production layer.

### Production Layer

The production layer stores final cleaned and analysis-ready data for reporting and business analysis.

## Analytical Requirements

The data mart supports the following analysis:

- Monthly CO and SO2 pollution analysis at the 90th percentile globally.
- Top 5 cities with the highest daily average PM2.5 pollution.
- Top 10 cities with the highest PM2.5 levels for a given hour.
- Mean, median, and mode calculations for CO and SO2.
- Country-level air quality index with low, moderate, and high categories.
- Data quality checks and bad data reporting.

## Technologies Used

- Python
- SQL
- ETL Processing
- Database/Data Mart Design
- Apache Airflow
- AWS Architecture Design
- OpenAQ Dataset

## Data Quality

The solution includes validation rules to separate valid and invalid records. Invalid records are excluded from the production layer and can be reviewed separately through data quality reports.

## Scalability

Although the prototype uses one OpenAQ file, the architecture is designed to scale for larger datasets. It can be extended to support multiple historical files, future data ingestion, cloud storage, and automated orchestration.

## Current Status

Completed:

- Project documentation
- Architecture design
- Python ETL scripts
- SQL scripts
- Raw, staging, and production layer design
- Business analyst queries
- Data quality handling approach

Partially completed:

- Airflow orchestration

The Airflow implementation was started but not fully completed due to time constraints. The initial structure is included for future extension.

## Author

Saad Azhar
