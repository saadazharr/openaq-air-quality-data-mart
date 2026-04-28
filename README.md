# OpenAQ Air Quality Data Mart

A data engineering project designed to ingest, clean, model, and analyze OpenAQ air quality data using Python ETL, SQL data layers, and a scalable AWS-based architecture.

---

## Project Summary

This project builds a data mart for OpenAQ air quality measurements to help business analysts perform pollution analysis across countries and cities.

The solution includes:

- Python-based ETL pipeline
- Raw, staging, and production SQL layers
- Data quality validation
- Business analyst SQL queries
- Architecture and ER diagrams
- Initial Airflow orchestration structure

---

## Repository Structure

```text
openaq-air-quality-data-mart/
│
├── README.md
├── .gitignore
│
├── Docs/
│   ├── Architecture.jpg
│   └── Database ER diagram (crow's foot) (2).jpeg
│
├── ELT Python Code/
│   └── Python scripts for data extraction, loading, and transformation
│
├── Sql Scripts/
│   ├── Business Analyst Queries.sql
│   └── Data Engineering Project (Production Layer ETL).sql
│
└── Airflow Work/
    └── Airflow Work/
        └── Initial Airflow orchestration files
