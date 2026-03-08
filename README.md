# Spark Query Plan Analyzer

A Streamlit-based tool to visualize and analyze Apache Spark query execution plans.

## Features

- Upload CSV datasets
- Run Spark SQL queries
- Inspect logical, optimized, and physical execution plans
- Detect operations like:
  - Shuffle
  - Aggregation
  - Projection
  - File scans

## Tech Stack

- PySpark
- Streamlit
- Python

## Run Locally

pip install -r requirements.txt

streamlit run app.py

## Example Query

SELECT country, SUM(amount)
FROM dataset
GROUP BY country

## Purpose

This project demonstrates understanding of:

- Spark execution plans
- Catalyst optimizer
- Shuffle operations
- Query performance analysis