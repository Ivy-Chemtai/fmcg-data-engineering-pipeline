# fmcg-data-engineering-pipeline
## Project Overview

This project showcases the design and implementation of an end-to-end data pipeline using the Medallion Architecture (Bronze → Silver → Gold) in a modern data engineering environment. Built on Databricks with AWS S3 and Delta Lake, the pipeline transforms raw FMCG sales data into scalable, analytics-ready datasets.

## Executive Summary

The solution ingests raw CSV data from AWS S3 into a multi-layered Delta Lake architecture. It applies systematic data cleaning, transformation, and modeling to produce reliable fact and dimension tables for business intelligence.

Key highlights:

End-to-end pipeline from ingestion to reporting
Incremental data processing using Delta Lake MERGE
Data quality handling and transformation at scale
Optimized data model for analytics and dashboards

## Problem & Context

FMCG organizations generate large volumes of raw transactional data that often:

Contain inconsistent formats (e.g., multiple date formats)
Have missing or null values
Include duplicate or unvalidated records
Lack structured relationships for analytics

This project addresses these challenges by:

Standardizing and cleaning raw data
Structuring it into meaningful entities (dimensions & facts)
Enabling efficient incremental processing
Delivering reliable, business-ready datasets

## Tech Stack
**Databricks**– Data processing and orchestration
**PySpark** – Data transformation and ETL logic
**Spark SQL** – Data modeling and querying
**Delta Lake** – Storage layer with ACID transactions and MERGE support
**AWS S3**– Data lake storage for raw and processed data

## Analysis Overview

The pipeline follows a structured transformation approach:

### Bronze Layer
Raw ingestion of CSV files from AWS S3
Minimal transformations to preserve original data
### Silver Layer
Data cleaning and validation using PySpark
Null handling using COALESCE
Date standardization using try_to_date
Duplicate removal and schema enforcement
Hash key generation (SHA-256) for entity resolution
### Gold Layer
Business-ready data modeling using Spark SQL
Creation of fact and dimension tables:
dim_products
dim_customers
dim_gross_price
fact_orders
Optimized for reporting and analytics

## Databricks Demonstrated Skills
Building scalable **Medallion Architecture pipelines**
Implementing Delta Lake **MERGE (upsert)** for incremental loads
Enabling Change Data Feed (CDF) for efficient data processing
Writing efficient** PySpark** transformations
Designing fact and dimension data models
Handling data quality issues (nulls, formats, duplicates)
**Orchestrating workflow**s using Databricks Workflows
Managing reusable configurations via utility notebooks




