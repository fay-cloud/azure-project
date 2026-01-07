
# Retail Lakehouse Data Engineering Project

## Project Overview

This project demonstrates an end-to-end **modern data engineering pipeline** using Azure and Databricks, following **industry best practices** such as the Medallion Architecture (Bronze, Silver, Gold).  
The goal is to ingest data from **multiple heterogeneous sources**, transform and enrich it, and prepare analytics-ready datasets for downstream analysis using Databricks and BI tools.

This project is intentionally designed to mirror **real-world enterprise data platforms** rather than toy examples.

---

## Business Context

The dataset represents a simple retail business with:
- Customers placing orders
- Orders containing products
- Products sold across multiple store locations

The pipeline enables analysis such as:
- Sales performance by store and country
- Product-level revenue and quantity trends
- Daily transaction metrics
- Customer purchasing behavior
<img width="1024" height="572" alt="image" src="infographic image of the project.png

---

## Source Systems

The project integrates data from **multiple sources**, simulating real production environments.

### 1. Azure SQL Database
Used as the transactional system of record.

Tables:
- `Orders`
- `OrderItems`
- `Products`
- `Stores`

Characteristics:
- Normalized relational schema
- Enforced primary and foreign key constraints
- Represents operational OLTP data

### 2. GitHub (CSV Source)
Used as an external data source to simulate a third-party or CRM system.

Table:
- `Customers` (CSV)

Rationale:
- Demonstrates ingestion from a non-database source
- Highlights schema alignment challenges in real pipelines

---

## Data Lake Architecture (ADLS Gen2)

Data is stored in Azure Data Lake Storage Gen2 using the **Medallion Architecture**.

```
/bronze   -> Raw ingested data (no transformations)
/silver   -> Cleaned, typed, and joined datasets
/gold     -> Aggregated, analytics-ready datasets
```

### Bronze Layer
- Raw copies of source data
- No schema enforcement beyond file format
- Sources include:
  - Parquet files from Azure SQL tables
  - CSV file from GitHub (Customers)

### Silver Layer
- Data cleaning and standardization
- Type casting and column renaming
- Removal of duplicates
- Business logic applied:
  - Calculated total transaction amount
  - Enforced consistent keys for joins

Output:
- Delta Lake fact table combining transactions, products, stores, and customers

### Gold Layer
- Aggregated datasets for analytics
- Optimized for reporting and analysis
- Metrics include:
  - Total quantity sold
  - Total sales amount
  - Number of transactions
  - Average transaction value

---

## Databricks Processing

Apache Spark on Databricks is used for transformation and analytics.

Key aspects:
- PySpark DataFrame API
- Delta Lake for ACID-compliant storage
- Left joins used to preserve data completeness
- Clear separation between transformation layers

Databricks handles:
- Reading from ADLS Gen2
- Data cleansing and enrichment
- Writing Silver and Gold Delta tables
- Registering tables for SQL-based analytics

---

## Security and Best Practices

- No secrets are hardcoded in notebooks or scripts
- Azure Storage access is designed to use Databricks Secret Scopes
- Delta Lake ensures data reliability and consistency
- Schema evolution handled safely without data loss
- Foreign key integrity preserved at source level

---

## Technologies Used

- Azure SQL Database
- Azure Data Lake Storage Gen2
- Azure Blob Storage
- Databricks (Apache Spark)
- Delta Lake
- PySpark
- GitHub (CSV source control)
- SQL
- Python

---

## Why This Project Matters

This project demonstrates the ability to:
- Design scalable data architectures
- Integrate multiple data sources
- Apply data modeling best practices
- Build production-style data pipelines
- Prepare datasets for analytics and machine learning
- Communicate data engineering decisions clearly

The solution is intentionally realistic and aligns with how modern data teams operate in cloud environments.

---

## Potential Extensions

- Incremental loading using watermarking
- Partitioning Delta tables by date
- Slowly Changing Dimensions (SCD)
- Power BI dashboards on Gold layer
- Databricks ML for demand forecasting
- CI/CD for data pipelines

---

## Author

This project was designed and implemented as part of a professional data engineering portfolio to demonstrate practical, real-world data engineering skills.
