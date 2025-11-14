
📌 Project Overview

This project implements an end-to-end Business Intelligence (BI) pipeline using:

Databricks

Alteryx / Python

ER Studio / Navicat

Power BI / Tableau

Two cities’ food inspection datasets (Chicago & Dallas) were processed through a Medallion Architecture (Bronze → Silver → Gold), creating a unified analytical model and reporting output.

🏛️ Architecture: Medallion Model
Bronze Layer

Ingest raw TSV files from Chicago & Dallas.

No transformations applied.

Validated row counts and schemas.

Silver Layer

Clean, standardize, and unify both cities into one common schema.

Convert dates, normalize codes, unify inspection fields.

Produce a single curated table: silver_inspection.

Gold Layer

Build a dimensional warehouse (Star Schema):

dim_restaurant_history (SCD-2)

dim_location

dim_date

dim_violation

fact_inspection

fact_inspection_violation

Implement Slowly Changing Dimensions (Type-2).

Validate referential integrity and model consistency.

🗂️ Project Folder Structure
/notebooks/
    - bronze_ingestion.ipynb
    - silver_transformation.ipynb
    - gold_dim_fact_model.ipynb
    - validation_queries.ipynb

/final_output/
    - dim_restaurant_history.csv
    - dim_location.csv
    - dim_date.csv
    - dim_violation.csv
    - fact_inspection.csv
    - fact_inspection_violation.csv

/documentation/
    - ERD_Diagram.png
    - Mapping_Document.xlsx
    - README.md

🧪 Validation Summary

Key validation checks performed:

✔ Bronze (Raw)

Chicago & Dallas row counts match source.

Schema validated for both cities.

✔ Silver (Unified)

Total unified row count validated.

CHICAGO vs DALLAS distribution validated.

Null checks performed on critical columns.

✔ Gold (Dimensions + Facts)

dim_restaurant_history validated for:

SCD2 versioning

correct is_current flag values

fact_inspection validated for:

row count

foreign key integrity with dim_restaurant_history

fact_inspection_violation validated similarly.

✔ Export

All final Gold-layer dimension and fact tables exported to /final_output as CSV.

🧬 Core Validation Queries Used
Bronze Row Counts
SELECT COUNT(*) FROM bronze_chicago;
SELECT COUNT(*) FROM bronze_dallas;

Silver Row Count
SELECT COUNT(*) FROM silver_inspection;

Silver City Distribution
SELECT city_code, COUNT(*)
FROM silver_inspection
GROUP BY city_code;

SCD2 Version Check
SELECT business_nk, COUNT(*) AS version_count
FROM dim_restaurant_history
GROUP BY business_nk
HAVING COUNT(*) > 1;

Fact FK Validation
SELECT COUNT(*) AS missing_restaurant_keys
FROM fact_inspection f
LEFT JOIN dim_restaurant_history d
ON f.restaurant_key = d.restaurant_key
WHERE d.restaurant_key IS NULL;

🧱 Data Model (ERD Overview)

Star Schema Design:

Fact Tables

fact_inspection

fact_inspection_violation

Dimensions

dim_restaurant_history (SCD-2)

dim_location

dim_date

dim_violation

Relationships:

fact_inspection → restaurant_key → dim_restaurant_history

fact_inspection → location_key → dim_location

fact_inspection → date_key → dim_date

fact_inspection_violation → violation_key → dim_violation

(ERD diagram included separately in the documentation.)

📊 BI Reports

The Gold-layer tables were imported into Power BI / Tableau to generate reports, including:

Total inspections over time

Pass vs Fail distribution

Top violations by frequency

City comparison (Chicago vs Dallas)

Violation trend analysis

🚀 How to Reproduce

Import raw TSV files into Databricks Volumes.

Run bronze_ingestion.ipynb.

Run silver_transformation.ipynb.

Run gold_dim_fact_model.ipynb (includes SCD2 logic).

Run validation_queries.ipynb.

Export results to /final_output/.

Load CSVs into Power BI or Tableau.

🏁 Conclusion

This project successfully demonstrates the full lifecycle of a BI solution:

Data ingestion

Standardization

Dimensional modeling

SCD-2 implementation

ETL validation

BI report generation

The final dataset provides clean, standardized, and analytics-ready insights across two major U.S. cities’ food inspection datasets.
