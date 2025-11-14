# Food Inspection BI Pipeline

## Project Overview

This project implements an end-to-end Business Intelligence pipeline processing food inspection datasets from Chicago and Dallas through a Medallion Architecture (Bronze → Silver → Gold), creating a unified dimensional model for reporting.

**Technology Stack:**
- Databricks
- Alteryx / Python
- ER Studio / Navicat
- Power BI / Tableau

## Core Validation Queries

**Bronze Row Counts:**
```sql
SELECT COUNT(*) FROM bronze_chicago;
SELECT COUNT(*) FROM bronze_dallas;
```

**Silver Row Count:**
```sql
SELECT COUNT(*) FROM silver_inspection;
```

**Silver City Distribution:**
```sql
SELECT city_code, COUNT(*)
FROM silver_inspection
GROUP BY city_code;
```

**SCD Type-2 Version Check:**
```sql
SELECT business_nk, COUNT(*) AS version_count
FROM dim_restaurant_history
GROUP BY business_nk
HAVING COUNT(*) > 1;
```

**Fact Foreign Key Validation:**
```sql
SELECT COUNT(*) AS missing_restaurant_keys
FROM fact_inspection f
LEFT JOIN dim_restaurant_history d
ON f.restaurant_key = d.restaurant_key
WHERE d.restaurant_key IS NULL;
```

## Architecture: Medallion Model

**Bronze Layer**
- Ingest raw TSV files from Chicago and Dallas
- No transformations applied
- Validated row counts and schemas

**Silver Layer**
- Clean, standardize, and unify both cities into common schema
- Convert dates, normalize codes, unify inspection fields
- Output: `silver_inspection` table

**Gold Layer**
- Build dimensional warehouse (Star Schema):
  - `dim_restaurant_history` (SCD Type-2)
  - `dim_location`
  - `dim_date`
  - `dim_violation`
  - `fact_inspection`
  - `fact_inspection_violation`
- Implement Slowly Changing Dimensions Type-2
- Validate referential integrity

---

## Project Structure

```
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
```

---

## Validation Summary

**Bronze Layer:**
- ✔ Chicago and Dallas row counts match source
- ✔ Schema validated for both cities

**Silver Layer:**
- ✔ Total unified row count validated
- ✔ CHICAGO vs DALLAS distribution validated
- ✔ Null checks performed on critical columns

**Gold Layer:**
- ✔ `dim_restaurant_history` validated for SCD Type-2 versioning and `is_current` flag
- ✔ `fact_inspection` validated for row count and foreign key integrity
- ✔ `fact_inspection_violation` validated for referential integrity
- ✔ All Gold-layer tables exported to `/final_output/` as CSV

---

## Data Model (Star Schema)

**Fact Tables:**
- `fact_inspection`
- `fact_inspection_violation`

**Dimensions:**
- `dim_restaurant_history` (SCD Type-2)
- `dim_location`
- `dim_date`
- `dim_violation`

**Relationships:**
- `fact_inspection` → `restaurant_key` → `dim_restaurant_history`
- `fact_inspection` → `location_key` → `dim_location`
- `fact_inspection` → `date_key` → `dim_date`
- `fact_inspection_violation` → `violation_key` → `dim_violation`

(ERD diagram included in documentation)

---

## BI Reports

Gold-layer tables imported into Power BI / Tableau for reporting:
- Total inspections over time
- Pass vs Fail distribution
- Top violations by frequency
- City comparison (Chicago vs Dallas)
- Violation trend analysis

---

## Reproduction Steps

1. Import raw TSV files into Databricks Volumes
2. Run `bronze_ingestion.ipynb`
3. Run `silver_transformation.ipynb`
4. Run `gold_dim_fact_model.ipynb` (includes SCD Type-2 logic)
5. Run `validation_queries.ipynb`
6. Export results to `/final_output/`
7. Load CSVs into Power BI or Tableau

---

## Conclusion

This project demonstrates the full lifecycle of a BI solution:
- Data ingestion
- Standardization
- Dimensional modeling
- SCD Type-2 implementation
- ETL validation
- BI report generation

The final dataset provides clean, standardized, analytics-ready insights across two major U.S. cities' food inspection datasets.
