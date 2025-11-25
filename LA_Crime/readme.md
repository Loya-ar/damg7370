LA Crime Data Analysis – End-to-End Pipeline

This project builds a complete data engineering pipeline for the Los Angeles Crime Dataset (2020–Present) using Databricks and Delta Live Tables.
The goal is to clean the data, design a star schema, and create analytics-ready tables for visualization.

1. Project Summary

Source: LA Open Data Portal – Crime Data

Tools: Databricks, PySpark, Delta Live Tables, ER Studio/Navicat, Power BI/Tableau

Pipeline: Bronze → Silver → Gold

Output: Fact + Dimension tables supporting business analysis.

2. Data Profiling

Key profiling steps:

Checked schema, row counts, and column types

Null counts & distinct counts

Found invalid values:

Ages <0 or >120

Invalid TIME OCC (not 0000–2359)

LAT/LON = 0 or missing

Verified date ranges (2020–Present)

Reviewed distributions: Vict Sex, Status, Crimes, Weapons, Areas

All issues were cleaned and standardized in the Silver layer.

3. Dimensional Model

A Star Schema with:

Fact Table

fact_crime_incident_la

Grain: 1 row per crime incident (DR_NO)

Dimensions

dim_date_la

dim_time_la

dim_area_la

dim_crime_la

dim_status_la

dim_weapon_la

dim_victim_la

Model approved by professor.

4. Bronze Layer

Raw CSV loaded directly from source

No transformations

Stored as bronze_la_crime

5. Silver Layer

Main cleaning logic:

Parse DATE OCC / Date Rptd

Parse TIME OCC into hour, minute, bucket

Clean invalid ages

Standardize victim sex (M/F/X)

Fix LAT/LON (replace 0 with NULL)

Prepare clean base table: silver_base_la

Dimensions were created from this clean Silver dataset.

6. Gold Layer

Final analytics tables created using DLT:

All dimension tables (dim_*_la)

fact_crime_incident_la

These are ready for Power BI/Tableau dashboards.

7. Screenshots to Include

Attach the following in your documentation:

Profiling

Schema (printSchema)

Null counts

Distinct counts

Range checks (dates, time, age, lat/lon)

Category distributions (sex, status, crime, weapon, area)

ERD

Full approved star schema

Bronze

Bronze table preview

Bronze pipeline screenshot

Silver

Silver table preview

Cleaning logic

DLT graph (Bronze → Silver)

Gold

Each dimension table preview

Fact table preview

Full pipeline graph (Bronze → Silver → Gold)
