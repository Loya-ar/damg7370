# Seattle Pet License Data Warehouse Project

## Overview
This project builds an end-to-end data pipeline using Azure Data Factory and Snowflake to transform and analyze Seattle Pet License data.

## Architecture
1. Extract: Source CSV file from Azure Blob Storage
2. Transform: Convert CSV → Parquet (ADF pipeline)
3. Load: Load Parquet → Snowflake Staging → Dimension + Fact tables
4. Model: Star schema with DATE_DIM, BREED_DIM, LOCATION_DIM, and PETLICENSE_FACT

## Deliverables
- ✅ SQL Scripts: Schema creation and data load
- ✅ ER Diagram: Star schema with 3 dimensions, 1 fact
- ✅ ADF Pipelines: CSV→Parquet→Snowflake→DW
- ✅ GitHub Repo: Integrated and published JSONs
- ✅ Snowflake Proof: Row counts match, queries validated

## Row Count Validation
| Table | Row Count |
|--------|------------|
| STG_PETLICENSE | 41,900 |
| PETLICENSE_FACT | 41,733 |
| DATE_DIM | 954 |
| BREED_DIM | 3,188 |
| LOCATION_DIM | 195 |

## Team & Tools
**Tools:** Azure Data Factory, Snowflake, SQL, ER Studio  
**Team:** [Your Team Members’ Names]

---
