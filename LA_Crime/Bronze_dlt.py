import dlt
from pyspark.sql.functions import current_timestamp, col

# Folder (volume) where your CSV lives
RAW_PATH = "/Volumes/workspace/crime/la_crime"   # your volume path


@dlt.table(
    name="bronze_la_crime",
    comment="Raw LA crime data with lightly cleaned column names (Bronze Layer)."
)
def bronze_la_crime():
    # Read all CSV files in the folder
    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(RAW_PATH)
    )

    # Sanitize column names: replace spaces, slashes, dashes, parentheses
    df = df.select([
        col(c).alias(
            c.strip()
             .replace(" ", "_")
             .replace("/", "_")
             .replace("-", "_")
             .replace("(", "")
             .replace(")", "")
        )
        for c in df.columns
    ])

    # Add ingest timestamp
    return df.withColumn("_ingest_ts", current_timestamp())
