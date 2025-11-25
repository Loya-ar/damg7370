import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# =========================================================
# SILVER BASE (cleaning)
# =========================================================

@dlt.table(
    name="silver_base_la",
    comment="Cleaned Silver Layer Base Table for LA crime."
)
def silver_base_la():
    df = dlt.read("bronze_la_crime")

    df_clean = (
        df
        # -------------------------
        # Date columns
        # -------------------------
        .withColumn("date_occ_ts", to_timestamp("DATE_OCC", "yyyy MMM dd hh:mm:ss a"))
        .withColumn("date_rptd_ts", to_timestamp("Date_Rptd", "yyyy MMM dd hh:mm:ss a"))
        .withColumn("date_key", to_date("date_occ_ts"))

        # -------------------------
        # Time cleaning
        # -------------------------
        .withColumn("time_occ_str", lpad(col("TIME_OCC").cast("string"), 4, "0"))
        .withColumn("hour", substring("time_occ_str", 1, 2).cast("int"))
        .withColumn("minute", substring("time_occ_str", 3, 2).cast("int"))
        .withColumn(
            "time_bucket",
            when((col("hour") >= 5) & (col("hour") < 12), "Morning")
            .when((col("hour") >= 12) & (col("hour") < 17), "Afternoon")
            .when((col("hour") >= 17) & (col("hour") < 21), "Evening")
            .otherwise("Night")
        )

        # -------------------------
        # Victim age cleaning
        # -------------------------
        .withColumn(
            "vict_age_clean",
            when((col("Vict_Age") < 0) | (col("Vict_Age") > 120), None)
            .otherwise(col("Vict_Age"))
        )
        .withColumn(
            "age_group",
            when(col("vict_age_clean").isNull(), "Unknown")
            .when(col("vict_age_clean") < 13, "0-12")
            .when(col("vict_age_clean") < 18, "13-17")
            .when(col("vict_age_clean") < 25, "18-24")
            .when(col("vict_age_clean") < 45, "25-44")
            .when(col("vict_age_clean") < 65, "45-64")
            .otherwise("65+")
        )

        # -------------------------
        # Victim sex cleaning
        # -------------------------
        .withColumn(
            "vict_sex_clean",
            when(col("Vict_Sex").isin("M", "F", "X"), col("Vict_Sex"))
            .otherwise("X")
        )

        # -------------------------
        # Coordinates cleaning
        # -------------------------
        .withColumn("lat_clean", when(col("LAT") == 0, None).otherwise(col("LAT")))
        .withColumn("lon_clean", when(col("LON") == 0, None).otherwise(col("LON")))
    )

    return df_clean


# =========================================================
# DIMENSIONS
# =========================================================

@dlt.table(
    name="dim_date_la",
    comment="Date dimension for LA crime."
)
def dim_date_la():
    base = dlt.read("silver_base_la").select("date_key").dropna().dropDuplicates()
    return (
        base
        .withColumn("year", year("date_key"))
        .withColumn("quarter", quarter("date_key"))
        .withColumn("month", month("date_key"))
        .withColumn("month_name", date_format("date_key", "MMMM"))
        .withColumn("week_of_year", weekofyear("date_key"))
        .withColumn("day_of_week_name", date_format("date_key", "EEEE"))
    )


@dlt.table(
    name="dim_time_la",
    comment="Time-of-day dimension for LA crime."
)
def dim_time_la():
    base = dlt.read("silver_base_la")
    return (
        base.select(
            col("TIME_OCC").alias("time_key"),
            "time_occ_str",
            "hour",
            "minute",
            "time_bucket"
        ).dropDuplicates()
    )


@dlt.table(
    name="dim_area_la",
    comment="Geographic Area dimension (LAPD areas)."
)
def dim_area_la():
    base = dlt.read("silver_base_la")
    return (
        base.select(
            col("AREA").alias("area_key"),
            col("AREA_NAME").alias("area_name"),
            col("lat_clean").alias("lat"),
            col("lon_clean").alias("lon")
        ).dropDuplicates()
    )


@dlt.table(
    name="dim_crime_la",
    comment="Crime code/type dimension."
)
def dim_crime_la():
    base = dlt.read("silver_base_la")
    return (
        base.select(
            col("Crm_Cd").alias("crime_key"),
            col("Crm_Cd_Desc").alias("crime_desc")
        ).dropDuplicates()
    )


@dlt.table(
    name="dim_status_la",
    comment="Crime case status dimension with arrest mapping."
)
def dim_status_la():
    base = dlt.read("silver_base_la")
    return (
        base.select(
            col("Status").alias("status_key"),
            col("Status_Desc").alias("status_desc")
        ).dropDuplicates()
        .withColumn(
            "is_arrest_made",
            when(col("status_key").isin("AA", "AO", "JA", "JO"), 1).otherwise(0)
        )
        .withColumn(
            "arrest_type",
            when(col("status_key").isin("AA", "AO"), "Adult")
            .when(col("status_key").isin("JA", "JO"), "Juvenile")
            .otherwise("None")
        )
    )


@dlt.table(
    name="dim_weapon_la",
    comment="Weapon dimension."
)
def dim_weapon_la():
    base = dlt.read("silver_base_la")
    return (
        base.select(
            col("Weapon_Used_Cd").alias("weapon_key"),
            col("Weapon_Desc").alias("weapon_desc")
        ).dropDuplicates()
    )


@dlt.table(
    name="dim_victim_la",
    comment="Victim demographic dimension."
)
def dim_victim_la():
    base = dlt.read("silver_base_la")
    return (
        base.select(
            col("vict_age_clean").alias("victim_age"),
            "age_group",
            col("vict_sex_clean").alias("victim_sex"),
            col("Vict_Descent").alias("victim_descent")
        ).dropDuplicates()
    )


# =========================================================
# FACT TABLE
# =========================================================

@dlt.table(
    name="fact_crime_incident_la",
    comment="Fact table of crime incidents."
)
def fact_crime_incident_la():
    base = dlt.read("silver_base_la")

    return base.select(
        col("DR_NO").alias("crime_incident_id"),
        col("AREA").alias("area_key"),
        col("Crm_Cd").alias("crime_key"),
        "date_key",
        col("TIME_OCC").alias("time_key"),
        col("Status").alias("status_key"),
        col("Weapon_Used_Cd").alias("weapon_key"),
        col("vict_age_clean").alias("victim_age"),
        col("vict_sex_clean").alias("victim_sex"),
        col("Vict_Descent").alias("victim_descent"),
        col("lat_clean").alias("lat"),
        col("lon_clean").alias("lon")
    )
