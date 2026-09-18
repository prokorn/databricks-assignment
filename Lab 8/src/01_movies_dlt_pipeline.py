import dlt
import sys
import os
from pyspark.sql.functions import col, current_timestamp, when, lit

# Safe path resolution for Databricks DLT runtime
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = os.getcwd()

if current_dir not in sys.path:
    sys.path.append(current_dir)

from transforms import clean_silver_metadata


# --- DQX: Realistic Data Quality Validation Rules ---
# Nullable rating and release_year are tolerated if title is present
DQ_RULES = {
    "valid_title": "title IS NOT NULL AND length(title) > 0",
    "valid_release_year": "release_year IS NULL OR (release_year >= 1888 AND release_year <= 2026)",
    "valid_rating": "rating_out_of_10 IS NULL OR (rating_out_of_10 >= 0.0 AND rating_out_of_10 <= 10.0)"
}

COMBINED_RULE = f"({DQ_RULES['valid_title']}) AND ({DQ_RULES['valid_release_year']}) AND ({DQ_RULES['valid_rating']})"


# 1. BRONZE LAYER
@dlt.table(
    name="movies_bronze",
    comment="Bronze layer: Raw movies ingested via Auto Loader"
)
def movies_bronze():
    # Dynamically retrieve volume path from pipeline configuration
    source_volume = spark.conf.get("movies.source_volume", "/Volumes/main/lab_data/movies_volume/")
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_volume)
        .withColumn("_ingested_at", current_timestamp())
    )


# 2. SILVER LAYER
@dlt.table(
    name="movies_silver",
    comment="Silver layer: Validated and cleaned movie dataset"
)
@dlt.expect_all_or_drop(DQ_RULES)
def movies_silver():
    df_bronze = dlt.read_stream("movies_bronze")
    return clean_silver_metadata(df_bronze)


# 3. QUARANTINE TABLE
@dlt.table(
    name="movies_quarantine",
    comment="Quarantine table: Defective records failing DQ rules"
)
def movies_quarantine():
    df_cleaned = clean_silver_metadata(dlt.read_stream("movies_bronze"))
    
    return (
        df_cleaned
        .filter(f"NOT ({COMBINED_RULE})")
        .withColumn("_quarantined_at", current_timestamp())
        .withColumn(
            "failure_reason",
            when(~col("title").isNotNull() | (col("title") == ""), lit("Invalid Title"))
            .when(~col("release_year").between(1888, 2026), lit("Invalid Year"))
            .otherwise(lit("Invalid Rating Range"))
        )
    )