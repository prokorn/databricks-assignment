import dlt
from pyspark.sql.functions import col, current_timestamp, trim

# ==============================================================================
# 1. BRONZE LAYER
@dlt.table(
    name="movies_bronze",
    comment="Bronze layer: Raw movies data loaded from Volume via Auto Loader"
)
def movies_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/main/lab_data/movies_volume/")
        .withColumn("_ingested_at", current_timestamp())
    )

# ==============================================================================
# 2. SILVER LAYER
@dlt.table(
    name="movies_silver",
    comment="Silver layer: Cleaned movies data with expectations"
)
@dlt.expect_or_drop("valid_title", "title IS NOT NULL")
def movies_silver():
    
    df = dlt.read_stream("movies_bronze")
    return (
        df.select(
            trim(col("title")).alias("title"),
            trim(col("genres")).alias("genres"),
            trim(col("country")).alias("country"),
            trim(col("director")).alias("director"),
            trim(col("cast")).alias("cast"),
            col("_ingested_at")
        )
    )