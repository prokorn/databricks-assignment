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
            trim(col("release_year")).cast("int").alias("release_year"),
            trim(col("release_month")).cast("int").alias("release_month"),
            trim(col("release_day")).cast("int").alias("release_day"),
            trim(col("genres")).alias("genres"),
            trim(col("rating_out_of_10")).cast("double").alias("rating_out_of_10"),
            trim(col("vote_count")).cast("int").alias("vote_count"),
            trim(col("runtime_minutes")).cast("int").alias("runtime_minutes"),
            trim(col("box_office_usd")).cast("double").alias("box_office_usd"),
            trim(col("budget_usd")).cast("double").alias("budget_usd"),
            trim(col("director")).alias("director"),
            trim(col("screenwriter")).alias("screenwriter"),
            trim(col("cast")).alias("cast"),
            trim(col("production_company")).alias("production_company"),
            trim(col("content_rating")).alias("content_rating"),
            trim(col("language")).alias("language"),
            trim(col("country")).alias("country"),
            trim(col("synopsis")).alias("synopsis"),
            trim(col("wikipedia_url")).alias("wikipedia_url"),
            trim(col("poster_url")).alias("poster_url")
        )
    )