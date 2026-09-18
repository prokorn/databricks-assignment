from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, coalesce, md5, concat, split, explode, lit

def clean_silver_metadata(df: DataFrame) -> DataFrame:
    """Clean, trim whitespace, and enforce types for all movie attributes."""
    return df.select(
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

def add_surrogate_movie_key(df: DataFrame) -> DataFrame:
    """Generate deterministic MD5 surrogate key for dim_movies."""
    return df.withColumn(
        "movie_id",
        md5(concat(coalesce(col("title"), lit("")), coalesce(col("country"), lit("")), coalesce(col("release_year").cast("string"), lit(""))))
    )

def split_and_trim_genres(df: DataFrame) -> DataFrame:
    """Explode and clean genre tags for dim_genres."""
    return (
        df.filter(col("genres").isNotNull())
        .withColumn("genre_name", explode(split(col("genres"), ",")))
        .withColumn("genre_name", trim(col("genre_name")))
        .withColumn("genre_id", md5(col("genre_name")))
    )