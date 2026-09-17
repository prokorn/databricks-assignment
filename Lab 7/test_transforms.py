import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from transforms import clean_silver_metadata, add_surrogate_movie_key, split_and_trim_genres

@pytest.fixture(scope="session")
def spark():
    """Spark session fixture (works both in the cluster and through Databricks Connect)."""
    return SparkSession.builder.appName("UnitTests").getOrCreate()

def test_clean_silver_metadata(spark):
    # Preparation of data with extra spaces and string numbers
    input_data = [
        Row(
            title=" Inception ",
            release_year=" 2010 ",
            release_month=" 7 ",
            release_day=" 16 ",
            genres="Action, Sci-Fi",
            rating_out_of_10=" 8.8 ",
            vote_count=" 2000000 ",
            runtime_minutes=" 148 ",
            box_office_usd=" 836800000.0 ",
            budget_usd=" 160000000.0 ",
            director=" Christopher Nolan ",
            screenwriter=" Christopher Nolan ",
            cast=" Leonardo DiCaprio, Joseph Gordon-Levitt ",
            production_company=" Warner Bros ",
            content_rating=" PG-13 ",
            language=" English ",
            country=" USA ",
            synopsis=" A thief who steals corporate secrets ",
            wikipedia_url=" https://en.wikipedia.org/wiki/Inception ",
            poster_url=" https://example.com/inception.jpg "
        )
    ]
    df = spark.createDataFrame(input_data)
    
    result_df = clean_silver_metadata(df)
    row = result_df.collect()[0]
    
    assert row.title == "Inception"
    assert row.release_year == 2010
    assert isinstance(row.release_year, int)
    assert row.rating_out_of_10 == 8.8
    assert isinstance(row.rating_out_of_10, float)
    assert row.country == "USA"

def test_add_surrogate_movie_key(spark):
    # Checking coalesce processing and MD5 stability
    input_data = [
        Row(title="Interstellar", country="USA"),
        Row(title="Interstellar", country=None)
    ]
    df = spark.createDataFrame(input_data)
    
    result_df = add_surrogate_movie_key(df)
    rows = result_df.collect()
    
    # Keys must be generated and not empty
    assert rows[0].movie_id is not None
    assert rows[1].movie_id is not None
    assert rows[0].movie_id != rows[1].movie_id

def test_split_and_trim_genres(spark):
    # Check genre string split to individual entries and whitespace cleanup
    input_data = [
        Row(genres="Drama, Comedy , Thriller ")
    ]
    df = spark.createDataFrame(input_data)
    
    result_df = split_and_trim_genres(df)
    genre_names = [r.genre_name for r in result_df.collect()]
    
    assert len(genre_names) == 3
    assert genre_names == ["Drama", "Comedy", "Thriller"]