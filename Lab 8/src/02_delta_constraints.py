# Databricks notebook source
# DBTITLE 1,Widget Parameters
# Widget parameters for catalog and schema
dbutils.widgets.text("catalog", "main", "Catalog Name")
dbutils.widgets.text("schema", "lab_data", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# DBTITLE 1,Delta Constraints (Idempotent)
# MAGIC %sql
# MAGIC -- DDL Migration: Delta Lake Storage Constraints (Idempotent)
# MAGIC USE CATALOG ${catalog};
# MAGIC USE SCHEMA ${schema};
# MAGIC
# MAGIC -- Add constraints to dim_movies
# MAGIC ALTER TABLE dim_movies DROP CONSTRAINT IF EXISTS chk_movie_id_not_null;
# MAGIC ALTER TABLE dim_movies ADD CONSTRAINT chk_movie_id_not_null CHECK (movie_id IS NOT NULL);
# MAGIC
# MAGIC ALTER TABLE dim_movies DROP CONSTRAINT IF EXISTS chk_movie_title_not_null;
# MAGIC ALTER TABLE dim_movies ADD CONSTRAINT chk_movie_title_not_null CHECK (title IS NOT NULL AND length(title) > 0);
# MAGIC
# MAGIC -- Add constraints to dim_genres
# MAGIC ALTER TABLE dim_genres DROP CONSTRAINT IF EXISTS chk_genre_id_not_null;
# MAGIC ALTER TABLE dim_genres ADD CONSTRAINT chk_genre_id_not_null CHECK (genre_id IS NOT NULL);
# MAGIC
# MAGIC ALTER TABLE dim_genres DROP CONSTRAINT IF EXISTS chk_genre_name_not_null;
# MAGIC ALTER TABLE dim_genres ADD CONSTRAINT chk_genre_name_not_null CHECK (genre_name IS NOT NULL AND length(genre_name) > 0);

# COMMAND ----------

