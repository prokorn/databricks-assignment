-- DDL Migration: Delta Lake Storage Constraints
USE CATALOG main;
USE SCHEMA lab_data;

-- Add constraints to dim_movies
ALTER TABLE dim_movies 
ADD CONSTRAINT chk_movie_id_not_null 
CHECK (movie_id IS NOT NULL);

ALTER TABLE dim_movies 
ADD CONSTRAINT chk_movie_title_not_null 
CHECK (title IS NOT NULL AND length(title) > 0);

-- Add constraints to dim_genres
ALTER TABLE dim_genres 
ADD CONSTRAINT chk_genre_id_not_null 
CHECK (genre_id IS NOT NULL);

ALTER TABLE dim_genres 
ADD CONSTRAINT chk_genre_name_not_null 
CHECK (genre_name IS NOT NULL AND length(genre_name) > 0);