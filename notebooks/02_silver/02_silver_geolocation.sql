-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.silver.geolocation
USING DELTA
AS

SELECT 
  CAST(
    LPAD(CAST(geolocation_zip_code_prefix AS STRING),5,0)
    AS STRING
    ) AS geolocation_zip_code_prefix,
  CAST(geolocation_lat AS DOUBLE) AS geolocation_lat,     --- Using DOUBLE datatype to balance precision + storage efficiency
  CAST(geolocation_lng AS DOUBLE) AS geolocation_lng, 
  TRIM(geolocation_city) AS geolocation_city,
  TRIM(geolocation_state) AS geolocation_state,
  ingestion_ts
  
FROM marketplace_olist.bronze.geolocation
WHERE geolocation_zip_code_prefix IS NOT NULL

