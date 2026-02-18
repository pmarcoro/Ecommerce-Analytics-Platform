-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.silver.sellers
USING DELTA
AS

SELECT
   CAST(TRIM(seller_id) AS STRING) AS seller_id,
   CAST(
    LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0')
     AS STRING
     ) AS seller_zip_code_prefix,
   TRIM(seller_city) AS seller_city,
   TRIM(seller_state) AS seller_state,
   ingestion_ts

FROM marketplace_olist.bronze.sellers
WHERE seller_id IS NOT NULL

 --- Check for duplicates
SELECT seller_id, COUNT(*)
FROM marketplace_olist.silver.sellers
GROUP BY seller_id
HAVING COUNT(*) > 1
