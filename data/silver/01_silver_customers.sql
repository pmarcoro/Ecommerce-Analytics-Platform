-- Databricks notebook source

CREATE OR REPLACE TABLE marketplace_olist.silver.customers
USING DELTA
AS

SELECT
    TRIM(customer_id) AS customer_id,
    TRIM(customer_unique_id) AS customer_unique_id,
    CAST(
        LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0')
        AS STRING
    ) AS customer_zip_code_prefix,
    TRIM(customer_city) AS customer_city,
    TRIM(customer_state) AS customer_state,
    ingestion_ts
FROM marketplace_olist.bronze.customers
WHERE customer_id IS NOT NULL;

