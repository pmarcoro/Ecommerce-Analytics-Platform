-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.dim_sellers
USING DELTA
AS
SELECT
  seller_id AS seller_key,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM marketplace_olist.silver.sellers;