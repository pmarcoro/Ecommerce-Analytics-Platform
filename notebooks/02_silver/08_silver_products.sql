-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.silver.products
USING DELTA
AS
SELECT
  CAST(TRIM(p.product_id) AS STRING) AS product_id,
  CAST(TRIM(t.product_category_name) AS STRING) AS product_category_name,
  product_name_lenght,
  product_description_lenght,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm,
  p.ingestion_ts
FROM
  marketplace_olist.bronze.products p
  LEFT JOIN marketplace_olist.bronze.product_cat_name_translation t
  ON p.product_category_name = t.product_category_name

--- Check products without category ---
%sql
SELECT * FROM marketplace_olist.silver.products 
WHERE product_category_name IS NULL

--- Check duplicate in keys ---
%sql 
SELECT product_id, COUNT(*) 
FROM marketplace_olist.silver.products 
GROUP BY product_id 
HAVING COUNT(*) > 1





