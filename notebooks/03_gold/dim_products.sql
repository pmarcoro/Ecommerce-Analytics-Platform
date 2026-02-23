-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.dim_products
USING DELTA
AS
SELECT
  product_id AS product_key,
  product_category_name,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM marketplace_olist.silver.products;