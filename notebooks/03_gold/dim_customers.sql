-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.dim_customers
USING DELTA
AS
SELECT
  customer_id AS customer_key,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM marketplace_olist.silver.customers;