-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.dim_date
USING DELTA
AS
SELECT DISTINCT
  order_purchase_timestamp AS date_key,
  YEAR(order_purchase_timestamp)  AS year,
  MONTH(order_purchase_timestamp) AS month,
  DAY(order_purchase_timestamp)   AS day,
  WEEKOFYEAR(order_purchase_timestamp) AS week,
  QUARTER(order_purchase_timestamp) AS quarter
FROM marketplace_olist.silver.orders;