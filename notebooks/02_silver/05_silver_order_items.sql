-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.silver.order_items
USING DELTA
AS
SELECT
  TRIM(order_id) AS order_id,
  CAST(TRIM(order_item_id) AS STRING) AS order_item_id,
  CAST(TRIM(product_id) AS STRING) AS product_id,
  CAST(TRIM(seller_id ) AS STRING) AS seller_id,
  shipping_limit_date,
  CAST(price AS DECIMAL(10,2)) AS price,  
  CAST(freight_value AS DECIMAL(10,2)) AS freight_value,
  ingestion_ts
FROM marketplace_olist.bronze.order_items


 --- Check all order_item_id are unique
%sql
SELECT order_item_id, count(order_item_id)
FROM marketplace_olist.silver.order_items oi
GROUP BY order_item_id
HAVING count(order_item_id) > 1

 --- Check all order_id match with orders table
%sql
SELECT o.order_id
FROM marketplace_olist.silver.orders o
LEFT JOIN marketplace_olist.silver.order_items oi
ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL



 --- Check all seller_id match with sellers table
%sql
SELECT oi.order_id
FROM marketplace_olist.silver.order_items oi
FULL OUTER JOIN marketplace_olist.silver.sellers s
ON s.seller_id = oi.seller_id
WHERE oi.order_id IS NULL OR s.seller_id IS NULL


 --- Flag prices and freight values below or equal to 0
%sql
SELECT price, freight_value
FROM marketplace_olist.silver.order_items
WHERE price <= 0 OR freight_value <= 0
