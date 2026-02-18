-- Databricks notebook source
-- DBTITLE 1,Cell 1
CREATE OR REPLACE TABLE marketplace_olist.silver.order_items
USING DELTA
AS
SELECT
  CAST(TRIM(order_id) AS STRING) AS order_id,
  CAST(TRIM(order_item_id) AS STRING) AS order_item_id,
  CAST(TRIM(product_id) AS STRING) AS product_id,
  CAST(TRIM(seller_id) AS STRING) AS seller_id,
  shipping_limit_date,
  CAST(price AS DECIMAL(10,2)) AS price, 
  CAST(freight_value AS DECIMAL(10,2)) AS freight_value,
  ingestion_ts
FROM marketplace_olist.bronze.order_items

--- Check for duplicates:
SELECT order_id, order_item_id, COUNT(*)
FROM marketplace_olist.silver.order_items
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1

--- Check for nulls:
SELECT *
FROM marketplace_olist.silver.order_items
WHERE order_id IS NULL
   OR product_id IS NULL
   OR seller_id IS NULL


--- Check order_id available in order_items table but not in orders table
%sql
SELECT oi.order_id
FROM marketplace_olist.silver.order_items oi
LEFT JOIN marketplace_olist.silver.orders o
ON oi.order_id = o.order_id
WHERE o.order_id IS NULL

--- Check order_id available in ordera table but not in order_items table

%sql
SELECT o.order_id
FROM marketplace_olist.silver.orders o
LEFT JOIN marketplace_olist.silver.order_items oi
ON o.order_id = oi.order_id
WHERE oi.order_id IS NULL ---- 775 orders without an item associated to them 

--- Check if product_id is available in products table
--- ###### Pending to create products table ######
%sql
SELECT oi.product_id
FROM marketplace_olist.silver.order_items oi
LEFT JOIN marketplace_olist.silver.products p
ON oi.product_id = p.product_id
WHERE p.product_id IS NULL


--- Check if seller_id is available in sellers table
%sql
SELECT oi.seller_id
FROM marketplace_olist.silver.order_items oi
LEFT JOIN marketplace_olist.silver.sellers s
ON oi.seller_id = s.seller_id
WHERE s.seller_id IS NULL


 --- Flag prices and freight values below or equal to 0
%sql
SELECT *
FROM marketplace_olist.silver.order_items
WHERE price <= 0 OR freight_value <= 0
