-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.silver.orders
USING DELTA
AS
SELECT
  TRIM(order_id) AS order_id,
  TRIM(customer_id) AS customer_id,
  TRIM(order_status) AS order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date, 
  order_delivered_customer_date,
  order_estimated_delivery_date,
  CASE 
    WHEN order_purchase_timestamp IS NOT NULL
      AND order_approved_at IS NOT NULL
      AND order_approved_at > order_purchase_timestamp
    THEN 1 ELSE 0
  END AS invalid_approval_ts,
  CASE
    WHEN order_approved_at IS NOT NULL 
      AND order_delivered_carrier_date IS NOT NULL 
      AND order_approved_at > order_delivered_carrier_date 
    THEN 1 ELSE 0
  END AS invalid_delivery_ts,
  CASE
    WHEN order_delivered_carrier_date IS NOT NULL 
      AND order_delivered_customer_date IS NOT NULL 
      AND order_delivered_carrier_date > order_delivered_customer_date
    THEN 1 ELSE 0
  END AS invalid_customer_delivery_ts

  
  FROM marketplace_olist.bronze.orders

  --- Monitoring timestamp inconsistencies
%sql
SELECT * 
FROM marketplace_olist.silver.orders
WHERE invalid_approval_ts = 1 
   OR invalid_delivery_ts = 1 
   OR invalid_customer_delivery_ts = 1


--- Check client_id inconsistencies
%sql
SELECT o.*
FROM marketplace_olist.silver.orders o
LEFT JOIN marketplace_olist.silver.customers c
  ON o.customer_id = c.customer_id
WHERE 
    c.customer_id IS NULL
