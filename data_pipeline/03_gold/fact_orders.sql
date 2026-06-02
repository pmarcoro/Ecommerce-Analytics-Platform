-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_orders
USING DELTA
AS
WITH order_items_agg AS (
  SELECT
      order_id,
      COUNT(order_item_id) AS total_items,
      SUM(price) AS total_amount,
      SUM(freight_value) AS freight_value_total,
      MIN(shipping_limit_date) AS order_shipping_limit_date -- Even if only one item, we want the shipping limit date
  FROM marketplace_olist.silver.order_items
  GROUP BY order_id
)

SELECT
  o.order_id,
  o.customer_id AS customer_key,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  o.order_status,
  oi.total_items,
  oi.total_amount,
  oi.freight_value_total,
  order_shipping_limit_date,

  CASE
    WHEN o.order_delivered_carrier_date IS NOT NULL 
    THEN 1 ELSE 0
  END AS delivered_to_carrier_flag,

  CASE 
    WHEN o.order_delivered_carrier_date IS NOT NULL 
    AND o.order_delivered_carrier_date > order_shipping_limit_date
      THEN 1
      ELSE 0
  END AS late_to_carrier_flag,

  CASE
    WHEN o.order_delivered_customer_date IS NOT NULL 
    THEN 1 ELSE 0
  END AS delivered_to_customer_flag,

  CASE 
    WHEN o.order_delivered_customer_date > o.order_estimated_delivery_date
    THEN 1 ELSE 0
  END AS late_to_customer_flag,

  CASE
    WHEN o.order_delivered_customer_date IS NOT NULL
    THEN DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)
  END AS delivery_days
  
FROM marketplace_olist.silver.orders o
LEFT JOIN order_items_agg oi
ON o.order_id = oi.order_id;