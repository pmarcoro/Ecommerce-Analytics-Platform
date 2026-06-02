-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_olist_financial_scenarios
USING DELTA
AS

WITH freight_rules AS(
  SELECT
  o.order_purchase_timestamp,
  oi.order_id,
  oi.order_item_id,
  oi.seller_key,
  oi.price,
  oi.freight_value,
  CASE
    WHEN oi.price <= 79
    THEN 1.0 
    ELSE 0.0
    END AS client_shipping_share,
  CASE
    WHEN oi.price > 79
    THEN 0.5
    ELSE 0.0
    END AS baseline_olist_shipping_share,
  CASE
    WHEN oi.price > 79
    THEN 0.5 
    ELSE 0.0 
    END AS baseline_seller_shipping_share,
  si.seller_index_linear,
  si.seller_index_quadratic,
  si.seller_index_logistic,
  CASE
    WHEN oi.price > 79
    THEN
      CASE
          WHEN si.seller_index_linear = 5 THEN 0.5
          WHEN si.seller_index_linear = 4 THEN 0.4
          WHEN si.seller_index_linear = 3 THEN 0.3
          WHEN si.seller_index_linear = 2 THEN 0.15
          WHEN si.seller_index_linear = 1 THEN 0.15
      END
    ELSE NULL
    END AS linear_olist_shipping_share,
  CASE
    WHEN oi.price > 79
    THEN
      CASE
          WHEN si.seller_index_quadratic = 5 THEN 0.5
          WHEN si.seller_index_quadratic = 4 THEN 0.4
          WHEN si.seller_index_quadratic = 3 THEN 0.3
          WHEN si.seller_index_quadratic = 2 THEN 0.15
          WHEN si.seller_index_quadratic = 1 THEN 0.15
      END
    ELSE NULL
    END AS quadratic_olist_shipping_share,
  CASE
    WHEN oi.price > 79
    THEN
      CASE
          WHEN si.seller_index_logistic = 5 THEN 0.5
          WHEN si.seller_index_logistic = 4 THEN 0.4
          WHEN si.seller_index_logistic = 3 THEN 0.3
          WHEN si.seller_index_logistic = 2 THEN 0.15
          WHEN si.seller_index_logistic = 1 THEN 0.15
      END
    ELSE NULL
    END AS logistic_olist_shipping_share

FROM marketplace_olist.gold.fact_order_items oi
LEFT JOIN marketplace_olist.gold.fact_orders o
  ON oi.order_id = o.order_id
LEFT JOIN marketplace_olist.gold.fact_seller_index si
  ON CAST(o.order_purchase_timestamp AS DATE) = si.reference_date
  AND oi.seller_key = si.seller_key
)

SELECT
  order_purchase_timestamp,
  order_id,
  order_item_id,
  seller_key,
  price,
  freight_value,

  CASE 
    WHEN price + freight_value > 0 
    THEN 5 ELSE 0 
    END AS fixed_fee,
  CASE
    WHEN price > 0
    THEN  price - (price / 1.21) ELSE 0
    END AS variable_fee,

  -- Client shipping share

  client_shipping_share,
  ROUND(freight_value * client_shipping_share,2) AS client_shipping_cost,
  
  -- Baseline (fixed 50 / 50 shipping share between OLIST and sellers)

  baseline_olist_shipping_share,
  ROUND(freight_value * baseline_olist_shipping_share,2) AS baseline_olist_shipping_cost,
  baseline_seller_shipping_share,
  ROUND(freight_value * baseline_seller_shipping_share,2) AS baseline_seller_shipping_cost,


  -- Linear penalization function
  seller_index_linear,
  linear_olist_shipping_share,
  ROUND(freight_value * linear_olist_shipping_share,2) AS linear_olist_shipping_cost,
  ROUND((1.0 - client_shipping_share - linear_olist_shipping_share),2) AS linear_seller_shipping_share,
  ROUND(freight_value * linear_seller_shipping_share,2) AS linear_seller_shipping_cost,

  -- Quadratic penalization function
  seller_index_quadratic,
  quadratic_olist_shipping_share,
  ROUND((freight_value * quadratic_olist_shipping_share),2) AS quadratic_olist_shipping_cost,
  ROUND((1.0 - client_shipping_share - quadratic_olist_shipping_share),2) AS quadratic_seller_shipping_share,
  ROUND((freight_value * quadratic_seller_shipping_share),2) AS quadratic_seller_shipping_cost,

  -- Logistic penalization function
  seller_index_logistic,
  logistic_olist_shipping_share,
  ROUND((freight_value * logistic_olist_shipping_share),2) AS logistic_olist_shipping_cost,
  ROUND((1.0 - client_shipping_share - logistic_olist_shipping_share),2) AS logistic_seller_shipping_share,
  ROUND((freight_value * logistic_seller_shipping_share),2) AS logistic_seller_shipping_cost

  
  FROM freight_rules