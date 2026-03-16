-- Databricks notebook source
%sql
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_seller_metrics_smoothed
USING DELTA
AS

WITH parameters AS (
  SELECT
    15 AS alpha_orders,
    15 AS alpha_reviews,
    0.0785 AS global_late_rate,
    0.0046 AS global_cancel_rate,
    0.1486 AS global_claim_rate
)

SELECT
  m.seller_key,
  m.reference_date,
  m.orders_30d,
  m.reviews_60d,

  -- Bayesian Smoothing
  (m.late_shipments_30d + p.alpha_orders * p.global_late_rate) / (m.orders_30d + p.alpha_orders) AS late_rate_smoothed,
  (m.cancellations_30d + p.alpha_orders * p.global_cancel_rate) / (m.orders_30d + p.alpha_orders) AS cancel_rate_smoothed,
  (m.claims_60d + p.alpha_reviews * p.global_claim_rate) / (m.reviews_60d + p.alpha_reviews) AS 
  claim_rate_smoothed
  
FROM marketplace_olist.gold.fact_seller_metrics m
CROSS JOIN parameters p