-- Databricks notebook source
CREATE OR REPLACE TABLE seller_index_ratios AS
SELECT
  seller_key,
  reference_date,
  orders_30d,
  late_shipments_30d / NULLIF(orders_30d,0) AS pct_late_shipments,
  cancellations_30d / NULLIF(orders_30d,0) AS pct_cancellations,
  total_reviews_60d,
  avg_review_score_60d,
  claims_60d / NULLIF(total_reviews_60d,0) AS pct_claims

FROM marketplace_olist.gold.fact_seller_metrics;

-- COMMAND ----------

SELECT * FROM seller_index_ratios

-- COMMAND ----------

CREATE OR REPLACE TABLE marketplace_olist.gold.fact_seller_index_daily 
USING DELTA
AS

WITH si AS (

  SELECT
    seller_key,
    reference_date,
    pct_late_shipments,
    pct_cancellations,
    pct_claims,
    avg_review_score_60d,

    -- Individual components
    2 * GREATEST(0, pct_late_shipments / 0.01 - 1) AS component_late_shipments,
    1 * GREATEST(0, pct_cancellations / 0.0109 - 1) AS component_cancellations,
    1 * GREATEST(0, pct_claims / 0.0074 - 1) AS component_claims

  FROM seller_index_ratios
)

SELECT
  seller_key,
  reference_date,
  pct_late_shipments,
  pct_cancellations,
  pct_claims,
  avg_review_score_60d,

  component_late_shipments,
  component_cancellations,
  component_claims,

  -- Seller index base
  GREATEST(1, 5 - FLOOR(component_late_shipments + component_cancellations + component_claims)) AS seller_index,

  -- Olist shipping coverage depending on the seller index value
  CASE 
    WHEN GREATEST(1, 5 - FLOOR(component_late_shipments + component_cancellations + component_claims)) IN (1,2) THEN 0.15
    WHEN GREATEST(1, 5 - FLOOR(component_late_shipments + component_cancellations + component_claims)) = 3 THEN 0.30
    WHEN GREATEST(1, 5 - FLOOR(component_late_shipments + component_cancellations + component_claims)) IN (4,5) THEN 0.40
    ELSE 0.15
  END AS olist_shipping_coverage_percentage

FROM si;