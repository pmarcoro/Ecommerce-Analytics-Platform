-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_seller_index
USING DELTA
AS

WITH seller_index AS (

  SELECT
    seller_key,
    reference_date,

    late_score,
    cancel_score,
    claim_score,

    -- lineal
    GREATEST(1, 5 - FLOOR(2 * late_component_linear + cancel_component_linear + claim_component_linear)) AS seller_index_linear,

    -- quadratic
    GREATEST(1, FLOOR(5 - FLOOR(2 *late_component_quadratic + cancel_component_quadratic + claim_component_quadratic))) AS seller_index_quadratic,

    -- sqrt
    GREATEST(1, FLOOR(5 - FLOOR(2 * late_component_sqrt + cancel_component_sqrt + claim_component_sqrt))) AS seller_index_sqrt
  FROM marketplace_olist.gold.fact_seller_index_components
)

SELECT
  seller_key,
  reference_date,

  late_score,
  cancel_score,
  claim_score,

  -- lineal
  seller_index_linear,
  CASE 
    WHEN seller_index_linear IN (1,2) THEN 0.15
    WHEN seller_index_linear = 3 THEN 0.30
    WHEN seller_index_linear IN (4,5) THEN 0.40
    ELSE 0.15
  END AS olist_shipping_coverage_lineal,

  -- quadratic
  seller_index_quadratic,
  CASE 
    WHEN seller_index_quadratic IN (1,2) THEN 0.15
    WHEN seller_index_quadratic = 3 THEN 0.30
    WHEN seller_index_quadratic IN (4,5) THEN 0.40
    ELSE 0.15
  END AS olist_shipping_coverage_quadratic,

  -- sqrt
  seller_index_sqrt,
  CASE 
    WHEN seller_index_sqrt IN (1,2) THEN 0.15
    WHEN seller_index_sqrt = 3 THEN 0.30
    WHEN seller_index_sqrt IN (4,5) THEN 0.40
    ELSE 0.15
  END AS olist_shipping_coverage_sqrt

FROM seller_index