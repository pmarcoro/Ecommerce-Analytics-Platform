-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_seller_index_components
USING DELTA
AS

SELECT
  seller_key,
  reference_date,

  late_score,
  cancel_score,
  claim_score,

  -- linear
  (1 - late_score) AS late_component_linear,
  (1 - cancel_score) AS cancel_component_linear,
  (1 - claim_score) AS claim_component_linear,

  -- quadratic
  POW(1 - late_score,2) AS late_component_quadratic,
  POW(1 - cancel_score,2) AS cancel_component_quadratic,
  POW(1 - claim_score,2) AS claim_component_quadratic,

  -- sqrt
  SQRT(1 - late_score) AS late_component_sqrt,
  SQRT(1 - cancel_score) AS cancel_component_sqrt,
  SQRT(1 - claim_score) AS claim_component_sqrt

FROM marketplace_olist.gold.fact_seller_metrics_normalized