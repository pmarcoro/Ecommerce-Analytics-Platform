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
  (late_score) AS late_penalty_linear, 
  (cancel_score) AS cancel_penalty_linear,
  (claim_score) AS claim_penalty_linear,

  -- quadratic
  POW(late_score,2) AS late_penalty_quadratic,
  POW(cancel_score,2) AS cancel_penalty_quadratic,
  POW(claim_score,2) AS claim_penalty_quadratic,

  -- sqrt
  SQRT(late_score) AS late_penalty_sqrt,
  SQRT(cancel_score) AS cancel_penalty_sqrt,
  SQRT(claim_score) AS claim_penalty_sqrt

FROM marketplace_olist.gold.fact_seller_metrics_normalized