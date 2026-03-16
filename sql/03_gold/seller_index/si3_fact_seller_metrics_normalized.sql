-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_seller_metrics_normalized 
USING DELTA
AS

WITH params AS (

SELECT
    -- thresholds (objetivos Olist)
    0.01  AS late_threshold,
    0.0109  AS cancel_threshold,
    0.15  AS claim_threshold,

    -- upper limits 
    0.136 AS late_upper,
    0.011 AS cancel_upper,
    0.22  AS claim_upper
)

SELECT
    m.seller_key,
    m.reference_date,
    m.orders_30d,
    m.reviews_60d,

    m.late_rate_smoothed,
    m.cancel_rate_smoothed,
    m.claim_rate_smoothed,

    -- LATE SHIPMENTS SCORE
    CASE
        WHEN m.late_rate_smoothed <= p.late_threshold THEN 1
        WHEN m.late_rate_smoothed >= p.late_upper THEN 0
        ELSE 1 - (
            (m.late_rate_smoothed - p.late_threshold) /
            (p.late_upper - p.late_threshold)
        )
    END AS late_score,

    -- CANCELLATION SCORE
    CASE
        WHEN m.cancel_rate_smoothed <= p.cancel_threshold THEN 1
        WHEN m.cancel_rate_smoothed >= p.cancel_upper THEN 0
        ELSE 1 - (
            (m.cancel_rate_smoothed - p.cancel_threshold) /
            (p.cancel_upper - p.cancel_threshold)
        )
    END AS cancel_score,

    -- CLAIM SCORE
    CASE
        WHEN m.claim_rate_smoothed <= p.claim_threshold THEN 1
        WHEN m.claim_rate_smoothed >= p.claim_upper THEN 0
        ELSE 1 - (
            (m.claim_rate_smoothed - p.claim_threshold) /
            (p.claim_upper - p.claim_threshold)
        )
    END AS claim_score

FROM marketplace_olist.gold.fact_seller_metrics_smoothed m
CROSS JOIN params p