-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_reviews
USING DELTA
AS
WITH ranked_reviews AS (

  SELECT
    order_id,
    review_id,
    review_score,
    review_creation_date,
    review_answer_timestamp,
    ingestion_ts,

    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY review_answer_timestamp DESC
    ) AS rn

  FROM marketplace_olist.silver.order_reviews

)

SELECT
  order_id,
  review_id,
  review_score,
  review_creation_date AS review_date,
  review_answer_timestamp,
  ingestion_ts
FROM ranked_reviews
WHERE rn = 1;