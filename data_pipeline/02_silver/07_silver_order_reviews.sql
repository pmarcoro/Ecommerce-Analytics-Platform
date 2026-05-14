-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.silver.order_reviews
USING DELTA
AS
SELECT
  CAST(TRIM(order_id) AS STRING) AS order_id,
  CAST(TRIM(review_id) AS STRING) AS review_id,
  CAST(review_score AS INT) AS review_score,
  CAST(TRIM(review_comment_title) AS STRING) AS review_comment_title,
  CAST(TRIM(review_comment_message)AS STRING) AS review_comment_message,
  review_creation_date,
  review_answer_timestamp,
  ingestion_ts
FROM
  marketplace_olist.bronze.order_reviews


-- Check for nulls in keys
%sql
SELECT *
FROM marketplace_olist.silver.order_reviews
WHERE review_id IS NULL
   OR order_id IS NULL
   OR review_score IS NULL

-- Check all r.order_id match with orders table
%sql
SELECT r.order_id
FROM marketplace_olist.silver.order_reviews r
LEFT JOIN marketplace_olist.silver.orders o
ON r.order_id = o.order_id
WHERE o.order_id IS NULL

