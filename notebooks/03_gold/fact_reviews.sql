-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_reviews AS

WITH base_reviews AS (

    SELECT
        oi.seller_key,
        r.order_id,
        r.review_id,
        r.review_score,
        r.review_creation_date,
        r.review_answer_timestamp,

        ROW_NUMBER() OVER (
            PARTITION BY r.order_id
            ORDER BY r.review_creation_date ASC
        ) AS review_version_number,

        ROW_NUMBER() OVER (
            PARTITION BY r.order_id
            ORDER BY r.review_creation_date DESC
        ) AS rn_desc

    FROM marketplace_olist.silver.order_reviews r
    JOIN marketplace_olist.gold.fact_order_items oi
        ON r.order_id = oi.order_id

    WHERE r.review_score IS NOT NULL

)

SELECT
    seller_key,
    order_id,
    review_id,
    review_creation_date,
    review_score,
    review_version_number,
    CASE WHEN review_version_number = 1 THEN TRUE ELSE FALSE END AS is_first_review,
    CASE WHEN rn_desc = 1 THEN TRUE ELSE FALSE END AS is_latest_absolute

FROM base_reviews;

FROM marketplace_olist.silver.order_reviews;