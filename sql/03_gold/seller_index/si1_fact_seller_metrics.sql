-- Databricks notebook source
CREATE OR REPLACE TABLE seller_daily_orders AS

WITH seller_orders AS (

  SELECT DISTINCT
      oi.seller_key,
      o.order_id,
      DATE(o.order_purchase_timestamp) AS order_date,
      o.late_delivery_flag,
      o.order_status

  FROM marketplace_olist.gold.fact_orders o
  JOIN marketplace_olist.gold.fact_order_items oi
      ON o.order_id = oi.order_id

)

SELECT
    seller_key,
    order_date,
    COUNT(order_id) AS orders,

    SUM(CASE WHEN late_delivery_flag = 1 THEN 1 ELSE 0 END)
        AS late_shipments,

    SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END)
        AS cancelled_orders

FROM seller_orders
GROUP BY seller_key, order_date;

-- COMMAND ----------

CREATE OR REPLACE TABLE calendar AS
SELECT
  EXPLODE(
    SEQUENCE(
      MIN(do.order_date),
      MAX(do.order_date),
      interval 1 day
    )
  ) AS reference_date
FROM seller_daily_orders do;

-- COMMAND ----------

CREATE OR REPLACE TABLE seller_calendar AS
SELECT
  s.seller_key,
  c.reference_date
FROM (SELECT DISTINCT seller_key FROM marketplace_olist.gold.fact_order_items) s -- Maybe I should avoid renaming select_id in fact_order_items
  CROSS JOIN calendar c;

-- COMMAND ----------

CREATE OR REPLACE TABLE latest_review_per_order AS

WITH first_review AS (

    SELECT
        r.seller_key,
        r.order_id,
        MIN(r.review_creation_date) AS first_review_date
    
    FROM marketplace_olist.gold.fact_reviews r
    GROUP BY seller_key, order_id
),

last_score AS (

    SELECT
        r.seller_key,
        r.order_id,
        r.review_score,
        r.review_creation_date,
        ROW_NUMBER() OVER (
            PARTITION BY r.seller_key, r.order_id
            ORDER BY r.review_creation_date DESC
        ) AS rn
    FROM marketplace_olist.gold.fact_reviews r
)

SELECT
    f.seller_key,
    f.order_id,
    f.first_review_date AS review_date,
    l.review_score
FROM first_review f
JOIN last_score l
    ON f.seller_key = l.seller_key
    AND f.order_id = l.order_id
WHERE l.rn = 1
;

-- COMMAND ----------

CREATE OR REPLACE TABLE reviews_daily AS

SELECT
    seller_key,
    review_date,
    COUNT(*) AS reviews_day,
    SUM(CASE WHEN review_score < 3 THEN 1 ELSE 0 END) AS claims_day,
    SUM(review_score) AS sum_score_day
FROM latest_review_per_order
GROUP BY seller_key, review_date
;

-- COMMAND ----------

-- DBTITLE 1,Fix PARSE_SYNTAX_ERROR in orders_reviews_daily
CREATE OR REPLACE TABLE orders_reviews_daily AS

WITH base_calendar AS (
    SELECT
        sc.seller_key,
        sc.reference_date,
        o.orders,
        o.late_shipments,
        o.cancelled_orders
    FROM seller_calendar sc
    LEFT JOIN seller_daily_orders o
        ON sc.seller_key = o.seller_key
        AND sc.reference_date = o.order_date
),

reviews_calendar AS (
    SELECT
        sc.seller_key,
        sc.reference_date,
        COALESCE(rd.reviews_day,0) AS reviews_day,
        COALESCE(rd.claims_day,0) AS claims_day,
        COALESCE(rd.sum_score_day,0) AS sum_score_day
    FROM seller_calendar sc
    LEFT JOIN reviews_daily rd
        ON sc.seller_key = rd.seller_key
        AND sc.reference_date = rd.review_date
),

combined_base AS (
    SELECT
      bc.seller_key,
      bc.reference_date,
      bc.orders,
      bc.late_shipments,
      bc.cancelled_orders,
      rc.reviews_day,
      rc.claims_day,
      rc.sum_score_day
    FROM base_calendar bc
    LEFT JOIN reviews_calendar rc
      ON bc.seller_key = rc.seller_key
      AND bc.reference_date = rc.reference_date
)

SELECT * FROM combined_base


-- COMMAND ----------

CREATE OR REPLACE TABLE marketplace_olist.gold.fact_seller_metrics 
USING DELTA
AS

WITH metrics_base AS (
SELECT
    seller_key,
    reference_date,

    -- operational metrics over 30d period

    SUM(COALESCE(orders,0)) OVER (
        PARTITION BY seller_key
        ORDER BY reference_date
        RANGE BETWEEN INTERVAL 32 DAYS PRECEDING
        AND INTERVAL 2 DAYS PRECEDING
    ) AS orders_30d,

    SUM(COALESCE(late_shipments,0)) OVER (
        PARTITION BY seller_key
        ORDER BY reference_date
        RANGE BETWEEN INTERVAL 32 DAYS PRECEDING
        AND INTERVAL 2 DAYS PRECEDING
    ) AS late_shipments_30d,

    SUM(COALESCE(cancelled_orders,0)) OVER (
        PARTITION BY seller_key
        ORDER BY reference_date
        RANGE BETWEEN INTERVAL 32 DAYS PRECEDING
        AND INTERVAL 2 DAYS PRECEDING
    ) AS cancellations_30d,

    --  review metrics over 60d period

    SUM(reviews_day) OVER (
        PARTITION BY seller_key
        ORDER BY reference_date
        RANGE BETWEEN INTERVAL 62 DAYS PRECEDING
        AND INTERVAL 2 DAYS PRECEDING
    ) AS reviews_60d,

    SUM(claims_day) OVER (
        PARTITION BY seller_key
        ORDER BY reference_date
        RANGE BETWEEN INTERVAL 62 DAYS PRECEDING
        AND INTERVAL 2 DAYS PRECEDING
    ) AS claims_60d,

    SUM(sum_score_day) OVER (
        PARTITION BY seller_key
        ORDER BY reference_date
        RANGE BETWEEN INTERVAL 62 DAYS PRECEDING
        AND INTERVAL 2 DAYS PRECEDING
    )
    /
    NULLIF(
        SUM(reviews_day) OVER (
            PARTITION BY seller_key
            ORDER BY reference_date
            RANGE BETWEEN INTERVAL 62 DAYS PRECEDING
            AND INTERVAL 2 DAYS PRECEDING
        ),0
    ) AS avg_review_score_60d

FROM orders_reviews_daily
)

SELECT
  *,
  late_shipments_30d / NULLIF(orders_30d,0) AS late_rate,
  cancellations_30d / NULLIF(orders_30d,0) AS cancel_rate,
  claims_60d / NULLIF(reviews_60d,0) AS claim_rate

FROM metrics_base
WHERE orders_30d > 0 OR
  reviews_60d > 0
;