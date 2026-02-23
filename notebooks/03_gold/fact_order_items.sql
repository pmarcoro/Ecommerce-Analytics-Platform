-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_order_items
USING DELTA
AS

WITH base AS (
    SELECT
        oi.order_id,
        oi.order_item_id,

        -- Foreign keys
        o.customer_id AS customer_key,
        oi.product_id AS product_key,
        oi.seller_id  AS seller_key,

        -- Metrics,
        o.order_status,
        oi.price,
        oi.freight_value,
        (oi.price + oi.freight_value) AS total_amount,

        -- Dates
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,

        -- Geography
        s.seller_zip_code_prefix,
        s.seller_state,
        c.customer_zip_code_prefix,
        c.customer_state,

        -- Window metric
        COUNT(oi.order_item_id) OVER (PARTITION BY oi.order_id) AS total_items

    FROM marketplace_olist.silver.order_items oi

    LEFT JOIN marketplace_olist.silver.orders o
        ON oi.order_id = o.order_id

    LEFT JOIN marketplace_olist.silver.sellers s
        ON oi.seller_id = s.seller_id

    LEFT JOIN marketplace_olist.silver.customers c
        ON o.customer_id = c.customer_id
)

SELECT
    order_id,
    order_item_id,

    -- Foreign keys
    customer_key,
    product_key,
    seller_key,

    -- Metrics
    price,
    freight_value,
    total_amount,

    -- Delivery KPI
    order_status,
    DATEDIFF(order_delivered_customer_date,
             order_purchase_timestamp) AS delivery_days,

    -- New columns
    seller_zip_code_prefix <> customer_zip_code_prefix AS is_different_zip_code,
    seller_state <> customer_state AS is_interstate,

    CAST(
        freight_value / NULLIF(total_amount, 0)
        AS DECIMAL(10,2)
    ) AS freight_ratio,

    CAST(total_items AS INT) AS total_order_items,
    total_items > 1 AS is_multi_product,

    -- Date
    order_purchase_timestamp

FROM base;