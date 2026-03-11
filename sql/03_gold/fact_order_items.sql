-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_order_items
USING DELTA
AS

SELECT
    oi.order_id,
    oi.order_item_id,

    -- Foreign keys
    o.customer_id AS customer_key,
    oi.product_id AS product_key,
    oi.seller_id  AS seller_key,

    -- Financial metrics (item level)
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_amount,

    CAST(
        oi.freight_value / NULLIF((oi.price + oi.freight_value), 0)
        AS DECIMAL(10,2)
    ) AS freight_ratio,

    -- Date
    o.order_purchase_timestamp

FROM marketplace_olist.silver.order_items oi

LEFT JOIN marketplace_olist.silver.orders o
    ON oi.order_id = o.order_id;