-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.fact_logistics
USING DELTA
AS

SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.seller_id,

    -- Order timestamps
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- Late carrier delivery flag
    CASE
        WHEN o.order_delivered_carrier_date IS NOT NULL
             AND oi.shipping_limit_date IS NOT NULL
             AND oi.shipping_limit_date < o.order_delivered_carrier_date
        THEN 1 ELSE 0
    END AS late_delivered_carrier,

    -- Late customer delivery flag
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
             AND o.order_estimated_delivery_date IS NOT NULL
             AND o.order_estimated_delivery_date < o.order_delivered_customer_date
        THEN 1 ELSE 0
    END AS late_delivered_customer

FROM marketplace_olist.silver.order_items oi

JOIN marketplace_olist.silver.orders o
    ON oi.order_id = o.order_id;