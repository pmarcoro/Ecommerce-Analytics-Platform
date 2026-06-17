-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.order_payments
USING DELTA
AS
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM marketplace_olist.silver.order_payments;