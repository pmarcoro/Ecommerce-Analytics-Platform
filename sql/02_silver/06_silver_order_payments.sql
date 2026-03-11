-- Databricks notebook source
-- DBTITLE 1,Cell 1
CREATE OR REPLACE TABLE marketplace_olist.silver.order_payments
USING DELTA
AS
SELECT
  CAST(TRIM(order_id) AS STRING) AS order_id,
  payment_sequential,
  CAST(TRIM(payment_type) AS STRING) AS payment_type,
  CAST(TRIM(payment_installments) AS INT) AS payment_installments,
  CAST(TRIM(payment_value) AS DECIMAL(10,2)) AS payment_value
FROM
  marketplace_olist.bronze.order_payments

