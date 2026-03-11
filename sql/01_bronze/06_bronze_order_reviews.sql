-- Databricks notebook source
-- ============================================
-- Bronze Order Reviews
-- Source: Brazilian E-Commerce Public Dataset by Olist
-- Description:
--   Raw ingestion of customers data from CSV
--   Stored as Delta table with ingestion metadata

-- ============================================

CREATE OR REPLACE TABLE marketplace_olist.bronze.order_reviews
USING DELTA
AS
SELECT *,
       current_timestamp() AS ingestion_ts,
       'kaggle' AS source_system
FROM read_files('/Volumes/marketplace_olist/default/raw_data/olist_order_reviews_dataset.csv',
  format => 'csv',
  header => true,
  inferSchema => true,
  multiLine => true, -- Allows miltiline review comments
  quote => '"',      -- Allows escaped quotes
  escape => '"'
);