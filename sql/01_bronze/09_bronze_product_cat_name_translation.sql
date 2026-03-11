-- Databricks notebook source
-- ============================================
-- Bronze Product Category Name Translation
-- Source: Brazilian E-Commerce Public Dataset by Olist
-- Description:
--   Raw ingestion of customers data from CSV
--   Stored as Delta table with ingestion metadata
-- ============================================

CREATE TABLE IF NOT EXISTS marketplace_olist.bronze.product_cat_name_translation
USING DELTA
AS
SELECT *,
       current_timestamp() AS ingestion_ts,
       'kaggle' AS source_system
FROM read_files('/Volumes/marketplace_olist/default/raw_data/product_category_name_translation.csv',
  format => 'csv',
  header => true
);
