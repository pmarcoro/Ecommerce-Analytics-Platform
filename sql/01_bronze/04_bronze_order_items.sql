-- Databricks notebook source
-- ============================================
-- Bronze Order Items
-- Source: Brazilian E-Commerce Public Dataset by Olist
-- Description:
--   Raw ingestion of customers data from CSV
--   Stored as Delta table with ingestion metadata
-- ============================================

CREATE TABLE IF NOT EXISTS marketplace_olist.bronze.order_items
USING DELTA
AS
SELECT *,
       current_timestamp() AS ingestion_ts,
       'kaggle' AS source_system
FROM read_files('/Volumes/marketplace_olist/default/raw_data/olist_order_items_dataset.csv',
  format => 'csv',
  header => true
);
