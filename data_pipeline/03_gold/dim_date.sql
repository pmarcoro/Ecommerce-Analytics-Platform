-- Databricks notebook source
CREATE OR REPLACE TABLE marketplace_olist.gold.dim_date
USING DELTA
AS

WITH CALENDAR AS (
    SELECT EXPLODE(
                SEQUENCE(DATE'2016-01-01', DATE'2017-12-31', INTERVAL 1 DAY)
                ) AS date_key
)

SELECT 
    date_key,
    -- Date Hierarchy
    year(date_key) AS year,
    quarter(date_key) AS quarter,
    month(date_key) AS month,
    day(date_key) AS day,

    -- Month names
    CASE month(date_key)
        WHEN  1 THEN 'Jan' WHEN  2 THEN 'Feb' WHEN  3 THEN 'Mar'
        WHEN  4 THEN 'Apr' WHEN  5 THEN 'May' WHEN  6 THEN 'Jun'
        WHEN  7 THEN 'Jul' WHEN  8 THEN 'Aug' WHEN  9 THEN 'Sep'
        WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
    END AS month_name,

    -- Facilitate sorting by column in Power BI
    date_format(date_key, 'yyyy-MM')                    AS year_month,
    year(date_key) * 100 + month(date_key)              AS year_month_sort,

    CONCAT(year(date_key), '-Q', quarter(date_key))     AS year_quarter,
    year(date_key) * 10  + quarter(date_key)            AS year_quarter_sort,

    -- Period - Days
    weekday(date_key) + 1                               AS day_of_week_sort,

    CASE weekday(date_key)
        WHEN 0 THEN 'Mon' WHEN 1 THEN 'Tue' WHEN 2 THEN 'Wed'
        WHEN 3 THEN 'Thu' WHEN 4 THEN 'Fri' WHEN 5 THEN 'Sat'
        WHEN 6 THEN 'Sun'
    END                                                 AS day_name,
 
    CASE 
        WHEN weekday(date_key) >= 5 THEN TRUE ELSE FALSE 
    END AS is_weekend,

    -- Period boundaries (for grouping in visuals)
    CAST(date_trunc('MONTH',   date_key) AS DATE)       AS month_start,
    CAST(date_trunc('QUARTER', date_key) AS DATE)       AS quarter_start,
    CAST(date_trunc('YEAR',    date_key) AS DATE)       AS year_start,
    last_day(date_key)                                  AS month_end,
    dayofyear(date_key)                                 AS day_of_year

FROM CALENDAR