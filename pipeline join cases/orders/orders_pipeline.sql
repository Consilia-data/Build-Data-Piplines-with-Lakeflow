-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze_demo5
 COMMENT "Ingest order JSON files from cloud storage"
 TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.reset.allowed"= false
 )
AS 
SELECT *, current_timestamp () AS processing_time, _metadata.file_name AS source_file FROM STREAM read_files("/Volumes/workspace/monschema1/my_volume/order_status", format => "json", schema => 'customer_id INT, order_id BIGINT, notification STRING, order_timestamp TIMESTAMP');

CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo5
(
  CONSTRAINT valid_notifications EXPECT (notification IN ('Y','N')),
  CONSTRAINT valide_date EXPECT (order_timestamp > "2025-07-20T12:00:00") ON VIOLATION DROP ROW,
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
) 
COMMENT "Silver clean orders table"
AS
SELECT order_id,
timestamp(order_timestamp) AS order_timestamp,
customer_id,
notification
FROM STREAM 1_bronze_db.orders_bronze_demo5;

CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date_demo5
COMMENT "Gold orders table"
TBLPROPERTIES ("quality" = "gold")

AS 
SELECT 
  date(order_timestamp) AS order_date,
  count(*) AS total_daily_orders
FROM 2_silver_db.orders_silver_demo5
GROUP BY DATE (order_timestamp);
