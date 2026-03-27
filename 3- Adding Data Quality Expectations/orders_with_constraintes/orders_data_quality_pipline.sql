-- Databricks notebook source
CREATE OR REFRESH STREAMING  TABLE 1_bronze_db.orders_bronze_demo3 AS 
SELECT *, current_timestamp() AS processing_time, _metadata.file_name AS source_file
FROM STREAM read_files(
  "/Volumes/workspace/monschema1/my_volume/orders_constraints", 
  format => 'json',
  schema => 'customer_id INT, order_id BIGINT, notification STRING, order_timestamp TIMESTAMP'
);

CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo3 

(
  CONSTRAINT valid_notification EXPECT (notification = 'Y') ,
  CONSTRAINT valid_date EXPECT (order_timestamp > '2025-07-22') ON VIOLATION DROP ROW

)

AS SELECT order_id, timestamp(order_timestamp) AS order_timestamp,customer_id, notification from STREAM  1_bronze_db.orders_bronze_demo3;




CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date_demo3
AS
SELECT 
date(order_timestamp) AS order_date, 
count(*) AS total_daily_orders
FROM  2_silver_db.orders_silver_demo3
GROUP BY date(order_timestamp);
