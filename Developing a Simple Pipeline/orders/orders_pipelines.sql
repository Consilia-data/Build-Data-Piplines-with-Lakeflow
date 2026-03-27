-- Databricks notebook source
--DROP TABLE IF EXISTS 1_bronze_db.orders_bronze_demo2;

CREATE OR REFRESH STREAMING  TABLE 1_bronze_db.orders_bronze_demo2 AS 
SELECT *, current_timestamp() AS processing_time, _metadata.file_name AS source_file
FROM STREAM read_files(
  "/Volumes/workspace/monschema1/my_volume/orders", 
  format => 'json',
  schema => 'customer_id INT, order_id INT, notification STRING, order_timestamp TIMESTAMP'
);

-- B create silver tabe streaming
CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo2
AS SELECT order_id, 
timestamp(order_timestamp) AS order_timestamp,
customer_id, 
notification FROM STREAM 1_bronze_db.orders_bronze_demo2;


CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.gold_orders_by_date_demo2
AS select date(order_timestamp) as order_date,count(*) as total_daily_orders from 2_silver_db.orders_silver_demo2 group by date(order_timestamp); 
 
