-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.status_bronze_demo5
 COMMENT "Ingest status JSON files from cloud storage"
 TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.reset.allowed"= false
 )
AS 
SELECT *, current_timestamp () AS processing_time, _metadata.file_name AS source_file FROM STREAM read_files("/Volumes/workspace/monschema1/my_volume/status", format => "json", schema => 'order_id BIGINT, order_status string, status_timestamp TIMESTAMP')



-- COMMAND ----------


CREATE OR REFRESH STREAMING TABLE 2_silver_db.status_silver_demo5
(
  CONSTRAINT valid_timestamp EXPECT (status_timestamp > "2025-07-20T14:00:00") ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_status EXPECT (order_status  IN ('on_the_way','placed','delivered')) 
) 
COMMENT "Order with each status and timestamp"
TBLPROPERTIES (
  "quality" = "silver")
AS
SELECT order_id,
order_status,
timestamp(status_timestamp) AS status_timestamp
FROM STREAM 1_bronze_db.status_bronze_demo5;


-- COMMAND ----------


CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.full_order_info_demo5
COMMENT "Joining orders and status"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
orders.order_id,
orders.order_timestamp,
status.order_status,
status.status_timestamp
FROM  2_silver_db.status_silver_demo5 status
INNER JOIN 2_silver_db.orders_silver_demo5 orders
ON  status.order_id=orders.order_id ;
