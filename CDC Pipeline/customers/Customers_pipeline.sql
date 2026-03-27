-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE 1_bronze_db.customers_bronze_raw_demo6
 COMMENT "Raw data from customers CDC feed"
 TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.reset.allowed" = false
)
AS 
SELECT 
*, 
_metadata.file_name AS source_file 
FROM STREAM read_files("/Volumes/workspace/monschema1/my_volume/customers", format=>"json", schema=>'address string, city string, customer_id Int, email string, name string, operation string, state string, process_time BIGINT, zip_code string');

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE 1_bronze_db.customers_bronze_clean_demo6
(
CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation="DELETE") ,
CONSTRAINT valid_address EXPECT (address IS NOT NULL and city is not NULL and state is not NULL and zip_code is not NULL),
CONSTRAINT valid_email EXPECT (rlike( email, '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$') OR operation="DELETE")ON VIOLATION DROP ROW
)
COMMENT "Clean BRONZE raw table "
AS SELECT * FROM STREAM 1_bronze_db.customers_bronze_raw_demo6;


-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE 2_silver_db.customers_silver_demo6
COMMENT "SCD Type 2 Silver table";

APPLY CHANGES INTO  2_silver_db.customers_silver_demo6
FROM STREAM 1_bronze_db.customers_bronze_clean_demo6
KEYS(customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY process_time
COLUMNS * EXCEPT (operation)
STORED AS SCD type 2;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.current_customers_demo6
COMMENT "Current active customers"
AS SELECT *, current_timestamp() updated_at FROM 2_silver_db.customers_silver_demo6 WHERE `__END_AT` IS NULL; 
