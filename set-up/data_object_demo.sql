-- Databricks notebook source
CREATE DATABASE demo;

-- COMMAND ----------

 CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

-- MAGIC %run "../set-up/includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

CREATE OR REPLACE TABLE race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2019;

-- COMMAND ----------

DROP TABLE race_results_sql;

-- COMMAND ----------

