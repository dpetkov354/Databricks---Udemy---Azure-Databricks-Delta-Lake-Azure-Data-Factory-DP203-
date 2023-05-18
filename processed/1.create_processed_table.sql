-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dpdl1/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

