# Databricks notebook source
dbutils.fs.ls("abfss://demo@formula1dpdl1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dpdl1.dfs.core.windows.net/circuits.csv", inferSchema=True, header=True))