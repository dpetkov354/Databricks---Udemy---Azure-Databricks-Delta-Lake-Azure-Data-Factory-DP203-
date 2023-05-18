# Databricks notebook source
# MAGIC %run "../set-up/includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results/")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

race_results_sql_df = spark.sql("SELECT * FROM v_race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

