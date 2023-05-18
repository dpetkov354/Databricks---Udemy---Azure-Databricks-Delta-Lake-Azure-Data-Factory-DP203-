# Databricks notebook source
from pyspark.sql.functions import date_format
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../set-up/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                     .withColumnRenamed("name", "race_name") \
                     .withColumn("race_date", date_format("race_timestamp", "dd/MM/yyyy")) \
                     .select("race_id", "circuit_id", "race_year", "race_name", "race_date")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .withColumnRenamed("location", "circuit_location") \
                        .select("circuit_id", "circuit_location")

# COMMAND ----------

races_and_circuit_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
                       .withColumnRenamed("name", "driver_name") \
                       .withColumnRenamed("number", "driver_number") \
                       .withColumnRenamed("nationality", "driver_nationality") \
                       .select("driver_id", "driver_name", "driver_number", "driver_nationality")

# COMMAND ----------

teams_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
                     .withColumnRenamed("constructor_id", "team_id") \
                     .withColumnRenamed("name", "team") \
                     .select("team_id", "team")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
                       .withColumnRenamed("constructor_id", "team_id") \
                       .withColumnRenamed("time", "race_time") \
                       .select("result_id", "race_id", "driver_id", "team_id", "grid", "fastest_lap", "race_time", "points")

# COMMAND ----------

final_results_df = results_df.join(races_df, races_df.race_id == results_df.race_id, "inner") \
                             .join(drivers_df, drivers_df.driver_id == results_df.driver_id, "inner") \
                             .join(teams_df, teams_df.team_id == results_df.team_id, "inner")

# COMMAND ----------

actual_final_results_df = final_results_df.join(circuits_df, circuits_df.circuit_id == final_results_df.circuit_id, "inner")

# COMMAND ----------

filtered_results = actual_final_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points") \
                                          .withColumn("created_date", current_timestamp())                                       

# COMMAND ----------

display(filtered_results.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'"))

# COMMAND ----------

filtered_results.write.mode("overwrite").parquet("/mnt/formula1dpdl1/presentation/race_results")