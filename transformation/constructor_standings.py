# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, when, count, col, max, first, desc, rank

# COMMAND ----------

# MAGIC %run "../set-up/includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

#ordered_results = constructor_standings_df.orderBy(constructor_standings_df.total_points.desc())

constructor_rank = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank))

# COMMAND ----------

#display(ordered_results.filter("race_year = 2019"))

display(final_df.filter("race_year = 2019"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

