# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

# MAGIC %run "../set-up/includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, max, first, desc

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

#ordered_results = driver_standings_df.orderBy(driver_standings_df.total_points.desc())

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#display(ordered_results.filter("race_year = 2013"))

# COMMAND ----------

max_wins = driver_standings_df.select(max('wins')).first()[0]
rows_with_max_wins = driver_standings_df.filter(col('wins') == max_wins)
result = rows_with_max_wins.collect()
display(result)

# COMMAND ----------

max_points = driver_standings_df.select(max('total_points')).first()[0]
rows_with_max_points = driver_standings_df.filter(col('total_points') == max_points)
total_points_result = rows_with_max_points.collect()
display(total_points_result)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").parquet("/mnt/formula1dpdl1/presentation/driver_standings/")

# COMMAND ----------

