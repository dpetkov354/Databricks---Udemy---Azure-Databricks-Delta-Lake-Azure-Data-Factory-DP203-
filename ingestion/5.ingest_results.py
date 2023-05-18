# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("driverId", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("miliseconds", IntegerType(), True),
                                    StructField("fastestlap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", StringType(), True),
                                    StructField("statusId", IntegerType(), True)      
])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

results_df = spark.read \
                       .schema(results_schema) \
                       .json("/mnt/formula1dpdl1/raw/results.json")    

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

results_dropped_df = results_df.drop(col("statusId"))

# COMMAND ----------

display(results_dropped_df)

# COMMAND ----------

results_final_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
                                     .withColumnRenamed("raceId", "race_id") \
                                     .withColumnRenamed("driverId", "driver_id") \
                                     .withColumnRenamed("constructorId", "constructor_id") \
                                     .withColumnRenamed("positionText", "position_text") \
                                     .withColumnRenamed("positionOrder", "position_order") \
                                     .withColumnRenamed("fastestLap", "fastest_lap") \
                                     .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                     .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                     .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

#results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/formula1dpdl1/processed/results/")
results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

