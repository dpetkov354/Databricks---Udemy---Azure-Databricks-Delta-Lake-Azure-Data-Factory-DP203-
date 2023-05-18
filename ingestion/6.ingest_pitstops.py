# Databricks notebook source
from pyspark.sql.functions import current_timestamp, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", StringType(), True)
])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

pitstops_df = spark.read \
                        .schema(pitstops_schema) \
                        .option("multiline", True) \
                        .json("/mnt/formula1dpdl1/raw/pit_stops.json")    

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

pitstops_df.printSchema()

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumn('time', date_format('time', 'HH:mm:ss')) \
                               .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(pitstops_final_df)

# COMMAND ----------

#pitstops_final_df.write.mode("overwrite").parquet("/mnt/formula1dpdl1/processed/pitstops/")
pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

