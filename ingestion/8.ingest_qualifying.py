# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("/mnt/formula1dpdl1/raw/qualifying/qualifying_split_*.json")

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualifying_id") \
                        .withColumnRenamed("raceId", "race_id") \
                        .withColumnRenamed("driverId", "driver_id") \
                        .withColumnRenamed("constructorId", "constructor_id") \
                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

#final_df.write.mode("overwrite").parquet("/mnt/formula1dl/processed/qualifying")
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

