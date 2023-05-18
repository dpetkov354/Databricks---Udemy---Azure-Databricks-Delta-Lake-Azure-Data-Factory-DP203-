# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# drivers_schema = "driverId INT, driverRef STRING, number INT, code STRING, name STRUCT<forename STRING, surname STRING>, dob DATE, nationality STRING, url STRING"

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

drivers_df = spark.read \
                       .schema(drivers_schema) \
                       .json("/mnt/formula1dpdl1/raw/drivers.json")                           

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_dropped_df = drivers_df.drop(col("url"))

# COMMAND ----------

display(drivers_dropped_df)

# COMMAND ----------

drivers_final_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
                                             .withColumnRenamed("driverRef", "driver_ref") \
                                             .withColumn("name", concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

#drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dpdl1/processed/drivers/")
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

