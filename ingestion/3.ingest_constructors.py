# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

constructor_df = spark.read \
                           .schema(constructors_schema) \
                           .json("/mnt/formula1dpdl1/raw/constructors.json")                           

# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col("url"))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

#constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dpdl1/processed/constructors/")
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

