# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import current_timestamp, col, to_timestamp, concat, lit

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dpdl1/raw

# COMMAND ----------

races_initial_df = spark.read.csv("/mnt/formula1dpdl1/raw/races.csv")

# COMMAND ----------

display(races_initial_df)

# COMMAND ----------

race_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType(), False),
                                    StructField("circuitId", IntegerType(), False),
                                    StructField("name", StringType(), True),
                                    StructField("date", DateType(), False),
                                    StructField("time", StringType(), False),
                                    StructField("url", StringType(), False)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(race_schema) \
.csv("/mnt/formula1dpdl1/raw/races.csv")

# COMMAND ----------

type(races_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

races_formated_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                            .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_formated_df)

# COMMAND ----------

races_renamed_df = races_formated_df.withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("year", "race_year") \
                                          .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

races_final_df = races_renamed_df.select(races_renamed_df["race_id"],
                                         races_renamed_df["race_year"],
                                         races_renamed_df["round"],
                                         races_renamed_df["circuit_id"],
                                         races_renamed_df["name"],
                                         races_renamed_df["race_timestamp"],
                                         races_renamed_df["ingestion_date"])

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

#races_final_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1dpdl1/processed/races/")
races_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

#%fs
#ls /mnt/formula1dpdl1/processed/races/

# COMMAND ----------

#df = spark.read.parquet("/mnt/formula1dpdl1/processed/races/")

# COMMAND ----------

#display(df)