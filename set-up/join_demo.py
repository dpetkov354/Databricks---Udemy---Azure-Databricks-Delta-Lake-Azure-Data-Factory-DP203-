# Databricks notebook source
# MAGIC %run "../set-up/includes/configuration"

# COMMAND ----------

circuit_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                       .withColumnRenamed("name", "circuit_name") \
                       .filter("circuit_id < 70")
display(circuit_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
                     .withColumnRenamed("name", "race_name")
display(races_df)

# COMMAND ----------

race_circuits_df = circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "inner") \
                             .select(circuit_df.circuit_name, circuit_df.location, circuit_df.race_country, races_df.race_name, races_df.round)
display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "left") \
                             .select(circuit_df.circuit_name, circuit_df.location, circuit_df.race_country, races_df.race_name, races_df.round)
display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "right") \
                             .select(circuit_df.circuit_name, circuit_df.location, circuit_df.race_country, races_df.race_name, races_df.round)
display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuit_df.join(races_df, circuit_df.circuit_id == races_df.circuit_id, "anti")
display(race_circuits_df)

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuit_df)
display(race_circuits_df)

# COMMAND ----------

