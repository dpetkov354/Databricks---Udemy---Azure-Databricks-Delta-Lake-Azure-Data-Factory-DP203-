# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dpdl1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dpdl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dpdl1.dfs.core.windows.net", "sp=rl&st=2023-05-09T09:17:55Z&se=2023-05-09T17:17:55Z&spr=https&sv=2022-11-02&sr=c&sig=h1wYXbJi%2FE6NAtWglPSXgjhnbRc82qBGeYckuLfKRRc%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dpdl1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dpdl1.dfs.core.windows.net/circuits.csv", inferSchema=True, header=True))

# COMMAND ----------

