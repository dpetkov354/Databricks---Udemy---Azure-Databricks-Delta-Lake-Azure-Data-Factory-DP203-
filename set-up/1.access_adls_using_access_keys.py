# Databricks notebook source
formula1dpdl1_account_key = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dpdl1.dfs.core.windows.net", formula1dpdl1_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dpdl1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dpdl1.dfs.core.windows.net/circuits.csv", inferSchema=True, header=True))

# COMMAND ----------

