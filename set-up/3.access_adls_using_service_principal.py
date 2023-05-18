# Databricks notebook source
client_id = "7eca48f3-39d5-439d-a59f-c56d9e3131a8"
tenant_id = "93f33571-550f-43cf-b09f-cd331338d086"
client_secret = "tXk8Q~AGp7vbfjjcrxKmaWyw8w4MB6DJTryr2aPk"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dpdl1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dpdl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dpdl1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dpdl1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dpdl1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dpdl1.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dpdl1.dfs.core.windows.net/circuits.csv", inferSchema=True, header=True))

# COMMAND ----------

