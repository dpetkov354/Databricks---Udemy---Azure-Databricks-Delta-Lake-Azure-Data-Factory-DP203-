# Databricks notebook source
client_id = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-client-id-key')
tenant_id = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-tenant-id-key')
client_secret = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-client-secret-key')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
          }

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dpdl1.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dpdl1/demo",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dpdl1/demo")

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dpdl1/demo/circuits.csv", inferSchema=True, header=True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dpdl1/demo")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dpdl1.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dpdl1/demo",
  extra_configs = configs)