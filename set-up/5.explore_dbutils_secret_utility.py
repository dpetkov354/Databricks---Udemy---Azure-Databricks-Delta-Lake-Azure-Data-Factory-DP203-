# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1dpdl1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-account-key')

# COMMAND ----------

