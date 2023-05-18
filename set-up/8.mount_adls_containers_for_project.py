# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    #Get secrets from key vault
    client_id = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-client-id-key')
    tenant_id = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-tenant-id-key')
    client_secret = dbutils.secrets.get(scope='formula1dpdl1-scope', key='formula1dpdl1-client-secret-key')

    #Spark Config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
          }

    #Check if container is mounted and unmount if it is already mounted
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    #Display mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dpdl1', 'raw')
#mount_adls('formula1dpdl1', 'processed')
#mount_adls('formula1dpdl1', 'presentation')

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dpdl1/demo")

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dpdl1/demo/circuits.csv", inferSchema=True, header=True))