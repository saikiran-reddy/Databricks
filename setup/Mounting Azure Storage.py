# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting Azure Data Lake
# MAGIC * Configs
# MAGIC * Mount function

# COMMAND ----------

def mount_adls(container_name, storage_account_name, scope_name):
    
    """
    Mounting azure container to databricks.
    
    Parameters: 
        container_name (string): Azure storage container name.
        
    """
    
    client_id = dbutils.secrets.get(scope=scope_name, key='databricks-app-client-id')
    tenant_id = dbutils.secrets.get(scope=scope_name, key='databricks-app-tenant-id')
    client_secret = dbutils.secrets.get(scope=scope_name, key='databricks-app-client-secret')
    
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": f"{client_id}",
              "fs.azure.account.oauth2.client.secret": f"{client_secret}",
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
        
   

# COMMAND ----------

#mount_adls(container_name='processing', storage_account_name='appdatabricks', scope_name = 'formula1-scope')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------


