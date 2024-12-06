# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.deaspeedrun.dfs.core.windows.net", dbutils.secrets.get(scope = "dea-speedrun-scope", key = "deaspeedrun-access-key")
)
