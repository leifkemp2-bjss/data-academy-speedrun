# Databricks notebook source
dbutils.fs.ls('abfss://metastore@deaspeedrunucextdl.dfs.core.windows.net/')


# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP CATALOG IF EXISTS dea_speedrun CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dea_speedrun
# MAGIC MANAGED LOCATION 'abfss://metastore@deaspeedrunucextdl.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG dea_speedrun TO `leif.kemp2@bjss.com`;
# MAGIC GRANT ALL PRIVILEGES ON CATALOG dea_speedrun TO `stephen.mccoy@bjssacademy.onmicrosoft.com`;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dea_speedrun

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP SCHEMA IF EXISTS bronze CASCADE;
# MAGIC -- DROP SCHEMA IF EXISTS silver CASCADE;
# MAGIC -- DROP SCHEMA IF EXISTS gold CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES IN dea_speedrun

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dea_speedrun.bronze.raw_data
