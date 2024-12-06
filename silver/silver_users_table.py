# Databricks notebook source
# MAGIC %run "./funcs/users_select"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.bronze.users")
display(df)

# COMMAND ----------

final_df = selectUserColumns(df)
display(final_df)

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("dea_speedrun.silver.users")
