# Databricks notebook source
# MAGIC %run "./funcs/df_select_columns"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.bronze.platforms")
display(df)

# COMMAND ----------

final_df = selectColumns(df, ["id", "name"])
display(final_df)

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("dea_speedrun.silver.platforms")
