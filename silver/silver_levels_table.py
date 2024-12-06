# Databricks notebook source
# MAGIC %run "./funcs/df_select_columns"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.bronze.levels")
display(df)

# COMMAND ----------

final_df = selectColumns(df, ["id", "name", "rules"])
display(final_df)

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("dea_speedrun.silver.levels")
