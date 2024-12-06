# Databricks notebook source
# MAGIC %run "./funcs/games_select"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.bronze.games")
display(df)

# COMMAND ----------

final_df = selectGameColumns(df)
display(final_df)

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("dea_speedrun.silver.games")
