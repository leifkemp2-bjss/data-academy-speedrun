# Databricks notebook source
# MAGIC %run "./funcs/create_levels_df"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.silver.levels")
display(df)

# COMMAND ----------

levels_dim_df = createLevelsDF(df)
display(levels_dim_df)

# COMMAND ----------

levels_dim_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.levels_dim")
