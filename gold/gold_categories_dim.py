# Databricks notebook source
# MAGIC %run "./funcs/create_categories_df"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.silver.categories")
display(df)

# COMMAND ----------

categories_dim_df = createCategoriesDF(df)
display(categories_dim_df)

# COMMAND ----------

categories_dim_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.categories_dim")
