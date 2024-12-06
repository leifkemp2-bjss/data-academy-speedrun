# Databricks notebook source
# MAGIC %run "./extract_data_from_raw"

# COMMAND ----------

dbutils.widgets.dropdown("folder", "categories_data", ["categories_data", "games_data", "levels_data", "platforms_data", "regions_data", "runs_data", "users_data"])

# COMMAND ----------

folder = dbutils.widgets.get("folder")

if folder not in ["categories_data", "games_data", "levels_data", "platforms_data", "regions_data", "runs_data", "users_data"]:
    raise Exception("Invalid folder parameter, must be one of: categories_data, games_data, levels_data, platforms_data, regions_data, runs_data, users_data")

# COMMAND ----------

df = spark.read.format("json").load(f"/Volumes/dea_speedrun/bronze/raw_data/runs_data/runs_allverified.json", multiLine=True)
# display(df) 

final_df = extractDataFromRawDF(df)
display(final_df.select("*").limit(10))

final_df.printSchema()

# COMMAND ----------

df = spark.read.format("json").load(f"/Volumes/dea_speedrun/bronze/raw_data/{folder}/*.json", multiLine=True)
# display(df) 

final_df = extractDataFromRawDF(df)
display(final_df.select("*").limit(10))

final_df.printSchema()

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.bronze.{folder.replace('_data', '')}")
