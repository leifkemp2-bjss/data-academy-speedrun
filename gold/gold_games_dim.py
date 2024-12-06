# Databricks notebook source
# MAGIC %run "./funcs/create_games_df"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.silver.games")
display(df)

# COMMAND ----------

games_dim_df = createGamesDF(df)
display(games_dim_df)

# COMMAND ----------

games_dim_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.games_dim")
