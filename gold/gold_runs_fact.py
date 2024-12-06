# Databricks notebook source
# imports
from pyspark.sql.functions import col, lit, dayofmonth, month, year
from pyspark.sql.types import IntegerType

# COMMAND ----------

runs_silver_df = spark.read.format("delta").table("dea_speedrun.silver.runs")

categories_dim_df = spark.read.format("delta").table("dea_speedrun.gold.categories_dim")
dates_dim_df = spark.read.format("delta").table("dea_speedrun.gold.dates_dim")
games_dim_df = spark.read.format("delta").table("dea_speedrun.gold.games_dim")
levels_dim_df = spark.read.format("delta").table("dea_speedrun.gold.levels_dim")
guests_dim_df = spark.read.format("delta").table("dea_speedrun.gold.guests_dim")
users_dim_df = spark.read.format("delta").table("dea_speedrun.gold.users_dim")
reviewers_dim_df = spark.read.format("delta").table("dea_speedrun.gold.reviewers_dim")
teams_dim_df = spark.read.format("delta").table("dea_speedrun.gold.teams_dim")
teams_bridge_df = spark.read.format("delta").table("dea_speedrun.gold.teams_bridge")

# COMMAND ----------

display(runs_silver_df)

# COMMAND ----------

# Attach every SK to the runs, except teams - as attaching the teams SK is going to be more complicated
runs_initial_df = runs_silver_df.join(games_dim_df, runs_silver_df.game == games_dim_df.id, "left") \
                                .join(categories_dim_df, runs_silver_df.category == categories_dim_df.id, "left") \
                                .join(reviewers_dim_df, runs_silver_df.reviewer == reviewers_dim_df.id, "left") \
                                .join(dates_dim_df, 
                                      (dayofmonth(runs_silver_df.submitted_date) == dates_dim_df.day) & 
                                      (month(runs_silver_df.submitted_date) == dates_dim_df.month) & 
                                      (year(runs_silver_df.submitted_date) == dates_dim_df.year), "left") \
                                .join(levels_dim_df, runs_silver_df.level == levels_dim_df.id, "left") \
                                .select("game_sk", "level_sk", "category_sk", "reviewer_sk", "date_sk", "players", col("time").alias("primary_time"), "status", "status_comment")
                                

display(runs_initial_df)

# COMMAND ----------

runs_teams_df = runs_initial_df.join(teams_dim_df, runs_initial_df.players == teams_dim_df.players, "left").distinct().drop("players") \
                                .select("game_sk", "level_sk", "category_sk", "reviewer_sk", "date_sk", "team_sk", "primary_time", "status", "status_comment")
display(runs_teams_df)

# COMMAND ----------

runs_teams_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.runs_fact")
