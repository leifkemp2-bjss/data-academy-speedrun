# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

users_dim_df = spark.read.format("delta").table("dea_speedrun.gold.users_dim")
runs_df = spark.read.format("delta").table("dea_speedrun.silver.runs")

# COMMAND ----------

reviewers_df = runs_df.select("reviewer").distinct().dropna()
display(reviewers_df)

# COMMAND ----------

reviewers_joined_df = reviewers_df.join(users_dim_df, reviewers_df.reviewer == users_dim_df.id, "left")
display(reviewers_joined_df)

# COMMAND ----------

final_df = reviewers_joined_df.withColumn("reviewer_sk", monotonically_increasing_id()) \
                                .select("reviewer_sk", "id", "name")

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.reviewers_dim")
