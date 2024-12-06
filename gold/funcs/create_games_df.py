# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, col

# COMMAND ----------

def createGamesDF(df):
    output_df = df.withColumn("game_sk", monotonically_increasing_id()) \
                .select("game_sk", "id", "name")

    return output_df
