# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, col

# COMMAND ----------

def createLevelsDF(df):
    output_df = df.withColumn("level_sk", monotonically_increasing_id()) \
                .select("level_sk", "id", "name", "rules")

    return output_df
