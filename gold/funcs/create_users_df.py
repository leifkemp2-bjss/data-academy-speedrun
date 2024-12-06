# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id

# COMMAND ----------

def createUsersDF(df):
    output_df = df.withColumn("user_sk", monotonically_increasing_id()) \
                .select("user_sk", "id", "name", "country")

    return output_df
