# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, col

# COMMAND ----------

def createCategoriesDF(df):
    output_df = df.withColumn("category_sk", monotonically_increasing_id()) \
                .select("category_sk", "id", "name", "rules")

    return output_df
