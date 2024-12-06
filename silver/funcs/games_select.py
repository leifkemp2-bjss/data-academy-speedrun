# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

def selectGameColumns(df):
    columns = ["id", "abbreviation", "names"]
    if any(item not in df.columns for item in columns):
        raise Exception(f"columns missing from dataframe, dataframe must contain: {columns}")

    output_df = df.select("id", col("abbreviation"), col("names.international").alias("name"))
    return output_df
