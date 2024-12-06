# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

def selectUserColumns(df):
    columns = ["id", "names", "location", "role"]
    if any(item not in df.columns for item in columns):
        raise Exception(f"columns missing from dataframe, dataframe must contain: {columns}")

    if "international" not in df.select("names.*").columns:
        raise Exception("names column does not have an international field, this is required")

    if "country" not in df.select("location.*").columns:
        raise Exception("location does not contain a country, this is required")

    if "names" not in df.select("location.country.*").columns:
        raise Exception("location.country does not contain a names field, this is required")

    if "international" not in df.select("location.country.names.*").columns:
        raise Exception("location.country.names does not contain an international field, this is required")

    output_df = df.select("id", col("names.international").alias("name"), col("location.country.names.international").alias("country"), "role")
    return output_df
