# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, explode, col

# COMMAND ----------

def createGuestsDF(df):
    """Extracts all guests from the runs table, assumes that the players column is suitably formatted as the silver table applies the UDF to format it"""
    output_df = df.withColumn("guest", explode(col("players"))) \
                    .filter(col("guest").rel == "guest") \
                    .select("guest.name").distinct() \
                    .withColumn("guest_sk", monotonically_increasing_id()) \
                    .select("guest_sk", "name")
    
    return output_df

# COMMAND ----------


