# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id, explode, col

# COMMAND ----------

def createReviewersDF(r_df, u_df):
    """Extracts all reviewers from the runs table, assumes that the status column is suitably formatted as the silver table applies the UDF to format it"""
    output_df = df.select("reviewer").distinct() \
                    .withColumn("reviewer_sk", monotonically_increasing_id()) \
                    .select("reviewer_sk", col("reviewer").alias("name"))
    
    return output_df

# COMMAND ----------


