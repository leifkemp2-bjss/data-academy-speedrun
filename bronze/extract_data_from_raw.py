# Databricks notebook source
from pyspark.sql.functions import explode, col
from pyspark.sql.types import ArrayType, StructType, StringType

# COMMAND ----------

def extractDataFromRawDF(df):
    """
    Extracts the columns from the 'data' object in the raw JSON dataframe.
    The 'data' column must be an ArrayType or StructType, and will throw an error if not present, or does not match these types.
    """
    if "data" not in df.columns:
        raise Exception("No data column is present in the raw data")

    if isinstance(df.schema["data"].dataType, ArrayType):
        return df.select(explode(df.data)).select(col("col.*"))
    elif isinstance(df.schema["data"].dataType, StructType):
        return df.select(col("data.*"))
    else:
        raise Exception("Unsupported data type for data column, should be either ArrayType or StructType")
