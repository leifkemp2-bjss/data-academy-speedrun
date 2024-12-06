# Databricks notebook source
# MAGIC %run "./remove_uri_from_player"

# COMMAND ----------

# MAGIC %run "./create_full_status"

# COMMAND ----------

# MAGIC %run "./df_select_columns"

# COMMAND ----------

from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import ArrayType, StructType, StringType, StructField, DoubleType

# COMMAND ----------

schema = StructType(fields=[
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("rel", StringType()),
])

removeUriFromPlayersUDF = udf(removeUriFromPlayers, ArrayType(schema))

# COMMAND ----------

status_schema = StructType(fields=[
    StructField("status", StringType()),
    StructField("examiner", StringType()),
    StructField("reason", StringType())
])

createFullStatusUDF = udf(createFullStatus, status_schema)

# COMMAND ----------

def selectRunColumns(df):
    columns = ["id", "game", "level", "category", "date", "submitted", "times", "status", "players"]

    selected_df = selectColumns(df, columns)

    hasComment = "reason" in selected_df.select("status.*").columns

    output_df = selected_df \
                    .withColumn("status_struct", createFullStatusUDF("status")) \
                    .withColumn("reviewer", col("status_struct.examiner")) \
                    .withColumn("time", col("times.primary_t")) \
                    .withColumn("players", removeUriFromPlayersUDF("players")) \
                    .withColumn("submitted_date", to_date(col("submitted"))) \
                    .withColumn("status", col("status_struct.status")) \
                    .withColumn("status_comment", col("status_struct.reason"))
                    # .withColumnRenamed("status", "status_struct") \

    output_df = output_df.drop("status_struct", "times")

    return output_df
