# Databricks notebook source
# MAGIC %run "./funcs/runs_select"

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StringType, StructField
from pyspark.sql.functions import col, to_date, explode

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.bronze.runs")
display(df)

# COMMAND ----------

final_df = selectRunColumns(df)

display(final_df)

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable("dea_speedrun.silver.runs")
