# Databricks notebook source
# MAGIC %run "./funcs/create_guests_df"

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# COMMAND ----------

runs_df = spark.read.format("delta").table("dea_speedrun.silver.runs")
display(runs_df)

# COMMAND ----------

# guests_dim_df = runs_df.withColumn("guest", explode(col("players"))) \
#                         .filter(col("guest").rel == "guest") \
#                         .select("guest.name").distinct() \
#                         .withColumn("guest_sk", monotonically_increasing_id()) \
#                         .select("guest_sk", "name")

guests_dim_df = createGuestsDF(runs_df)

display(guests_dim_df)

# COMMAND ----------

guests_dim_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.guests_dim")
