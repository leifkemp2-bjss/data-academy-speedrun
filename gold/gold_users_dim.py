# Databricks notebook source
# MAGIC %run "./funcs/create_users_df"

# COMMAND ----------

df = spark.read.format("delta").table("dea_speedrun.silver.users")
display(df)

# COMMAND ----------

users_dim_df = createUsersDF(df)

display(users_dim_df)

# COMMAND ----------

users_dim_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.users_dim")
