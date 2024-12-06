# Databricks notebook source
# MAGIC %run "./funcs/create_dates_df"

# COMMAND ----------

from datetime import datetime, date

# COMMAND ----------

start_date = date(2024, 10, 1)
end_date = date(2024, 12, 31)

date_dim_df = createDatesDF(start_date, end_date)
display(date_dim_df)

# COMMAND ----------

date_dim_df.write.format("delta").mode("overwrite").saveAsTable(f"dea_speedrun.gold.dates_dim")
