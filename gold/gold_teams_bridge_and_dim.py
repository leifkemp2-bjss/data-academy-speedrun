# Databricks notebook source
from pyspark.sql.functions import col, array_contains, monotonically_increasing_id, explode, concat_ws, when, collect_set
from pyspark.sql.types import IntegerType

# COMMAND ----------

users_dim_df = spark.read.format("delta").table("dea_speedrun.gold.users_dim")
guests_dim_df = spark.read.format("delta").table("dea_speedrun.gold.guests_dim")
runs_df = spark.read.format("delta").table("dea_speedrun.silver.runs")

# COMMAND ----------

distinct_teams_df = runs_df.select("players").distinct().alias("distinct_teams")
display(distinct_teams_df)

# COMMAND ----------

teams_bridge_df = distinct_teams_df.withColumn("team_sk", monotonically_increasing_id()) \
                            .withColumn("player", explode("players")) \
                            .withColumn("player_id", col("player.id")) \
                            .withColumn("guest_name", col("player.name")) \
                            .withColumn("is_guest", col("player.rel") == "guest") \
                            .select("team_sk", "player_id", "guest_name", "is_guest", "players")

display(teams_bridge_df)

# COMMAND ----------

final_df = teams_bridge_df.join(guests_dim_df, teams_bridge_df.guest_name == guests_dim_df.name, "left") \
                                .withColumnRenamed("name", "guest_d_name") \
                                .join(users_dim_df, teams_bridge_df.player_id == users_dim_df.id, "left") \
                                .withColumn("player_sk", concat_ws("", "guest_sk", "user_sk")) \
                                .select("team_sk", when(col("player_sk") == "", None).otherwise(col("player_sk")).alias("player_sk").cast(IntegerType()), 
                                        "is_guest", "players")
display(final_df)

# COMMAND ----------

teams_dim_df = final_df.select(explode(collect_set("team_sk")).alias("team_sk")).sort("team_sk").join(final_df, "team_sk", "left") \
                        .select("team_sk", "players").sort("team_sk")
display(teams_dim_df)

# COMMAND ----------

teams_dim_df.write.format("delta").mode("overwrite").saveAsTable("dea_speedrun.gold.teams_dim")
final_df.drop("players").write.format("delta").mode("overwrite").saveAsTable("dea_speedrun.gold.teams_bridge")
