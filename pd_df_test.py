# Databricks notebook source
import requests
import json
import pandas as pd

# Check if the games table already exists, and check if the ID is already present in the games table

# A test to call an API multiple times and construct a singular pandas DataFrame from the responses, then parse it to a Spark DataFrame

ids_to_call = ["j1lewj6g", "j1lnmq76"]

pd_df = []

for id in ids_to_call:
    games_api_url = f"https://www.speedrun.com/api/v1/games/{id}"
    resp = requests.get(games_api_url)
    j = resp.json()
    if len(pd_df) == 0:
        pd_df = pd.DataFrame([j]) # Initialize the DF with the first result, this will produce the schema/columns needed for the next responses
    else:
        pd_df.loc[len(pd_df)] = j # Push the response to a new row in the DF

df = spark.createDataFrame(pd_df)
display(df)

# COMMAND ----------

df = spark.read.format("json").load(f"/Volumes/dea_speedrun/bronze/raw_data/games_data/games-j1lewj6g.json", multiLine=True).select("data.*")
display(df)
df.printSchema()

# COMMAND ----------

import requests

jsondata = []

games_api_url = f"https://www.speedrun.com/api/v1/games/j1lewj6g"
resp = requests.get(games_api_url)
j = resp.json()
jsondata.extend(j)

df2 = spark.read.json(jsondata)

display(df2)

# COMMAND ----------

df = spark.read.format("json").load("/Volumes/dea_speedrun/bronze/raw_data/runs_data/runs_allverified.json", multiLine=True)

display(df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DateType, IntegerType, BooleanType
from pyspark.sql import Row
import requests
import json
import pandas as pd

speedrun_api_url = 'https://www.speedrun.com/api/v1'

# Pull in data from the games API for all those games whose IDs are in the list, but not already in the database.
games_api_url = f"{speedrun_api_url}/games/"
dflist = []
url = games_api_url
resp = requests.get(url + "76r30wq6")
resp2 = requests.get(url + "nj1ne1p4")

j_dict = json.loads(resp.text)["data"]

del(j_dict["moderators"], j_dict["assets"], j_dict["links"])

new_row = Row(**j_dict)

games_schema = StructType([ StructField("id", StringType(), False),
                            StructField("names", StructType(fields=[
                                StructField("international", StringType()),
                                StructField("japanese", StringType()),
                                StructField("twitch", StringType()),
                            ]), True),
                            StructField("boostReceived", IntegerType(), True),
                            StructField("boostDistinctDonors", IntegerType(), True),
                            StructField("abbreviation", StringType(), True),
                            StructField("weblink", StringType(), True),
                            StructField("discord", StringType(), True),
                            StructField("released", StringType(), True),
                            StructField("release-date", StringType(), True),
                            StructField("ruleset", StructType(fields=[
                                StructField("show-milliseconds", BooleanType()),
                                StructField("require-verification", BooleanType()),
                                StructField("require-video", BooleanType()),
                                StructField("run-times", ArrayType(StringType())),
                                StructField("default-time", StringType()),
                                StructField("emulators-allowed", BooleanType())
                            ]), True),
                            StructField("romhack", BooleanType(), True),
                            StructField("gametypes", ArrayType(StringType()), True),
                            StructField("platforms", ArrayType(StringType()), True),
                            StructField("regions", ArrayType(StringType()), True),
                            StructField("genres", ArrayType(StringType()), True),
                            StructField("engines", ArrayType(StringType()), True),
                            StructField("developers", ArrayType(StringType()), True),
                            StructField("publishers", ArrayType(StringType()), True),
                            StructField("created", StringType(), True),
                            ])

schema = StructType(fields=[
    StructField("id", StringType()),
    StructField("names", StructType(fields=[
        StructField("international", StringType()),
        StructField("japanese", StringType()),
        StructField("twitch", StringType()),
    ])),
    StructField("abbreviation", StringType()),
    StructField("platforms", ArrayType(StringType())),
    StructField("regions", ArrayType(StringType())),
])

df = spark.createDataFrame([new_row], games_schema)
display(df)

# COMMAND ----------


