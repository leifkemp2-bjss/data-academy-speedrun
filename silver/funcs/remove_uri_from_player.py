# Databricks notebook source
from pyspark.sql import Row

# COMMAND ----------

from pyspark.errors import PySparkAttributeError
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing import assertSchemaEqual

# COMMAND ----------

playerSchema = StructType(fields=[
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("rel", StringType(), True)
])

def removeUriFromPlayers(players):
    nameValue = None
    idValue = None
    new_players = []
    for player in players:
        try:
            player.rel
            player.uri
        except Exception:
            raise ValueError("row mismatch, the row should contain: rel, uri and one of (id, name)")

        try:
            idValue = player.id
        except Exception:
            pass
        
        try:
            nameValue = player.name
        except Exception:
            pass

        if nameValue == None and idValue == None:
            raise ValueError("row mismatch, id and name are both missing, the row should contain at least one of these")

        new_players.append(Row(id=idValue, name=nameValue, rel=player.rel))

    return new_players
