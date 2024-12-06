# Databricks notebook source
# MAGIC %run "./create_guests_df"

# COMMAND ----------

import unittest
from datetime import datetime, date, timedelta
from pyspark.sql import Row

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_findsOneGuest(self):
        test_df = spark.createDataFrame([
            Row(id="1", game="Game 1", level=None, category="Category 1", date="2024-11-27", submitted="2024-11-27T00:00:00Z", status="verified", 
                players=[Row(id=None,name="G1",rel="guest")], 
                reviewer="R1", time=500, submitted_date=date(2024, 11, 27), status_comment=None)
        ], "id string, game string, level string, category string, date string, submitted string, status string, players array<struct<id:string,name:string,rel:string>>, reviewer string, time int, submitted_date date, status_comment string")

        output_df = createGuestsDF(test_df)

        self.assertEqual(output_df.count(), 1)
        # We cannot predict what monotonically_increasing_id will output, so just check for a non-null SK
        self.assertIsNotNone(output_df.first()["guest_sk"]) 
        self.assertEqual(output_df.first()["name"], "G1")

    def test_findsNoGuest(self):
        test_df = spark.createDataFrame([
            Row(id="1", game="Game 1", level=None, category="Category 1", date="2024-11-27", submitted="2024-11-27T00:00:00Z", status="verified", 
                players=[Row(id="P1",name=None,rel="user")], 
                reviewer="R1", time=500, submitted_date=date(2024, 11, 27), status_comment=None)
        ], "id string, game string, level string, category string, date string, submitted string, status string, players array<struct<id:string,name:string,rel:string>>, reviewer string, time int, submitted_date date, status_comment string")

        output_df = createGuestsDF(test_df)

        self.assertEqual(output_df.count(), 0)

    def test_findsMultipleGuests(self):
        test_df = spark.createDataFrame([
            Row(id="1", game="Game 1", level=None, category="Category 1", date="2024-11-27", submitted="2024-11-27T00:00:00Z", status="verified", 
                players=[Row(id="P1",name=None,rel="user"), Row(id=None,name="G1",rel="guest"), Row(id=None,name="G2",rel="guest")], 
                reviewer="R1", time=500, submitted_date=date(2024, 11, 27), status_comment=None)
        ], "id string, game string, level string, category string, date string, submitted string, status string, players array<struct<id:string,name:string,rel:string>>, reviewer string, time int, submitted_date date, status_comment string")

        output_df = createGuestsDF(test_df)

        self.assertEqual(output_df.count(), 2)

        self.assertEqual(len(output_df.filter(output_df.name.contains("G1")).collect()), 1)
        self.assertEqual(len(output_df.filter(output_df.name.contains("G2")).collect()), 1)

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
