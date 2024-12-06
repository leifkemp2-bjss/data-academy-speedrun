# Databricks notebook source
# MAGIC %run "./extract_data_from_raw"

# COMMAND ----------

import unittest
import pandas as pd

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_ExtractFromStructType(self):
        expected = {
            "id":"xd131x4k", 
            "name":"Any%", 
            "weblink":"https://www.speedrun.com/little_nightmares"
        }

        structJsonStr = [{ "data": { "id": "xd131x4k", "name": "Any%", "weblink": "https://www.speedrun.com/little_nightmares" } }]
        struct_df = spark.createDataFrame(pd.DataFrame(structJsonStr))

        output_df = extractDataFromRawDF(struct_df)
        
        # The output should have 1 row and 3 columns
        self.assertEqual(len(output_df.columns), len(expected))
        self.assertEqual(output_df.count(), 1)

        for col in output_df.columns:
            self.assertTrue(col in list(expected.keys()))
            self.assertEqual(output_df.first()[col], expected[col])

    def test_ExtractFromArrayType(self):
        expected = {
            "id":["ypl25l47", "mol4z19n"], 
            "name":["BRA / PAL","CHN / PAL"], 
        }

        arrayJsonStr = [{ "data": [ { "id": "ypl25l47", "name": "BRA / PAL" }, { "id": "mol4z19n", "name": "CHN / PAL" } ] }]
        array_df = spark.createDataFrame(pd.DataFrame(arrayJsonStr))

        output_df = extractDataFromRawDF(array_df)

        # The output should have two rows and two columns
        self.assertEqual(len(output_df.columns), len(expected))
        self.assertEqual(output_df.count(), 2)

        for col in output_df.columns:
            self.assertTrue(col in list(expected.keys()))
            for index in range(2):
                self.assertEqual(output_df.collect()[index][col], expected[col][index])

    def test_ExtractInvalidType(self):
        intJsonStr = [{ "data": "5" }]
        int_df = spark.createDataFrame(pd.DataFrame(intJsonStr))

        with self.assertRaises(Exception):
            extractDataFromRawDF(int_df)

    def test_ExtractNoData(self):
        noDataJsonStr = [{ "nonsense_column": "5" }]
        noData_df = spark.createDataFrame(pd.DataFrame(noDataJsonStr))

        with self.assertRaises(Exception):
            extractDataFromRawDF(noData_df)


# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
