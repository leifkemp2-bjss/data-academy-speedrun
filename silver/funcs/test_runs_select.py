# Databricks notebook source
# MAGIC %run "./runs_select"

# COMMAND ----------

# MAGIC %run "./test_data/test_runs_data"

# COMMAND ----------

import unittest
import pandas as pd
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual
import os

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_createRunColumns(self):
        test_df = spark.createDataFrame(pd.DataFrame(test_runs_valid))

        expected_df = spark.createDataFrame(pd.DataFrame(test_runs_valid_expected))
        output_df = selectRunColumns(test_df)

        # Checking to see if the contents match, schema checks are too awkward to implement due to how the dataframes are created from large variables and PD DFs
        self.assertEqual(expected_df.collect()[0].asDict(), output_df.collect()[0].asDict())

    def test_missingColumns(self):
        test_df = spark.createDataFrame(pd.DataFrame(test_runs_noId))

        with self.assertRaises(Exception):
            output_df = selectRunColumns(test_df)
        

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
