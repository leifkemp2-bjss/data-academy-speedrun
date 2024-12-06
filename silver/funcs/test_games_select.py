# Databricks notebook source
# MAGIC %run "./games_select"

# COMMAND ----------

import unittest
from pyspark.sql.types import _parse_datatype_string
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_selectsAndRenamesCorrectColumns(self):
        test_df = spark.createDataFrame([
            ("1", "Game 1", "Developer 1", "Game 1 Comment")
        ], schema='id string, abbreviation string, developer string, comment string')

        expected_df = spark.createDataFrame([
            ("1", "Game 1")
        ], schema='id string, name string')
        
        output_df = selectGameColumns(test_df)

        self.assertEqual(len(output_df.columns), 2)
        self.assertFalse("comment" in output_df.columns)

        assertDataFrameEqual(output_df, expected_df)
        assertSchemaEqual(output_df.schema, expected_df.schema)

    def test_retainsAppropriateData(self):
        test_df = spark.createDataFrame([
            ("1", "Game 1", "Game 1 Comment"),
            ("2", "Game 2", "Game 2 Comment"),
            ("3", "Game 3", "Game 3 Comment"),
        ], schema='id string, abbreviation string, comment string')

        expected_df = spark.createDataFrame([
            ("1", "Game 1"),
            ("2", "Game 2"),
            ("3", "Game 3"),
        ], schema='id string, name string')
        
        output_df = selectGameColumns(test_df)

        self.assertEqual(len(output_df.columns), 2)
        self.assertFalse("comment" in output_df.columns)

        assertDataFrameEqual(output_df, expected_df)
        assertSchemaEqual(output_df.schema, expected_df.schema)
        
    
    def test_missingColumns(self):
        test_df = spark.createDataFrame([
            ("1", "Comment 1"),
            ("2", "Comment 2"),
            ("3", "Comment 3"),
        ], schema='id string, comment string')

        with self.assertRaises(Exception):
            selectGameColumns(test_df)

    def test_incorrectlyNamedColumns(self):
        test_df = spark.createDataFrame([
            ("1", "Game 1"),
            ("2", "Game 2"),
            ("3", "Game 3"),
        ], schema='id string, name string')

        with self.assertRaises(Exception):
            selectGameColumns(test_df)

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
