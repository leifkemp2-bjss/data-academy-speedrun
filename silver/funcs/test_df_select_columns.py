# Databricks notebook source
# MAGIC %run "./df_select_columns"

# COMMAND ----------

import unittest
from pyspark.sql.types import _parse_datatype_string
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_selectsCorrectColumns(self):
        columns = ["id", "name", "rules"]

        test_df = spark.createDataFrame([
            ("1", "Category 1", "Category 1 Rules", "Category 1 Comment")
        ], schema='id string, name string, rules string, comment string')

        expected_df = spark.createDataFrame([
            ("1", "Category 1", "Category 1 Rules")
        ], schema='id string, name string, rules string')
        
        output_df = selectColumns(test_df, columns)

        self.assertEqual(len(output_df.columns), len(columns))
        self.assertFalse("comment" in output_df.columns)

        assertDataFrameEqual(output_df, expected_df)
        assertSchemaEqual(output_df.schema, expected_df.schema)

    def test_retainsAppropriateData(self):
        columns = ["id", "name", "rules"]

        test_df = spark.createDataFrame([
            ("1", "Category 1", "Category 1 Rules", "Category 1 Comment"),
            ("2", "Category 2", "Category 2 Rules", "Category 2 Comment"),
            ("3", "Category 3", "Category 3 Rules", "Category 3 Comment"),
        ], schema='id string, name string, rules string, comment string')

        expected_df = spark.createDataFrame([
            ("1", "Category 1", "Category 1 Rules"),
            ("2", "Category 2", "Category 2 Rules"),
            ("3", "Category 3", "Category 3 Rules"),
        ], schema='id string, name string, rules string')
        
        output_df = selectColumns(test_df, columns)

        self.assertEqual(len(output_df.columns), len(columns))
        self.assertFalse("comment" in output_df.columns)

        assertDataFrameEqual(output_df, expected_df)
        assertSchemaEqual(output_df.schema, expected_df.schema)
        
    
    def test_missingColumns(self):
        columns = ["id", "name", "rules"]

        test_df = spark.createDataFrame([
            ("1", "Category 1", "Comment 1"),
            ("2", "Category 2", "Comment 2"),
            ("3", "Category 3", "Comment 3"),
        ], schema='id string, name string, comment string')

        with self.assertRaises(Exception):
            selectColumns(test_df, columns)

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
