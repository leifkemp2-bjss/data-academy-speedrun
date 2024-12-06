# Databricks notebook source
# MAGIC %run "./users_select"

# COMMAND ----------

import unittest
import pandas as pd
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_selectsCorrectData(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "international": "NiceGouda", "japanese": None }, "pronouns": "He/Him", "role": "user", "signup": "2023-02-19T20:42:01Z", "location": { "country": { "code": "us", "names": { "international": "United States", "japanese": None } } } }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))

        expected_df = spark.createDataFrame([
            ("xk1kw9kj", "NiceGouda", "United States", "user")
        ], schema='id string, name string, country string, role string')

        output_df = selectUserColumns(test_df)

        self.assertFalse("signup" in output_df.columns)
        
        assertDataFrameEqual(output_df, expected_df)
        assertSchemaEqual(output_df.schema, expected_df.schema)

    def test_missingRole(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "international": "NiceGouda", "japanese": None }, "pronouns": "He/Him", "signup": "2023-02-19T20:42:01Z", "location": { "country": { "code": "us", "names": { "international": "United States", "japanese": None } } } }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))
        
        with self.assertRaises(Exception):
            output_df = selectUserColumns(test_df)

    def test_missingInternationalName(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "japanese": "テスト" }, "pronouns": "He/Him", "role": "user", "signup": "2023-02-19T20:42:01Z", "location": { "country": { "code": "us", "names": { "international": "United States", "japanese": None } } } }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))

        with self.assertRaises(Exception):
            output_df = selectUserColumns(test_df)

    def test_missingLocation(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "international": "NiceGouda", "japanese": None }, "pronouns": "He/Him", "role": "user", "signup": "2023-02-19T20:42:01Z" }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))

        with self.assertRaises(Exception):
            output_df = selectUserColumns(test_df)

    def test_missingCountry(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "international": "NiceGouda", "japanese": None }, "pronouns": "He/Him", "role": "user", "signup": "2023-02-19T20:42:01Z", "location": { } }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))

        with self.assertRaises(Exception):
            output_df = selectUserColumns(test_df)

    def test_missingCountryNames(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "international": "NiceGouda", "japanese": None }, "pronouns": "He/Him", "role": "user", "signup": "2023-02-19T20:42:01Z", "location": { "country": { "code": "us" } } }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))

        with self.assertRaises(Exception):
            output_df = selectUserColumns(test_df)

    def test_missingCountryInternationalName(self):
        testJson = [{ "id": "xk1kw9kj", "names": { "international": "NiceGouda", "japanese": None }, "pronouns": "He/Him", "role": "user", "signup": "2023-02-19T20:42:01Z", "location": { "country": { "code": "us", "names": { "japanese": None } } } }]

        test_df = spark.createDataFrame(pd.DataFrame(testJson))

        with self.assertRaises(Exception):
            output_df = selectUserColumns(test_df)

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
