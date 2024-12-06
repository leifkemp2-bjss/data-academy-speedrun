# Databricks notebook source
# MAGIC %run "./create_users_df"

# COMMAND ----------

import unittest
from pyspark.testing import assertDataFrameEqual

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_createsOneUser(self):
        test_df = spark.createDataFrame([
            ("1", "P1", "England", "user")
        ], "id string, name string, country string, role string")
        
        expected_df = spark.createDataFrame([
            (0, "1", "P1", "England")
        ], "user_sk long, id string, name string, country string")

        output_df = createUsersDF(test_df)

        self.assertIsNotNone(output_df.first()["user_sk"])
        assertDataFrameEqual(output_df.drop("user_sk"), expected_df.drop("user_sk"))

    def test_createsMultipleUsers(self):
        test_df = spark.createDataFrame([
            ("1", "P1", "England", "user"),
            ("2", "P2", "United States", "user"),
            ("3", "P3", "France", "user")
        ], "id string, name string, country string, role string")
        
        expected_df = spark.createDataFrame([
            (0, "1", "P1", "England"),
            (0, "2", "P2", "United States"),
            (0, "3", "P3", "France")
        ], "user_sk long, id string, name string, country string")

        output_df = createUsersDF(test_df)

        self.assertIsNotNone(output_df.collect()[0]["user_sk"])
        self.assertIsNotNone(output_df.collect()[1]["user_sk"])
        self.assertIsNotNone(output_df.collect()[2]["user_sk"])
        assertDataFrameEqual(output_df.drop("user_sk"), expected_df.drop("user_sk"))



# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
