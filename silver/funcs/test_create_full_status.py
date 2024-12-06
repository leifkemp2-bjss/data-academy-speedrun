# Databricks notebook source
# MAGIC %run "./create_full_status"

# COMMAND ----------

import unittest
from pyspark.sql import Row

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_doesNothingIfFull(self):
        status = Row(status="verified", examiner="test", reason="test reason")
        expected = Row(status="verified", examiner="test", reason="test reason")
        
        self.assertEqual(createFullStatus(status).asDict(), expected.asDict())

    def test_removesUnneededColumns(self):
        status = Row(status="verified", examiner="test", verify_date="2022-01-01", reason="test reason")
        expected = Row(status="verified", examiner="test", reason="test reason")
        
        self.assertEqual(createFullStatus(status).asDict(), expected.asDict())

    def test_noStatus(self):
        status = Row(examiner="test", verify_date="2022-01-01", reason="test reason")
        expected = Row(status=None, examiner="test", reason="test reason")
        
        self.assertEqual(createFullStatus(status).asDict(), expected.asDict())

    def test_noExaminer(self):
        status = Row(status="verified", verify_date="2022-01-01", reason="test reason")
        expected = Row(status="verified", examiner=None, reason="test reason")
        
        self.assertEqual(createFullStatus(status).asDict(), expected.asDict())

    def test_noReason(self):
        status = Row(status="verified", examiner="test", verify_date="2022-01-01")
        expected = Row(status="verified", examiner="test", reason=None)
        
        self.assertEqual(createFullStatus(status).asDict(), expected.asDict())

    def test_nothing(self):
        status = Row()
        expected = Row(status=None, examiner=None, reason=None)
        
        self.assertEqual(createFullStatus(status).asDict(), expected.asDict())

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
