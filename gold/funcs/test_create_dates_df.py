# Databricks notebook source
# MAGIC %run "./create_dates_df"

# COMMAND ----------

import unittest
from datetime import datetime, date, timedelta

# COMMAND ----------

class TestHelpers(unittest.TestCase):
    def test_createsDatesSmallSize(self):
        start_date = date(2020, 1, 1)
        end_date = date(2020, 1, 10)

        output_df = createDatesDF(start_date, end_date)

        self.assertEqual(output_df.count(), 10)

        first_date = date(output_df.first().year, output_df.first().month, output_df.first().day)
        self.assertEqual(first_date, date(2020, 1, 1))

        last_date = date(output_df.collect()[-1].year, output_df.collect()[-1].month, output_df.collect()[-1].day)
        self.assertEqual(last_date, date(2020, 1, 10))

    def test_createsDatesLargeSize(self):
        start_date = date(2020, 1, 1)
        end_date = date(2023, 12, 31)

        output_df = createDatesDF(start_date, end_date)
        self.assertEqual(output_df.count(), 1461)

        first_date = date(output_df.first().year, output_df.first().month, output_df.first().day)
        self.assertEqual(first_date, date(2020, 1, 1))

        last_date = date(output_df.collect()[-1].year, output_df.collect()[-1].month, output_df.collect()[-1].day)
        self.assertEqual(last_date, date(2023, 12, 31))

    def test_startDateAfterEndDate(self):
        start_date = date(2021, 1, 1)
        end_date = date(2020, 1, 1)

        with self.assertRaises(Exception):
            createDatesDF(start_date, end_date)

    def test_startDateNotProvided(self):
        start_date = "Nonsense"
        end_date = date(2023, 12, 31)

        with self.assertRaises(Exception):
            createDatesDF(start_date, end_date)

    def test_endDateNotProvided(self):
        start_date = date(2020, 1, 1)
        end_date = "Nonsense"

        with self.assertRaises(Exception):
            createDatesDF(start_date, end_date)

# COMMAND ----------

r = unittest.main(argv=[''], verbosity=2, exit=False, warnings="ignore")
assert r.result.wasSuccessful(), 'Test failed; see logs above'
