# Databricks notebook source
# Building date table programatically to include dates that may not be in the orders table

from datetime import datetime, date, timedelta
from pyspark.sql.types import StructType, StructField, IntegerType
from dateutil.relativedelta import relativedelta


def createDatesDF(start_date, end_date):
    if start_date > end_date:
        raise ValueError("Start date must be before end date")
    if isinstance(start_date, date) == False:
        raise ValueError("Start date must be a date object")
    if isinstance(end_date, date) == False:
        raise ValueError("Start date must be a date object")

    date_dim_df_schema = StructType(fields=[
        StructField("date_sk", IntegerType(), False),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
    ])

    # start_date = date.today() - relativedelta(years=1)
    # start_date = date(2023, 1, 1)
    # end_date = date.today()
    delta = timedelta(days=1)
    index = 0

    rows = []
    while start_date <= end_date:
        rows.append((index, start_date.year, start_date.month, start_date.day))    
        index += 1
        start_date += delta

    output_df = spark.createDataFrame([i for i in rows], schema=date_dim_df_schema)

    return output_df
