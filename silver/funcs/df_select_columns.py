# Databricks notebook source
def selectColumns(df, columns):
    if any(item not in df.columns for item in columns):
        raise Exception(f"columns missing from dataframe, dataframe must contain: {columns}")

    output_df = df.select(*columns)
    return output_df
