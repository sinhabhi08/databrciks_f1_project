# Databricks notebook source
from pyspark.sql import functions as F


def metadata_adder(df):
    return(df
           .withColumn('ingestion_timestamp',F.current_timestamp())
           .withColumn('source_file',F.col('_metadata.file_path'))
    )