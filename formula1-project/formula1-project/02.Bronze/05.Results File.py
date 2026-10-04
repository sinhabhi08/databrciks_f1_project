# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

# MAGIC %run ../00.Common/bronze-helper-function

# COMMAND ----------

file_path = f"{landing_path}/results/"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType


results_schema = StructType([
    StructField('date',DateType()),
    StructField('raceName',StringType()),
    StructField('round',IntegerType()),
    StructField('season',StringType()),
    StructField('url',StringType()),
    StructField('constructorId',StringType()),
    StructField('driverId',StringType()),
    StructField('grid',IntegerType()),
    StructField('laps',IntegerType()),
    StructField('number',IntegerType()),
    StructField('points',DoubleType()),
    StructField('position',IntegerType()),
    StructField('positionText',StringType()),
    StructField('status',StringType())
])

# COMMAND ----------

df_results = (spark.read.format("json")
                  .option('header','true')
                  .option('mode','FAILFAST')
                  .schema(results_schema)
                  .load(file_path))

# COMMAND ----------

results_final_df = metadata_adder(df_results)

# COMMAND ----------

results_final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %fs ls '/Volumes/formula1/landing/files'