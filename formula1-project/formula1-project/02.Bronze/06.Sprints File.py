# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

# MAGIC %run ../00.Common/bronze-helper-function

# COMMAND ----------

file_path = f"{landing_path}/sprints/"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType


sprints_schema = StructType([
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

df_sprints = (spark.read.format("json")
                  .option('header','true')
                  .option('mode','FAILFAST')
                  .option('multiLine', 'true')
                  .schema(sprints_schema)
                  .load(file_path))

# COMMAND ----------

sprints_final_df = metadata_adder(df_sprints)

# COMMAND ----------

sprints_final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %fs ls '/Volumes/formula1/landing/files'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select season, count(*) from formula1.bronze.sprints
# MAGIC group by season order by season  