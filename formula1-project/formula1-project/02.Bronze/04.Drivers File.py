# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

# MAGIC %run ../00.Common/bronze-helper-function

# COMMAND ----------

file_path = f"{landing_path}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName',StringType())
])

drivers_schema = StructType([
    StructField('driverId',StringType()),
    StructField('name',name_schema),
    StructField('dateOfBirth',DateType()),
    StructField('nationality',StringType()),
    StructField('url',StringType())
])

# COMMAND ----------

df_drivers = (spark.read.format("json")
                  .option('header','true')
                  .option('mode','FAILFAST')
                  .schema(drivers_schema)
                  .load(file_path))

# COMMAND ----------

drivers_final_df = metadata_adder(df_drivers)

# COMMAND ----------

drivers_final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)