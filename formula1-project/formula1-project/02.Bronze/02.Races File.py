# Databricks notebook source
# MAGIC %md
# MAGIC ##Injesting the file into df

# COMMAND ----------

# MAGIC %run ../00.Common/environment-variable
# MAGIC

# COMMAND ----------

# MAGIC %run ../00.Common/bronze-helper-function

# COMMAND ----------

metadata_adder

# COMMAND ----------


table_name = f"{catalog_name}.{bronze_schema}.races"
file_path = f"{landing_path}/races.csv"

# COMMAND ----------

races_df = (spark.read.format("csv")
            .option('header','true')
            .option('inferSchema','true')
            .load(file_path))

# COMMAND ----------

races_df.display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

spark = SparkSession.builder.appName('abhispark').getOrCreate()
print(spark)

races_schema = StructType([
    StructField("season", IntegerType()),
    StructField("round", IntegerType()),
    StructField("url", StringType()),
    StructField('raceName', StringType()),
    StructField('date', DateType()),
    StructField('circuitId', StringType())
])

df_races = (spark.read
               .format('csv')
               .option('header', 'true')
               .schema(races_schema)
               .load(file_path)
               )

# COMMAND ----------

races_final_df = metadata_adder(df_races)

# COMMAND ----------

races_final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)