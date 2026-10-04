# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

# MAGIC %run ../00.Common/bronze-helper-function

# COMMAND ----------

table_name = f"{catalog_name}.{bronze_schema}.circuits"


# COMMAND ----------

file_path = f"{landing_path}/circuits.csv"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName('abhispark').getOrCreate()


circuits_schema = StructType([
    StructField("circuitId", StringType()),
    StructField("url", StringType()),
    StructField("circuitName", StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())
])

df_circuits = (spark.read
               .format('csv')
               .option('header', 'true')
               .schema(circuits_schema)
               .load(file_path)
               )

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName('abhispark').getOrCreate
print(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding metadata audit columns
# MAGIC

# COMMAND ----------

circuits_final_df = metadata_adder(df_circuits)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##creating table in delta format in bronze 
# MAGIC  

# COMMAND ----------

circuits_final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)