# Databricks notebook source
# MAGIC %md
# MAGIC Read bronze `circuits` table
# MAGIC Keep only the columns required for analytics (Drop `url` column)
# MAGIC Standardise column names using snake_case (`circuitId` → `circuit_id`, `circuitName` → `circuit_name`)
# MAGIC  Rename columns to make them more meaningful (`lat` → `latitude`, `long` → `longitude`)
# MAGIC  Filter out rows where `circuit_id` is null (business key validation)
# MAGIC  Remove duplicate records
# MAGIC  Transform values of columns `circuit_name` and `locality` to Title Case
# MAGIC  Write the transformed data to silver `circuits` table

# COMMAND ----------

# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------


bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName("myapp").getOrCreate()

df_circuits = spark.table(bronze_table)
df_circuits = spark.read.table(bronze_table) 

# COMMAND ----------

# MAGIC %md 
# MAGIC removing unwanted columns by not selecting them 

# COMMAND ----------

from pyspark.sql import functions as F
df_circuits_selected = df_circuits.select(
                        F.col('circuitId').alias('circuit_id'),
                        F.col('circuitName').alias('circuit_name'),
                        F.col('lat').alias('latitude'),
                        F.col('long').alias('longitude'),
                        F.col('locality'),
                        F.col('country'),
                        F.col('ingestion_timestamp'),
                        F.col('source_file')
)

display(df_circuits_selected)

# COMMAND ----------

df_circuits_no_null = df_circuits_selected.filter(F.col('circuit_id').isNotNull())

display(df_circuits_no_null)


# COMMAND ----------

df_circuits_dedup = df_circuits_no_null.distinct()

# COMMAND ----------

df_circuits_final = (df_circuits_dedup
                     .withColumn("circuit_name", F.initcap("circuit_name"))
                     .withColumn("locality",F.initcap("locality")))

display(df_circuits_final)       

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.silver.circuits

# COMMAND ----------

df_circuits_final.write.format("delta").mode("overwrite").saveAsTable(silver_table)