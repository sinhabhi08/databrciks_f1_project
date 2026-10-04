# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

constructors_df = spark.read.table(bronze_table)
display(constructors_df)

# COMMAND ----------

from pyspark.sql import functions as F 
constructors_selected_df = constructors_df.select(F.col("constructorId").alias("constructor_id"),
                                    F.col("name").alias("constructor_name"),
                                    F.col("nationality"),
                                    F.col("ingestion_timestamp"),
                                    F.col("source_file")
                                    )

# COMMAND ----------

constructors_dedup_df = constructors_selected_df.dropDuplicates(["constructor_id"])

# COMMAND ----------

constructors_final_df = constructors_dedup_df.withColumn("nationality", F.initcap("nationality"))

# COMMAND ----------

(constructors_final_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(silver_table))

# COMMAND ----------

# MAGIC %sql select * from formula1.silver.constructors