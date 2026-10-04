# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

races_df = spark.read.table(bronze_table)

# COMMAND ----------

from pyspark.sql import functions as F 
races_selected_df = races_df.select(F.col("season"),
                                    F.col("round"),
                                    F.col("raceName").alias("race_name"),
                                    F.col("date").alias("race_date"),
                                    F.col("circuitId").alias("circuit_id"),
                                    F.col("ingestion_timestamp"),
                                    F.col("source_file")
                                    )

# COMMAND ----------

races_dedup_df = races_selected_df.dropDuplicates()

# COMMAND ----------

races_final_df = races_dedup_df.withColumn("race_name", F.initcap("race_name"))

# COMMAND ----------

(races_final_df
 .write
 .format("delta")
 .mode("overwrite").saveAsTable(silver_table))

# COMMAND ----------

# MAGIC %sql select * from formula1.silver.races