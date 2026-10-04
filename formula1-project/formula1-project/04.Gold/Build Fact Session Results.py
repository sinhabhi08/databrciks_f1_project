# Databricks notebook source
# MAGIC %run 
# MAGIC ../00.Common/environment-variable

# COMMAND ----------

results_table =  f"{catalog_name}.{silver_schema}.results"
sprints_table =  f"{catalog_name}.{silver_schema}.sprints"
gold_table =  f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

from pyspark.sql import functions as F
df_results = (spark.read.table(results_table)
                   .withColumn(("session_type"), F.lit("RACE"))
                   .drop("race_name","race_date","source_file","ingestion_timestamp"))
df_sprints = (spark.read.table(sprints_table)
              .withColumn(("session_type"), F.lit("SPRINT"))
              .drop("race_name","race_date","source_file","ingestion_timestamp"))

# COMMAND ----------

df_fact_session_results = (df_results.unionByName(df_sprints)
                                     .withColumn("is_win", F.col("finish_position")== 1)
                                     .withColumn("is_podium",F.col("finish_position").between(1,3))
                                     .withColumn(("has_points"),F.col("points")>0))

# COMMAND ----------

(df_fact_session_results.write
                        .format("delta")
                        .mode("overwrite")
                        .saveAsTable(gold_table))