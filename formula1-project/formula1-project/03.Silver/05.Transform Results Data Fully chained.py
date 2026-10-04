# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F 
results_df = (spark.read.table(bronze_table)
                        .select(F.col("date").alias("race_date"),
                                    F.col("raceName").alias("race_name"),
                                    F.col("round"),
                                    F.col("season"),
                                    F.col("constructorId").alias("constructor_id"),
                                    F.col("driverId").alias("driver_id"),
                                    F.col("grid"),
                                    F.col("laps").alias("completed_laps"),
                                    F.col("number").alias("car_number"),
                                    F.col("points"),
                                    F.col("position").alias("finish_position"),
                                    F.col("positionText").alias("finish_position_text"),
                                    F.col("status"),
                                    F.col("laps"),
                                    F.col("ingestion_timestamp"),
                                    F.col("source_file")
                                    )
                        .dropDuplicates(["season","round","constructor_id","driver_id"])
                        .filter((F.col("season").isNotNull())
                                               & (F.col("round").isNotNull()) 
                                               & (F.col("constructor_id").isNotNull())
                                               & (F.col("driver_id").isNotNull()))
                        

                            )

# COMMAND ----------

results_final_df = results_df.withColumn("race_name", F.initcap("race_name"))

# COMMAND ----------

(results_final_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(silver_table))

# COMMAND ----------

# MAGIC %sql select * from formula1.silver.results