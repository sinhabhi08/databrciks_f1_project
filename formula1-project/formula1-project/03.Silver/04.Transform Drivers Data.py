# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

drivers_df = spark.read.table(bronze_table)
display(drivers_df)

# COMMAND ----------

from pyspark.sql import functions as F 
drivers_concatinated_df = (drivers_df
                           .withColumn("driver_name", F.initcap(F.concat_ws(" ", F.col("name.givenName"), F.col("name.familyName"))))
                           .drop("name")
                           .drop("url")
                           )

# COMMAND ----------

# DBTITLE 1,Cell 5
drivers_dedup_df = drivers_concatinated_df.dropDuplicates(["driverId"])

# COMMAND ----------

display(drivers_dedup_df)

# COMMAND ----------

from pyspark.sql import functions as F 
drivers_selected_df = drivers_dedup_df.select(F.col("driverId").alias("driver_id"),
                                    F.col("dateOfBirth").alias("date_of_birth"),
                                    F.col("nationality"),
                                    F.col("ingestion_timestamp"),
                                    F.col("source_file"),
                                    F.col("driver_name")
                                    )

# COMMAND ----------

drivers_final_df = (drivers_selected_df.withColumn("nationality", F.initcap("nationality"))
                               
)

# COMMAND ----------


(drivers_final_df
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(silver_table))

# COMMAND ----------

# MAGIC %sql select * from formula1.silver.drivers

# COMMAND ----------

