# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

gold_table = f"{catalog_name}.{gold_schema}.dim_drivers"
drivers_table = f"{catalog_name}.{silver_schema}.drivers"
regions_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

df_drivers = spark.read.table(drivers_table)
df_regions = spark.read.table(regions_table)

# COMMAND ----------

df_drivers_dim = (df_drivers.join(df_regions, df_drivers.nationality == df_regions.nationality,"left")
                                      .select(df_drivers.driver_id,
                                              df_drivers.driver_name,
                                              df_drivers.date_of_birth,
                                              df_drivers.nationality,
                                              df_regions.region))

# COMMAND ----------

(df_drivers_dim.write.format("delta")
                          .mode("overwrite")
                          .saveAsTable(gold_table) )