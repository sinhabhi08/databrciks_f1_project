# Databricks notebook source
# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

gold_table = f"{catalog_name}.{gold_schema}.dim_constructors"
constructors_table = f"{catalog_name}.{silver_schema}.constructors"
regions_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

df_constructors = spark.read.table(constructors_table)
df_regions = spark.read.table(regions_table)

# COMMAND ----------

df_constructors_dim = (df_constructors.join(df_regions, df_constructors.nationality == df_regions.nationality,"left")
                                      .select(df_constructors.constructor_id,
                                              df_constructors.constructor_name,
                                              df_constructors.nationality,
                                              df_regions.region))

# COMMAND ----------

(df_constructors_dim.write.format("delta")
                          .mode("overwrite")
                          .saveAsTable(gold_table) )