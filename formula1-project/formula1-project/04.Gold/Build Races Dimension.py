# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer will have Dimensional Modelling
# MAGIC ## Race, Driver, Constructor are Dims as they answer how when and qualitative measures
# MAGIC ## Building races Dims here which will be join of race and circuit

# COMMAND ----------

# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

gold_table = f"{catalog_name}.{gold_schema}.dim_races"
circuits_table = f"{catalog_name}.{silver_schema}.circuits"
races_table = f"{catalog_name}.{silver_schema}.races"


# COMMAND ----------

df_races = spark.read.table(races_table)
df_circuits = spark.read.table(circuits_table)

# COMMAND ----------

df_dim_races = (df_races.join(df_circuits,df_races.circuit_id == df_circuits.circuit_id,"left")
                        .select(df_races.season,
                                df_races.round,
                                df_races.race_name,
                                df_circuits.circuit_name,
                                df_circuits.latitude,
                                df_circuits.longitude,
                                df_circuits.locality,
                                df_circuits.country) )

# COMMAND ----------

(df_dim_races.write.format("delta").mode("overwrite").saveAsTable(gold_table))