# Databricks notebook source


# COMMAND ----------

# MAGIC %run ../00.Common/environment-variable

# COMMAND ----------

# MAGIC %run ../00.Common/bronze-helper-function

# COMMAND ----------

file_path = f"{landing_path}/constructors.json" 
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

landing_path

# COMMAND ----------

constructor_schema = """ 
                     constructorId string, name string, nationality string, url string                 
                    """

# COMMAND ----------

df_constructor = (spark.read.format("json")
                  .option('header','true')
                  .option('mode','FAILFAST')
                  .schema(constructor_schema)
                  .load(file_path))

# COMMAND ----------

constructor_final_df = metadata_adder(df_constructor)

# COMMAND ----------

constructor_final_df.write.format('delta').mode("overwrite").saveAsTable(table_name)