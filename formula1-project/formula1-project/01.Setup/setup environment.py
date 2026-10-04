# Databricks notebook source
# MAGIC %md 
# MAGIC # create external location to access data from formula1 container in azure data lake gen2 storage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a location accessed using the abfss_remote_cred credential
# MAGIC CREATE EXTERNAL LOCATION formula1_extlocation URL 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/'
# MAGIC     WITH (STORAGE CREDENTIAL `databricks-course-sc`)
# MAGIC     COMMENT 'source for formula data set from Azure exernal data lake gen2 container';
# MAGIC

# COMMAND ----------

# MAGIC %fs ls 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/landing'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #create catalog 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG if not exists formula1 MANAGED LOCATION 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC # create schemas under the catalog formula 1
# MAGIC ## landing, bronze, silver and gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists formula1.landing;
# MAGIC create schema if not exists formula1.bronze MANAGED LOCATION 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/bronze';
# MAGIC create schema if not exists formula1.silver MANAGED LOCATION 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/silver';
# MAGIC create schema if not exists formula1.gold MANAGED LOCATION 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/gold';

# COMMAND ----------

# MAGIC %md
# MAGIC create a volume inside the landing catolog to directly see the files in the landing folder of the cloud
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create external volume if not exists formula1.landing.files LOCATION 'abfss://formula1@atexternallcations71234a.dfs.core.windows.net/landing';

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas; 

# COMMAND ----------

# MAGIC %fs ls /Volumes/formula1/landing/files

# COMMAND ----------

