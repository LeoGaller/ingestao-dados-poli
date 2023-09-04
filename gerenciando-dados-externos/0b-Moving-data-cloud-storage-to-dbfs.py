# Databricks notebook source
# MAGIC %md
# MAGIC ### Copying files from bucket to DBFS

# COMMAND ----------

# moving bronze
dbutils.fs.cp("gs://pece-poli-de/bronze/", "dbfs:/pece-poli-de/bronze/", True)
