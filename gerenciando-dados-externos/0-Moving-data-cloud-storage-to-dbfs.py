# Databricks notebook source
# Verificando acessos
dbutils.fs.ls("gs://pece-poli-de")

# COMMAND ----------

# moving bronze
dbutils.fs.cp("gs://pece-poli-de/bronze/", "dbfs:/pece-poli-de/bronze/", True)
