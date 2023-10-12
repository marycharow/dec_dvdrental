# Databricks notebook source
df = spark.read.table("hive_metastore.project2team3._airbyte_raw_address")

# COMMAND ----------

display(df)

# COMMAND ----------


