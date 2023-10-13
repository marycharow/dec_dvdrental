# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Zone: Dimension Table: dim_store

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import prerequisite libraries

# COMMAND ----------

from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5, explode
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read in raw Airbyte tables to dataframes

# COMMAND ----------

df_store = spark.read.table("main.default._airbyte_raw_store")
df_address = spark.read.table("main.default._airbyte_raw_address")
df_city = spark.read.table("main.default._airbyte_raw_city")
df_country = spark.read.table("main.default._airbyte_raw_country")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Normalize dataframes

# COMMAND ----------

df_store_norm = df_store.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"store_id","manager_staff_id","address_id","last_update")) \
    .toDF("_airbyte_ab_id","store_id","manager_staff_id","address_id","last_update")
df_address_norm = df_address.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"address_id","address","address2","district","city_id","postal_code","phone","last_update")) \
    .toDF("_airbyte_ab_id","address_id","address","address2","district","city_id","postal_code","phone","last_update")
df_city_norm = df_city.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"city_id","city","country_id","last_update")) \
    .toDF("_airbyte_ab_id","city_id","city","country_id","last_update")
df_country_norm = df_country.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"country_id","country","last_update")) \
    .toDF("_airbyte_ab_id","country_id","country","last_update")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Tests with Great Expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test store table

# COMMAND ----------

ge_df_store = SparkDFDataset(df_store_norm)
expectation_store = ge_df_store.expect_column_values_to_not_be_null("store_id")
if not expectation_store["success"]: 
    raise Exception(expectation_store)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test address table

# COMMAND ----------

ge_df_address = SparkDFDataset(df_address_norm)
expectation_address = ge_df_address.expect_column_values_to_not_be_null("address_id")
if not expectation_address["success"]: 
    raise Exception(expectation_address)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test city table

# COMMAND ----------

ge_df_city = SparkDFDataset(df_city_norm)
expectation_city = ge_df_city.expect_column_values_to_not_be_null("city_id")
if not expectation_city["success"]: 
    raise Exception(expectation_city)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test country table

# COMMAND ----------

ge_df_country = SparkDFDataset(df_country_norm)
expectation_country = ge_df_country.expect_column_values_to_not_be_null("country_id")
if not expectation_country["success"]: 
    raise Exception(expectation_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Form Dimension Table

# COMMAND ----------

df_dim_store = df_store_norm.alias("s").join(
    other=df_address_norm.alias("a"), on="address_id", how="inner").join(
    other=df_city_norm.alias("ci"), on="city_id", how="inner").join(
    other=df_country_norm.alias("co"), on="country_id", how="inner").select("s.store_id", "a.city_id", "a.district", "s.last_update", "ci.city","ci.country_id","co.country")

display(df_dim_store)

df_dim_store = df_dim_store.select("store_id", "district", "city", "country","last_update")

display(df_dim_store)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add key

# COMMAND ----------

df_dim_store = df_dim_store.withColumn("store_key", \
        md5(concat_ws('-', col("store_id"), \
        col("city"))))

df_dim_store = df_dim_store.select("store_key", "store_id", "district", "city", "country","last_update")

# COMMAND ----------

display(df_dim_store)

# COMMAND ----------

df_dim_store.write.saveAsTable("main.default.dim_store")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.dim_store limit 10;

# COMMAND ----------


