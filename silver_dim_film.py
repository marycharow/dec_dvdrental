# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in raw Airbyte tables to dataframes

# COMMAND ----------

df_film = spark.read.table("main.default._airbyte_raw_film")

# COMMAND ----------

df_language = spark.read.table("main.default._airbyte_raw_language")

# COMMAND ----------

df_film_category = spark.read.table("main.default._airbyte_raw_film_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize dataframes

# COMMAND ----------

df_film_norm = df_film.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"film_id","title","description","release_year","language_id","rental_duration","rental_rate","length","replacement_cost","rating","last_update")) \
    .toDF("_airbyte_ab_id","film_id","title","description","release_year","language_id","rental_duration","rental_rate","length","replacement_cost","rating","last_update")

# COMMAND ----------

df_language_norm = df_language.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"language_id","name","last_update")) \
    .toDF("_airbyte_ab_id","language_id","name","last_update")

# COMMAND ----------

df_film_category_norm = df_film_category.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"film_id","category_id","last_update")) \
    .toDF("_airbyte_ab_id","film_id","category_id","last_update")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Testing on df_film

# COMMAND ----------

ge_silver_df_film = SparkDFDataset(df_film_norm)

# COMMAND ----------

for col in ["film_id","language_id","last_update"]:
    expectation = ge_silver_df_film.expect_column_values_to_not_be_null(col)
    if not expectation["success"]: 
        raise Exception(expectation)

# COMMAND ----------

expectation2 = ge_silver_df_film.expect_column_values_to_be_unique("film_id")
if not expectation2["success"]: 
    raise Exception(expectation2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Testing on df_language

# COMMAND ----------

ge_silver_df_language = SparkDFDataset(df_language_norm)

# COMMAND ----------

for col in ["language_id","name"]:
    expectation = ge_silver_df_language.expect_column_values_to_not_be_null(col)
    if not expectation["success"]: 
        raise Exception(expectation)

# COMMAND ----------

expectation3 = ge_silver_df_language.expect_column_values_to_be_unique("language_id")
if not expectation3["success"]: 
    raise Exception(expectation3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Testing on df_film_category

# COMMAND ----------

ge_silver_df_film_category = SparkDFDataset(df_film_category_norm)

# COMMAND ----------

for col in ["film_id","category_id"]:
    expectation = ge_silver_df_film_category.expect_column_values_to_not_be_null(col)
    if not expectation["success"]: 
        raise Exception(expectation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge film and language dfs

# COMMAND ----------

df_film_silver = df_film_norm.alias("f").join( \
    other=df_language_norm.alias("ln"), on="language_id", how="inner").join( \
    other=df_film_category_norm.alias("fc"), on="film_id", how="inner") \
    .select("f.film_id","f.title","f.release_year","f.rating","ln.name","fc.category_id") \
    .withColumn("last_update", current_timestamp()) \
    .withColumnRenamed("name", "language")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add surrogate key

# COMMAND ----------

df_film_silver = df_film_silver.withColumn("film_key", md5("film_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new film records into existing table
# MAGIC ##### Not treating this as an SCD table

# COMMAND ----------

df_film_silver.createOrReplaceTempView("film_silver_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO main.default.dim_film as target
# MAGIC USING film_silver_source as source
# MAGIC ON target.film_key = source.film_key
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (film_id, title, release_year, rating, language, category_id, last_update, film_key) 
# MAGIC   VALUES (source.film_id,  source.title, source.release_year, source.rating, source.language, source.category_id, source.last_update, source.film_key)
