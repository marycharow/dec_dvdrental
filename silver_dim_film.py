# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,TimestampType

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

df_film_category_norm = df_film_category.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"film_id","category_id","rental_duration","last_update")) \
    .toDF("_airbyte_ab_id","film_id","category_id","rental_duration","last_update")

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

for col in ["film_id","language_id","rental_duration","last_update"]:
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
    .select("f.film_id","f.title","f.release_year","f.rating","f.rental_duration","ln.name","fc.category_id","f.last_update") \
    .withColumnRenamed("name", "language")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply few remaining transformations

# COMMAND ----------

df_film_silver = df_film_silver.withColumn("film_id",df_film_silver.film_id.cast(IntegerType())) \
.withColumn("release_year",df_film_silver.release_year.cast(IntegerType())) \
.withColumn("rental_duration",df_film_silver.rental_duration.cast(IntegerType())) \
.withColumn("category_id", df_film_silver.category_id.cast(IntegerType())) \
.withColumn("last_update", df_film_silver.last_update.cast(TimestampType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add surrogate key

# COMMAND ----------

df_film_silver = df_film_silver.withColumn("film_key", md5(concat_ws("-", df_film_silver.film_id, df_film_silver.last_update)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new film records into existing table
# MAGIC ##### Not treating this as an SCD table

# COMMAND ----------

df_film_silver.createOrReplaceTempView("film_silver_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE MAIN.DEFAULT.dim_film as
# MAGIC   select film_id,
# MAGIC          title,
# MAGIC          release_year,
# MAGIC          rating,
# MAGIC          rental_duration,
# MAGIC          language,
# MAGIC          category_id,
# MAGIC          last_update,
# MAGIC          film_key
# MAGIC   FROM film_silver_source;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE HIVE_METASTORE.DEFAULT.dim_film as
# MAGIC   select *
# MAGIC   FROM MAIN.DEFAULT.dim_film;
