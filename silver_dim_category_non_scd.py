# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5, to_date, current_date
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in raw Airbyte tables to dataframes

# COMMAND ----------

df = spark.read.table("main.default._airbyte_raw_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize dataframes

# COMMAND ----------

df_norm = df.select(json_tuple(col("_airbyte_data"),"category_id","name","last_update")) \
    .toDF("category_id","name","last_update")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

ge_silver_df_category = SparkDFDataset(df_norm)

# COMMAND ----------

expectation1 = ge_silver_df_category.expect_column_values_to_not_be_null("category_id")
if not expectation1["success"]: 
    raise Exception(expectation1)

# COMMAND ----------

expectation2 = ge_silver_df_category.expect_column_values_to_not_be_null("name")
if not expectation2["success"]: 
    raise Exception(expectation2)

# COMMAND ----------

expectation3 = ge_silver_df_category.expect_column_values_to_be_unique("name")
if not expectation3["success"]: 
    raise Exception(expectation3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply few remaining transformations

# COMMAND ----------

df_norm = df_norm.withColumn("category_id",df_norm.category_id.cast(IntegerType())) \
.withColumn("last_update", df_norm.last_update.cast(TimestampType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add surrogate key

# COMMAND ----------

df_norm = df_norm.withColumn("category_key", \
        md5(concat_ws('-', col("category_id"), \
        col("last_update"))))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update existing table
# MAGIC ##### Not treating this as an SCD table

# COMMAND ----------

df_norm.createOrReplaceTempView("dim_category_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.dim_category as
# MAGIC SELECT category_id, name, last_update, category_key
# MAGIC FROM dim_category_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE HIVE_METASTORE.default.dim_category as
# MAGIC SELECT *
# MAGIC FROM main.default.dim_category;
