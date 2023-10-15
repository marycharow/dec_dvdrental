# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in raw Airbyte tables to dataframes

# COMMAND ----------

df_customer = spark.read.table("main.default._airbyte_raw_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize dataframes

# COMMAND ----------

df_customer_norm = df_customer.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"customer_id","store_id","first_name","last_name","activebool","create_date","last_update","active")) \
    .toDF("_airbyte_ab_id","customer_id","store_id","first_name","last_name","activebool","create_date","last_update","active")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop not needed columns

# COMMAND ----------

df_customer_norm = df_customer_norm.select("customer_id","store_id","first_name","last_name","activebool","create_date","last_update").\
    withColumnRenamed("activebool","is_active_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Testing on df_customer

# COMMAND ----------

ge_silver_df_customer = SparkDFDataset(df_customer_norm)

# COMMAND ----------

for col in ["customer_id","store_id","last_update"]:
    expectation = ge_silver_df_customer.expect_column_values_to_not_be_null(col)
    if not expectation["success"]: 
        raise Exception(expectation)

# COMMAND ----------

expectation2 = ge_silver_df_customer.expect_column_values_to_be_unique("customer_id")
if not expectation2["success"]: 
    raise Exception(expectation2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply few remaining transformations

# COMMAND ----------

df_customer_norm = df_customer_norm.withColumn("customer_id",df_customer_norm.customer_id.cast(IntegerType())) \
.withColumn("store_id",df_customer_norm.store_id.cast(IntegerType())) \
.withColumn("is_active_customer",df_customer_norm.is_active_customer.cast(BooleanType())) \
.withColumn("create_date", df_customer_norm.create_date.cast(DateType())) \
.withColumn("last_update", df_customer_norm.last_update.cast(TimestampType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add surrogate key

# COMMAND ----------

df_customer_norm = df_customer_norm.withColumn("customer_key", md5(concat_ws("-", df_customer_norm.customer_id, df_customer_norm.last_update)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update existing table
# MAGIC ##### Not treating this as an SCD table

# COMMAND ----------

df_customer_norm.createOrReplaceTempView("dim_customer_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE main.default.dim_customer as
# MAGIC SELECT CUSTOMER_ID, STORE_ID, FIRST_NAME, LAST_NAME, IS_ACTIVE_CUSTOMER, CREATE_DATE, LAST_UPDATE, CUSTOMER_KEY
# MAGIC FROM dim_customer_view;
