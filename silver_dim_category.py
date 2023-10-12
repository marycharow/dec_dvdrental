# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in raw Airbyte tables to dataframes

# COMMAND ----------

df = spark.read.table("main.default._airbyte_raw_category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize dataframes

# COMMAND ----------

df_norm = df.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"category_id","name","last_update")) \
    .toDF("_airbyte_ab_id","category_id","name","last_update")

#display(df_norm)

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
# MAGIC ### Add surrogate key

# COMMAND ----------

df_norm = df_norm.withColumn("category_key", \
        md5(concat_ws('-', col("category_id"), \
        col("name"))))

# COMMAND ----------

df_norm.createOrReplaceTempView("table_norm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Insert new record for any new category ids or category ids that have a new name  

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO main.default.dim_category as target
# MAGIC USING table_norm as source
# MAGIC ON source.category_key=target.category_key
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (category_id, name, start_date, end_date, last_update, category_key) VALUES (source.category_id,  source.name, current_timestamp(), '2999-01-01', current_timestamp(), category_key)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update end date for inactive category records

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE INSERTED_TODAY AS
# MAGIC   SELECT DISTINCT CATEGORY_KEY, START_DATE
# MAGIC   FROM MAIN.DEFAULT.dim_category
# MAGIC   WHERE DATE(start_date) = current_date()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inserted_today

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO main.default.dim_category as target
# MAGIC USING INSERTED_TODAY as source
# MAGIC ON source.category_key=target.category_key and target.start_date < source.start_date
# MAGIC WHEN MATCHED THEN UPDATE SET end_date = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE INSERTED_TODAY
