# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Zone: Fact Table: fact_rentals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Staging Table for fact_rentals

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load prerequisite libraries

# COMMAND ----------

from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,TimestampType
from delta.tables import DeltaTable

# stg_fact_rentals Design
# rental_id - stg_rental
# rental_date - stg_rental
# store_id - join stg_staff table on rental.staff_id=staff.staff_id
# customer_id - stg_rental
# return_date - stg_rental
# last_update - stg_rental
# film_id - join stg_inventory table where rental.inventory_id = inventory.inventory_id
# category_id - join stg_film_category where inventory.film_id = film_category.film_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the stg_fact_rentals table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.stg_fact_rentals;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE main.default.stg_fact_rentals AS
# MAGIC with stg_rental as (
# MAGIC     select *
# MAGIC     from main.default.stg_rental
# MAGIC ),
# MAGIC stg_staff as (
# MAGIC     select *
# MAGIC     from main.default.stg_staff
# MAGIC ),
# MAGIC stg_inventory as (
# MAGIC     select *
# MAGIC     from main.default.stg_inventory
# MAGIC ),
# MAGIC stg_film_category as (
# MAGIC     select *
# MAGIC     from main.default.stg_film_category
# MAGIC )
# MAGIC SELECT
# MAGIC     stg_rental.rental_id as rental_id,
# MAGIC     stg_rental.rental_date as rental_date,
# MAGIC     stg_rental.last_update as rental_last_update,
# MAGIC     stg_staff.store_id as store_id,
# MAGIC     stg_rental.customer_id as customer_id,
# MAGIC     stg_rental.return_date as return_date,
# MAGIC     stg_rental.last_update as last_update,
# MAGIC     stg_inventory.film_id as film_id,
# MAGIC     stg_film_category.category_id as category_id
# MAGIC from stg_rental
# MAGIC left join stg_staff on stg_rental.staff_id = stg_staff.staff_id
# MAGIC left join stg_inventory on stg_rental.inventory_id = stg_inventory.inventory_id
# MAGIC left join stg_film_category on stg_inventory.film_id = stg_film_category.film_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate rental key

# COMMAND ----------

# Read in stg_fact_rentals as DF
df_stg_fact_rentals = spark.read.table("main.default.stg_fact_rentals")
# Convert data types
df_stg_fact_rentals = df_stg_fact_rentals.withColumn("rental_id",df_stg_fact_rentals.rental_id.cast(IntegerType())) \
.withColumn("rental_last_update", df_stg_fact_rentals.last_update.cast(TimestampType()))
# Create surrogate key
df_stg_fact_rentals = df_stg_fact_rentals.withColumn("rental_key", md5(concat_ws("-", 
  df_stg_fact_rentals.rental_id, 
  df_stg_fact_rentals.rental_last_update)))


# COMMAND ----------

# Replace stg_fact_rentals
df_stg_fact_rentals.write.saveAsTable("main.default.stg_fact_rentals_updated")

# COMMAND ----------

# MAGIC %md
# MAGIC # BEGIN IGNORE: Manual verification of stg_fact_rentals

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.stg_fact_rentals_updated
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.dim_film
# MAGIC where film_id = "333"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.stg_rental
# MAGIC where customer_id = "459" and rental_id = "2"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.stg_staff
# MAGIC where staff_id = "1"

# COMMAND ----------

# MAGIC %md
# MAGIC # END IGNORE: Manual verification of stg_fact_rentals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build final fact_rentals

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS main.default.stg_fact_rentals;
# MAGIC CREATE TABLE main.default.fact_rentals AS
# MAGIC with stg_fact_rentals as (
# MAGIC     select *
# MAGIC     from main.default.stg_fact_rentals_updated
# MAGIC ),
# MAGIC dim_category as (
# MAGIC     select *
# MAGIC     from main.default.dim_category
# MAGIC ),
# MAGIC dim_customer as (
# MAGIC     select *
# MAGIC     from main.default.dim_customer
# MAGIC ),
# MAGIC dim_film as (
# MAGIC     select *
# MAGIC     from main.default.dim_film
# MAGIC ),
# MAGIC dim_store as (
# MAGIC     select *
# MAGIC     from main.default.dim_store
# MAGIC )
# MAGIC SELECT
# MAGIC     stg_fact_rentals.rental_key as rental_key,
# MAGIC     stg_fact_rentals.rental_date as rental_date,
# MAGIC     dim_store.store_key as store_key,
# MAGIC     dim_customer.customer_key as customer_key,
# MAGIC     stg_fact_rentals.return_date as return_date,
# MAGIC     stg_fact_rentals.last_update as last_update,
# MAGIC     dim_film.film_key as film_key,
# MAGIC     dim_category.category_key as category_key
# MAGIC from stg_fact_rentals
# MAGIC left join dim_category on stg_fact_rentals.category_id = dim_category.category_id
# MAGIC left join dim_customer on stg_fact_rentals.customer_id = dim_customer.customer_id
# MAGIC left join dim_film on stg_fact_rentals.film_id = dim_film.film_id
# MAGIC left join dim_store on stg_fact_rentals.store_id = dim_store.store_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read stg_fact_rentals in as a dataframe

# COMMAND ----------

df_stg_fact_rentals = spark.read.table("main.default.fact_rentals")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests with Great Expectations

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test stg_fact_rentals

# COMMAND ----------

ge_df_stg_fact_rentals = SparkDFDataset(df_stg_fact_rentals)
expectation_stg_fact_rentalsrental_key = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("rental_key")
if not expectation_stg_fact_rentalsrental_key["success"]: 
    raise Exception(expectation_stg_fact_rentalsrental_key)
expectation_stg_fact_rentals_rental_date = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("rental_date")
if not expectation_stg_fact_rentals_rental_date["success"]: 
    raise Exception(expectation_stg_fact_rentals_rental_date)
expectation_stg_fact_rentals_store_key = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("store_key")
if not expectation_stg_fact_rentals_store_key["success"]: 
    raise Exception(expectation_stg_fact_rentals_store_key)
expectation_stg_fact_rentals_customer_key = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("customer_key")
if not expectation_stg_fact_rentals_customer_key["success"]: 
    raise Exception(expectation_stg_fact_rentals_customer_key)
# expectation_stg_fact_rentals_return_date = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("return_date")
# if not expectation_stg_fact_rentals_return_date["success"]: 
#     raise Exception(expectation_stg_fact_rentals_return_date)
expectation_stg_fact_rentals_last_update = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("last_update")
if not expectation_stg_fact_rentals_last_update["success"]: 
    raise Exception(expectation_stg_fact_rentals_last_update)
expectation_stg_fact_rentals_film_key = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("film_key")
if not expectation_stg_fact_rentals_film_key["success"]: 
    raise Exception(expectation_stg_fact_rentals_film_key)
expectation_stg_fact_rentals_category_key = ge_df_stg_fact_rentals.expect_column_values_to_not_be_null("category_key")
if not expectation_stg_fact_rentals_category_key["success"]: 
    raise Exception(expectation_stg_fact_rentals_category_key)

# COMMAND ----------

# df_fact_rentals = df_stg_fact_rentals.select("rental_key", "rental_date", "store_key", "customer_key", "return_date", "last_update", "film_key", "category_key")
display(df_stg_fact_rentals)

# COMMAND ----------

df_stg_fact_rentals.write.saveAsTable("main.default.fact_rentals_updated")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table hive_metastore.default.fact_rentals as
# MAGIC select *
# MAGIC from main.default.fact_rentals_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.default.dim_customer
# MAGIC where customer_key = "1021011d7db10736e7602d0d0280029a";
# MAGIC

# COMMAND ----------


