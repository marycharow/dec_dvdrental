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
# MAGIC # Fix
# MAGIC ## What? Manual verification of stg_fact_rentals

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.stg_fact_rentals
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
# MAGIC ## Step 2: Build final fact_rentals
# MAGIC ### To-do
# MAGIC 1. Generate *_key columns
# MAGIC 2. TBD

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


