# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5, explode
from delta.tables import DeltaTable

# COMMAND ----------

df_actor = spark.read.table("main.default._airbyte_raw_actor")
df_address = spark.read.table("main.default._airbyte_raw_address")
df_category = spark.read.table("main.default._airbyte_raw_category")
df_city = spark.read.table("main.default._airbyte_raw_city")
df_country = spark.read.table("main.default._airbyte_raw_country")
df_customer = spark.read.table("main.default._airbyte_raw_customer")
df_film = spark.read.table("main.default._airbyte_raw_film")
df_film_actor = spark.read.table("main.default._airbyte_raw_film_actor")
df_film_category = spark.read.table("main.default._airbyte_raw_film_category")
df_inventory = spark.read.table("main.default._airbyte_raw_inventory")
df_language = spark.read.table("main.default._airbyte_raw_language")
df_payment = spark.read.table("main.default._airbyte_raw_payment")
df_rental = spark.read.table("main.default._airbyte_raw_rental")
df_staff = spark.read.table("main.default._airbyte_raw_staff")
df_store = spark.read.table("main.default._airbyte_raw_store")

# COMMAND ----------

df_actor_norm = df_actor.select(json_tuple(col("_airbyte_data"),"actor_id","first_name","last_name","last_update")) \
    .toDF("actor_id","first_name","last_name","last_update")
df_address_norm = df_address.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"address_id","address","address2","district","city_id","postal_code","phone","last_update")) \
    .toDF("_airbyte_ab_id","address_id","address","address2","district","city_id","postal_code","phone","last_update")
df_category_norm = df_category.select(json_tuple(col("_airbyte_data"),"category_id","name","last_update")) \
    .toDF("category_id","name","last_update")
df_city_norm = df_city.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"city_id","city","country_id","last_update")) \
    .toDF("_airbyte_ab_id","city_id","city","country_id","last_update")
df_country_norm = df_country.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"country_id","country","last_update")) \
    .toDF("_airbyte_ab_id","country_id","country","last_update")
df_customer_norm = df_customer.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"customer_id","store_id","first_name","last_name","activebool","create_date","last_update","active")) \
    .toDF("_airbyte_ab_id","customer_id","store_id","first_name","last_name","activebool","create_date","last_update","active")
df_film_norm = df_film.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"film_id","title","description","release_year","language_id","rental_duration","rental_rate","length","replacement_cost","rating","last_update")) \
    .toDF("_airbyte_ab_id","film_id","title","description","release_year","language_id","rental_duration","rental_rate","length","replacement_cost","rating","last_update")
df_film_actor_norm = df_film_actor.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"actor_id","film_id","last_update")) \
    .toDF("_airbyte_ab_id","actor_id","film_id","last_update")
df_film_category_norm = df_film_category.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"film_id","category_id","last_update")) \
    .toDF("_airbyte_ab_id","film_id","category_id","last_update")
df_inventory_norm = df_inventory.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"inventory_id","film_id","store_id","last_update")) \
    .toDF("_airbyte_ab_id","inventory_id","film_id","store_id","last_update")
df_language_norm = df_language.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"language_id","name","last_update")) \
    .toDF("_airbyte_ab_id","language_id","name","last_update")
df_payment_norm = df_payment.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"payment_id","customer_id","staff_id","rental_id","amount","payment_date")) \
    .toDF("_airbyte_ab_id","payment_id","customer_id","staff_id","rental_id","amount","payment_date")
df_rental_norm = df_rental.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"rental_id","rental_date","inventory_id","customer_id","return_date","staff_id","last_update")) \
    .toDF("_airbyte_ab_id","rental_id","rental_date","inventory_id","customer_id","return_date","staff_id","last_update")
df_staff_norm = df_staff.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"staff_id","first_name","last_name","address_id","email","store_id","active","username","password","last_update","picture")) \
    .toDF("_airbyte_ab_id","staff_id","first_name","last_name","address_id","email","store_id","active","username","password","last_update","picture")
df_store_norm = df_store.select(col("_airbyte_ab_id"),json_tuple(col("_airbyte_data"),"store_id","manager_staff_id","address_id","last_update")) \
    .toDF("_airbyte_ab_id","store_id","manager_staff_id","address_id","last_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists main.default.stg_actor;
# MAGIC drop table if exists main.default.stg_address;
# MAGIC drop table if exists main.default.stg_category;
# MAGIC drop table if exists main.default.stg_city;
# MAGIC drop table if exists main.default.stg_country;
# MAGIC drop table if exists main.default.stg_customer;
# MAGIC drop table if exists main.default.stg_film;
# MAGIC drop table if exists main.default.stg_film_actor;
# MAGIC drop table if exists main.default.stg_film_category;
# MAGIC drop table if exists main.default.stg_inventory;
# MAGIC drop table if exists main.default.stg_language;
# MAGIC drop table if exists main.default.stg_payment;
# MAGIC drop table if exists main.default.stg_rental;
# MAGIC drop table if exists main.default.stg_staff;
# MAGIC drop table if exists main.default.stg_store;

# COMMAND ----------

df_actor_norm.write.saveAsTable("main.default.stg_actor")
df_address_norm.write.saveAsTable("main.default.stg_address")
df_category_norm.write.saveAsTable("main.default.stg_category")
df_city_norm.write.saveAsTable("main.default.stg_city")
df_country_norm.write.saveAsTable("main.default.stg_country")
df_customer_norm.write.saveAsTable("main.default.stg_customer")
df_film_norm.write.saveAsTable("main.default.stg_film")
df_film_actor_norm.write.saveAsTable("main.default.stg_film_actor")
df_film_category_norm.write.saveAsTable("main.default.stg_film_category")
df_inventory_norm.write.saveAsTable("main.default.stg_inventory")
df_language_norm.write.saveAsTable("main.default.stg_language")
df_payment_norm.write.saveAsTable("main.default.stg_payment")
df_rental_norm.write.saveAsTable("main.default.stg_rental")
df_staff_norm.write.saveAsTable("main.default.stg_staff")
df_store_norm.write.saveAsTable("main.default.stg_store")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main.default.stg_actor limit 10;
