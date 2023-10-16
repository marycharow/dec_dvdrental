# Databricks notebook source
from pyspark.sql.functions import col, json_tuple, current_timestamp, concat_ws, md5
from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType,TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC # Build Gold One Big Table Using fact_rentals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create preset_rental_obt

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table main.default.preset_rental_obt as
# MAGIC   select r.rental_key,
# MAGIC          r.rental_date,
# MAGIC          year(r.rental_date) as rental_year,
# MAGIC          month(r.rental_date) as rental_month,
# MAGIC          day(r.rental_date) as rental_day,
# MAGIC          case when dayofweek(r.rental_date)= 1 then 'Sunday'
# MAGIC          when dayofweek(r.rental_date)= 2 then 'Monday'
# MAGIC          when dayofweek(r.rental_date)= 3 then 'Tuesday'
# MAGIC          when dayofweek(r.rental_date)= 4 then 'Wednesday'
# MAGIC          when dayofweek(r.rental_date)= 5 then 'Thursday'
# MAGIC          when dayofweek(r.rental_date)= 6 then 'Friday'
# MAGIC          when dayofweek(r.rental_date)= 7 then 'Saturday'
# MAGIC          else 'Other' end as rental_day_of_week,
# MAGIC          r.customer_key,
# MAGIC          cx.is_active_customer,
# MAGIC          r.store_key,
# MAGIC          s.city as store_city,
# MAGIC          s.country as store_country,
# MAGIC          s.district as store_district,
# MAGIC          r.film_key,
# MAGIC          f.title,
# MAGIC          f.release_year,
# MAGIC          f.language,
# MAGIC          f.rating,
# MAGIC          f.rental_duration,
# MAGIC          r.category_key,
# MAGIC          ct.name
# MAGIC   from main.default.fact_rentals r 
# MAGIC   left join main.default.dim_customer cx on r.customer_key = cx.CUSTOMER_KEY
# MAGIC   left join main.default.dim_store s on r.store_key = s.store_key
# MAGIC   left join main.default.dim_film f on r.film_key = f.film_key
# MAGIC   left join main.default.dim_category ct on r.category_key = ct.category_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table hive_metastore.default.preset_rental_obt as
# MAGIC select *
# MAGIC from main.default.preset_rental_obt;

# COMMAND ----------


