# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Create the two source tables in Unity Catalog
# MAGIC
# MAGIC Two **Delta** tables, in **two different catalogs**:
# MAGIC
# MAGIC * `demo_users.core.users` — 100 fake users, each with a city
# MAGIC * `demo_cities.core.cities` — city reference, with a **VARIANT** column holding metadata
# MAGIC
# MAGIC Nothing here needs a cluster: it runs on serverless as well, since it is plain SQL
# MAGIC over the notebook's own session.

# COMMAND ----------

dbutils.widgets.text("users_catalog", "demo_users", "Catalog for the users table")
dbutils.widgets.text("cities_catalog", "demo_cities", "Catalog for the cities table")
dbutils.widgets.text("schema", "core", "Schema in both catalogs")

users_catalog = dbutils.widgets.get("users_catalog").strip()
cities_catalog = dbutils.widgets.get("cities_catalog").strip()
schema = dbutils.widgets.get("schema").strip()

# COMMAND ----------

# MAGIC %md ## Catalogs and schemas

# COMMAND ----------

for catalog in (users_catalog, cities_catalog):
    spark.sql(f"create catalog if not exists {catalog}")
    spark.sql(f"create schema if not exists {catalog}.{schema}")
    print(f"ready: {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Table 1 — 100 users with a city
# MAGIC Deterministic on purpose: no faker, no randomness, so two runs give the same rows.

# COMMAND ----------

spark.sql(f"""
create or replace table {users_catalog}.{schema}.users (
    user_id   bigint,
    full_name string,
    email     string,
    city      string,
    signed_up date
)
using delta
""")

spark.sql(f"""
insert into {users_catalog}.{schema}.users
with seq as (select explode(sequence(1, 100)) as n),
     cities as (
        select explode(array('Paris','Lyon','Marseille','Bordeaux','Lille','Nantes')) as city,
               1 as dummy
     ),
     ranked as (select city, row_number() over (order by city) as idx from cities)
select
    cast(s.n as bigint)                                             as user_id,
    concat('User ', lpad(cast(s.n as string), 3, '0'))              as full_name,
    concat('user', lpad(cast(s.n as string), 3, '0'), '@example.com') as email,
    r.city                                                          as city,
    date_add(date'2026-01-01', cast(s.n as int))                     as signed_up
from seq s
join ranked r on r.idx = pmod(s.n, 6) + 1
""")

print(spark.table(f"{users_catalog}.{schema}.users").count(), "users")
display(spark.sql(f"select * from {users_catalog}.{schema}.users order by user_id limit 5"))

# COMMAND ----------

# MAGIC %md ## Table 2 — city reference with a VARIANT column
# MAGIC `parse_json` builds the VARIANT. Databricks turns on the Delta table features
# MAGIC `variantType` and `variantShredding` by itself as soon as such a column exists.

# COMMAND ----------

spark.sql(f"""
create or replace table {cities_catalog}.{schema}.cities (
    city     string,
    country  string,
    metadata variant
)
using delta
""")

spark.sql(f"""
insert into {cities_catalog}.{schema}.cities
select city, 'FR' as country, parse_json(payload) as metadata
from values
    ('Paris',     '{{"population": 2133111, "region": "Ile-de-France",     "coords": {{"lat": 48.8566, "lon": 2.3522}},  "tags": ["capital","tier1"]}}'),
    ('Lyon',      '{{"population": 522250,  "region": "Auvergne-Rhone-Alpes","coords": {{"lat": 45.7640, "lon": 4.8357}}, "tags": ["tier2","gastronomy"]}}'),
    ('Marseille', '{{"population": 873076,  "region": "Provence-Alpes-Cote d Azur","coords": {{"lat": 43.2965, "lon": 5.3698}}, "tags": ["port","tier2"]}}'),
    ('Bordeaux',  '{{"population": 259809,  "region": "Nouvelle-Aquitaine","coords": {{"lat": 44.8378, "lon": -0.5792}}, "tags": ["wine","tier2"]}}'),
    ('Lille',     '{{"population": 236234,  "region": "Hauts-de-France",  "coords": {{"lat": 50.6292, "lon": 3.0573}},  "tags": ["tier3"]}}'),
    ('Nantes',    '{{"population": 320732,  "region": "Pays de la Loire", "coords": {{"lat": 47.2184, "lon": -1.5536}}, "tags": ["tier3","atlantic"]}}')
    as t(city, payload)
""")

display(spark.sql(f"""
select city, country,
       metadata:population::bigint            as population,
       metadata:region::string                as region,
       schema_of_variant(metadata)            as variant_schema
from {cities_catalog}.{schema}.cities order by city
"""))

# COMMAND ----------

# MAGIC %md ## Check what landed in Unity Catalog

# COMMAND ----------

display(spark.sql(f"""
select table_catalog, table_schema, table_name, table_type, data_source_format
from system.information_schema.tables
where (table_catalog = '{users_catalog}' or table_catalog = '{cities_catalog}')
  and table_schema = '{schema}'
order by table_catalog
"""))

display(spark.sql(f"""
select column_name, full_data_type
from system.information_schema.columns
where table_catalog = '{cities_catalog}' and table_schema = '{schema}' and table_name = 'cities'
order by ordinal_position
"""))

dbutils.notebook.exit(
    f"{users_catalog}.{schema}.users + {cities_catalog}.{schema}.cities ready (Delta, VARIANT)"
)
