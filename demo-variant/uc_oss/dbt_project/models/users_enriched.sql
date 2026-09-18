-- Join the two Unity Catalog OSS catalogs and flatten the VARIANT metadata.
--
-- The colon path (metadata:population::bigint) is the same syntax as on Databricks: it
-- landed in Spark 4.1 — on Spark 4.0 it is a PARSE_SYNTAX_ERROR and variant_get() is the
-- only form, e.g. variant_get(c.metadata, '$.population', 'bigint').
--
-- Incremental on purpose. A `table` materialization emits CREATE OR REPLACE TABLE … AS
-- SELECT, and the UC OSS connector (0.4.1) resolves the LOCATION of that statement
-- against spark_catalog instead of the Unity Catalog one. The calling script pre-creates
-- the external table, and dbt merges into it.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id',
    file_format='delta'
) }}

select
    u.user_id,
    u.full_name,
    u.email,
    u.city,
    u.signed_up,
    c.country,
    c.metadata:population::bigint   as population,
    c.metadata:region::string        as region,
    c.metadata:coords.lat::double    as latitude,
    c.metadata:coords.lon::double    as longitude,
    c.metadata:tags[0]::string       as primary_tag,
    -- VARIANT has neither equality nor ordering: keep a comparable text projection
    to_json(c.metadata)              as metadata_json
from {{ source('users_src', 'users') }} as u
left join {{ source('cities_src', 'cities') }} as c
       on c.city = u.city
