-- Join the two catalogs and flatten the VARIANT metadata into typed columns.
{{ config(materialized='table', file_format='delta') }}

select
    u.user_id,
    u.full_name,
    u.email,
    u.city,
    u.signed_up,
    c.country,
    -- VARIANT flattening: colon path + cast, one column per field
    c.metadata:population::bigint            as population,
    c.metadata:region::string                as region,
    c.metadata:coords.lat::double            as latitude,
    c.metadata:coords.lon::double            as longitude,
    c.metadata:tags[0]::string               as primary_tag,
    -- the raw payload kept as text, so snapshots and `unique` tests have something
    -- comparable: VARIANT has neither equality nor ordering in Databricks
    to_json(c.metadata)                      as metadata_json
from {{ source('users_src', 'users') }} as u
left join {{ source('cities_src', 'cities') }} as c
       on c.city = u.city
