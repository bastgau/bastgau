-- Same join and same VARIANT flattening as the warehouse version: the SQL runs on the
-- notebook's compute here, so the Databricks colon syntax is available.
{{ config(materialized='table', file_format='delta') }}

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
    to_json(c.metadata)              as metadata_json
from {{ source('users_src', 'users') }} as u
left join {{ source('cities_src', 'cities') }} as c
       on c.city = u.city
