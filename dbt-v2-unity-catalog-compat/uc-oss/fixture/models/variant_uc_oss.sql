-- Same VARIANT column, but targeting the Unity Catalog OSS catalog. Expected to fail
-- the same way a plain table does: the write path is refused, before any type matters.
{{ config(materialized='table', catalog_name='dbt_oss', schema='analytics') }}
select cast(1 as bigint) as id, cast('{"a":1}' as variant) as payload
