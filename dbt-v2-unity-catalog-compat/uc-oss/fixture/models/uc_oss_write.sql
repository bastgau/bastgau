-- Targets the Unity Catalog OSS catalog. Expected to fail: UC OSS 0.3.0 exposes a
-- read-only Iceberg REST catalog (see the README).
{{ config(materialized='table', catalog_name='dbt_oss', schema='analytics') }}
select 1 as id, 'alice' as name
