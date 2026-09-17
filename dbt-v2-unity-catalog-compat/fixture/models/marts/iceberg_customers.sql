-- UC-managed Iceberg table; `catalog` pins the real UC catalog, `catalog_name`
-- selects the write integration declared in catalogs.yml.
{{ config(materialized='table',
          catalog_name='uc_iceberg_uniform',
          catalog=env_var('DBT_CATALOG', 'main'),
          schema='lakehouse') }}
select id from {{ ref('stg_customers') }}
