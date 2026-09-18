-- dbt-databricks 1.x exposes table_format directly; there is no catalogs.yml in 1.x.
{{ config(materialized='table', table_format='iceberg', file_format='delta') }}
select id from {{ ref('dim_customers') }}
