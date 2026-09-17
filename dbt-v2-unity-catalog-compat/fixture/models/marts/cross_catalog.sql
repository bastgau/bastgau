-- Write into a second UC catalog.
{{ config(materialized='table',
          catalog=env_var('DBT_CROSS_CATALOG', 'gold_prod'),
          schema='analytics') }}
select c.id as c_id from {{ ref('dim_customers') }} as c
