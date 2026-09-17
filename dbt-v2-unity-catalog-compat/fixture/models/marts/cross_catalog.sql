{{ config(materialized='table', catalog='gold_prod', schema='analytics') }}
select c.id as c_id from {{ ref('dim_customers') }} as c
