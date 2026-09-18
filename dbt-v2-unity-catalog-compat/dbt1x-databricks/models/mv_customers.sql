{{ config(materialized='materialized_view') }}
select id, count(*) as n from {{ ref('dim_customers') }} group by id
