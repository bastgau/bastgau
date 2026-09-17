{{ config(materialized='materialized_view') }}
select id, count(*) as n from {{ ref('stg_customers') }} group by id
