{{ config(materialized='view') }}
select * from {{ source('bronze', 'seed_bronze_v1') }}
