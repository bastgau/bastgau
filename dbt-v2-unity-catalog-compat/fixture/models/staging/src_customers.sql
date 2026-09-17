-- Exercises source resolution across a foreign UC catalog.
-- Live: only runnable when DBT_SOURCE_* point at a table you can read.
{{ config(materialized='view') }}
select * from {{ source('bronze', 'customers') }}
