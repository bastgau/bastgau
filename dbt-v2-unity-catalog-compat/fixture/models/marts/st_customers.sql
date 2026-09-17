{{ config(materialized='streaming_table') }}
select * from stream({{ source('bronze', 'customers') }})
