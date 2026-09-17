{{ config(materialized='streaming_table') }}
select * from stream({{ ref('seed_customers') }})
