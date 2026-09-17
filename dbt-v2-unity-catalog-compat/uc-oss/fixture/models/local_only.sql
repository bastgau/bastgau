-- Runs entirely in the local DuckDB file: proves the engine works with no warehouse.
{{ config(materialized='table') }}
select 1 as id, 'alice' as name
