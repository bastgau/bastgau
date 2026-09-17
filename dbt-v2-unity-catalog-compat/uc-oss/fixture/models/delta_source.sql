-- Reads a real Delta table with the DuckDB `delta` extension (declared in the profile).
-- This is the path that works locally: dbt reads Delta without any warehouse.
{{ config(materialized='table') }}
select id, name, payload_json, cast(payload_json as json) as payload
from delta_scan('{{ env_var("UC_OSS_DELTA_PATH", "file:///tmp/delta/customers") }}')
