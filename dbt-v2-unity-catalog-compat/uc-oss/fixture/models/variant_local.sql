-- DuckDB has its own VARIANT type, with different function names than Databricks:
-- no parse_json / variant_get / `:` accessor — variant_extract and casts instead.
{{ config(materialized='table') }}
select cast(1 as bigint)                                as id,
       cast('{"a":1}' as variant)                       as payload,
       typeof(cast('{"a":1}' as variant))               as payload_type,
       variant_extract(cast('{"a":1}' as variant), 'a') as extracted,
       cast(cast('{"a":1}' as variant) as json)         as payload_json
