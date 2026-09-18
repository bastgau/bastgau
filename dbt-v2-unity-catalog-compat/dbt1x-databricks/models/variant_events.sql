{{ config(materialized='table', file_format='delta') }}
select cast(1 as bigint) as id,
       parse_json('{"user":{"name":"alice"},"amount":12.5}') as payload,
       to_json(parse_json('{"user":{"name":"alice"},"amount":12.5}')) as payload_json
