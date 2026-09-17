-- Delta table carrying a VARIANT column. Databricks turns on the Delta table features
-- `variantType` and `variantShredding` by itself when such a column appears.
-- The to_json projection exists because VARIANT has no equality/ordering in Databricks:
-- snapshots and `unique` tests must compare that column, never the VARIANT itself.
{{ config(materialized='table',
          file_format='delta',
          tblproperties={'delta.enableChangeDataFeed': 'true'}) }}
select
    cast(id as bigint)                           as id,
    parse_json(raw)                              as payload,
    to_json(parse_json(raw))                     as payload_json,
    payload:user.name::string                    as user_name,
    variant_get(payload, '$.amount', 'double')   as amount,
    try_variant_get(payload, '$.missing', 'int') as missing_field,
    schema_of_variant(payload)                   as inferred_schema
from (
    select 1 as id, '{"user":{"name":"alice"},"amount":12.5}' as raw
    union all
    select 2 as id, '{"user":{"name":"bob"},"amount":7}'      as raw
)
