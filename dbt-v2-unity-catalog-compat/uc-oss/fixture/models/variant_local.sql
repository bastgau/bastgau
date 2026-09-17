-- DuckDB has its own VARIANT type, with a different idiom than Databricks.
--   * `cast('{"a":1}' as variant)` silently yields a VARIANT holding a *string*
--     (variant_typeof = VARCHAR) and nothing can be extracted from it;
--   * JSON -> VARIANT is the real equivalent of Databricks' parse_json():
--     variant_typeof = OBJECT(a), and variant_extract() then works.
-- `v:a` is NOT a path accessor here: in DuckDB `alias: expr` is the prefix-alias
-- syntax, so `v:a` means `a AS v`.
{{ config(materialized='table') }}
select
    cast(1 as bigint)                                           as id,
    cast(cast('{"user":{"name":"alice"},"amount":12.5}' as json) as variant) as payload,
    variant_typeof(cast(cast('{"a":1}' as json) as variant))    as object_variant_type,
    variant_typeof(cast('{"a":1}' as variant))                  as string_variant_type,
    cast(variant_extract(
        cast(cast('{"user":{"name":"alice"}}' as json) as variant), 'user') as json) as extracted_user,
    cast(cast('{"amount":12.5}' as json) as variant) :: json     as payload_json
