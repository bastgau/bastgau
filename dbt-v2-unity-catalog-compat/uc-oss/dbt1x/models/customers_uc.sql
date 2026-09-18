-- Written as Delta by the uc_delta plugin, then registered in Unity Catalog OSS
-- through its native REST API. `location` is only the parquet staging file.
{{ config(materialized='external', plugin='uc',
          location=env_var('UC_STAGE', '/tmp/uc_stage') ~ '/customers_uc.parquet') }}
select cast(1 as bigint) as id, 'alice' as name, cast(12.5 as double) as amount
union all
select cast(2 as bigint) as id, 'bob' as name, cast(7.0 as double) as amount
