{{ config(materialized='incremental', incremental_strategy='merge', unique_key='id',
          file_format='delta', liquid_clustered_by=['id'],
          tblproperties={'delta.enableChangeDataFeed': 'true'}) }}
select cast(id as bigint) as id, cast(name as string) as name
from (select 1 as id, 'alice' as name union all select 2 as id, 'bob' as name)
