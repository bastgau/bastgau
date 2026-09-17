-- Upstream is a seed so the live phase is self-contained.
select cast(id as bigint) as id,
       cast(name as string) as name,
       cast(_ingested_at as timestamp) as _ingested_at
from {{ ref('seed_customers') }}
