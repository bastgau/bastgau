{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='id',
    file_format='delta',
    liquid_clustered_by=['id'],
    tblproperties={'delta.enableChangeDataFeed': 'true'}
) }}
select id, name, _ingested_at from {{ ref('stg_customers') }}
{% if is_incremental() %}
where _ingested_at > (select max(_ingested_at) from {{ this }})
{% endif %}
