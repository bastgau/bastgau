{% snapshot snap_customers %}
{{ config(target_catalog='main', target_schema='snapshots', unique_key='id',
          strategy='timestamp', updated_at='_ingested_at') }}
select * from {{ ref('stg_customers') }}
{% endsnapshot %}
