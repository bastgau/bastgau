{% snapshot snap_v1 %}
{{ config(target_schema='snapshots_v1', unique_key='id',
          strategy='check', check_cols=['name']) }}
select * from {{ ref('dim_customers') }}
{% endsnapshot %}
