-- Cross-catalog read: this source lives in another Unity Catalog catalogue.
{{ config(materialized='view') }}
select c_custkey as id, c_name as name
from {{ source('ext', 'customer') }}
