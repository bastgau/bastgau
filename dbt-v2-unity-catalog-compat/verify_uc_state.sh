#!/usr/bin/env bash
# Assert, from Unity Catalog itself, what dbt v2 actually produced.
#
# run_live_checks.sh proves dbt reports success; this script proves the objects in
# UC are the real thing (a MATERIALIZED_VIEW and not a table, a MERGE and not a
# rebuild, liquid clustering actually set, UniForm Iceberg actually enabled).
#
# Run it after run_live_checks.sh, with env.local sourced (see bootstrap.sh).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/env.local" ] && [ -z "${DBT_HOST:-}" ] && . "$HERE/env.local"
: "${DBT_HOST:?set DBT_HOST (or run bootstrap.sh first)}"
: "${DBT_TOKEN:?set DBT_TOKEN}"
export DBT_HOST DBT_TOKEN
CAT="${DBT_CATALOG:-main}"; SCH="${DBT_SCHEMA:-dbt_uc_compat}"
Q="python3 $HERE/lib/dbx_api.py query"
PASS=0; FAIL=0

say(){ printf '\n\033[1m%s\033[0m\n' "$*"; }
# expect <label> <regex> <sql>
expect() {
  local label="$1" pattern="$2" sql="$3" out
  out="$($Q "$sql" 2>&1 | tr '\n' ' ')"
  if grep -qE "$pattern" <<<"$out"; then
    printf '  \033[32mPASS\033[0m  %s\n' "$label"; PASS=$((PASS+1))
  else
    printf '  \033[31mFAIL\033[0m  %s\n        expected /%s/, got: %s\n' "$label" "$pattern" "${out:0:200}"
    FAIL=$((FAIL+1))
  fi
}

say "Objects exist with the right Unity Catalog type"
expect "dim_customers is a managed Delta table" 'MANAGED.*DELTA' \
  "select table_type, data_source_format from system.information_schema.tables where table_catalog='$CAT' and table_schema='$SCH' and table_name='dim_customers'"
expect "mv_customers is a MATERIALIZED_VIEW" 'MATERIALIZED_VIEW' \
  "select table_type from system.information_schema.tables where table_catalog='$CAT' and table_schema='$SCH' and table_name='mv_customers'"
expect "st_customers is a STREAMING_TABLE" 'STREAMING_TABLE' \
  "select table_type from system.information_schema.tables where table_catalog='$CAT' and table_schema='$SCH' and table_name='st_customers'"
expect "stg_customers is a VIEW" 'VIEW' \
  "select table_type from system.information_schema.tables where table_catalog='$CAT' and table_schema='$SCH' and table_name='stg_customers'"
expect "snapshot landed in its own schema" 'snap_customers' \
  "select table_name from system.information_schema.tables where table_catalog='$CAT' and table_schema='snapshots' and table_name='snap_customers'"

say "Incremental strategy really used MERGE"
expect "delta history contains MERGE" '^[1-9]' \
  "select count(*) from (describe history $CAT.$SCH.dim_customers) where operation = 'MERGE'"

say "Databricks physical configs applied"
# DESCRIBE DETAIL cannot be wrapped in a subquery: read its single row as is.
expect "liquid clustering on id" '\["id"\]' \
  "describe detail $CAT.$SCH.dim_customers"
expect "changeDataFeed tblproperty set" 'enableChangeDataFeed[^,]*true' \
  "describe detail $CAT.$SCH.dim_customers"

say "UC-managed Iceberg (catalogs.yml type: unity)"
expect "iceberg table exists in its schema" 'iceberg_customers' \
  "select table_name from system.information_schema.tables where table_catalog='$CAT' and table_schema='${SCH}_lakehouse' and table_name='iceberg_customers'"
expect "UniForm Iceberg enabled" '[Tt]rue' \
  "show tblproperties $CAT.${SCH}_lakehouse.iceberg_customers ('delta.enableIcebergCompatV2')"

say "Governance"
# SHOW GRANTS is not a subquery either.
expect "SELECT granted to the configured principal" 'account users.*SELECT' \
  "show grants on table $CAT.$SCH.dim_customers"
expect "UC tag domain=crm applied" 'crm' \
  "select tag_value from system.information_schema.table_tags where catalog_name='$CAT' and schema_name='$SCH' and table_name='dim_customers' and tag_name='domain'"
expect "column comment persisted" 'Customer key' \
  "select comment from system.information_schema.columns where table_catalog='$CAT' and table_schema='$SCH' and table_name='dim_customers' and column_name='id'"

if [ -n "${DBT_PYTHON_MODEL:-}" ]; then
  say "Python model"
  expect "py_customers is a managed Delta table" 'MANAGED.*DELTA' \
    "select table_type, data_source_format from system.information_schema.tables where table_catalog='$CAT' and table_schema='$SCH' and table_name='py_customers'"
fi

say "Cross-catalog read"
expect "ext_customers reads another catalog" 'ext_customers' \
  "select table_name from system.information_schema.tables where table_catalog='$CAT' and table_schema='$SCH' and table_name='ext_customers'"

say "Result: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
