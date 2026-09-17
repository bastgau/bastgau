#!/usr/bin/env bash
# Phase 1 - offline compatibility checks for dbt v2 against Databricks / Unity Catalog.
# No warehouse credentials required: nothing here opens a SQL session.
# Usage: ./run_offline_checks.sh [dbt_version]   (default: 2.0.4)
set -uo pipefail

# The offline phase must not be influenced by live credentials.
unset DBT_HOST DBT_HTTP_PATH DBT_TOKEN DBT_CATALOG DBT_SCHEMA DBT_CROSS_CATALOG \
      DBT_SOURCE_CATALOG DBT_SOURCE_SCHEMA DBT_SOURCE_TABLE DBT_SOURCE_TS_COLUMN \
      DBT_EXT_CATALOG DBT_EXT_SCHEMA

DBT_VERSION="${1:-2.0.4}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK_DIR:-$(mktemp -d)}"
FIX="$WORK/fixture"
VENV="$WORK/.venv"
PASS=0; FAIL=0

say()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
ko()   { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }
# assert_contains <label> <needle> <haystack>
assert_contains() { if grep -qF -- "$2" <<<"$3"; then ok "$1"; else ko "$1 (expected to find: $2)"; fi; }
assert_absent()   { if grep -qF -- "$2" <<<"$3"; then ko "$1 (unexpected: $2)"; else ok "$1"; fi; }

say "0. Install dbt $DBT_VERSION (no adapter package)"
python3 -m venv "$VENV" >/dev/null
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q "dbt==$DBT_VERSION" || { ko "pip install dbt==$DBT_VERSION"; exit 1; }
DBT="$VENV/bin/dbt"
VER="$("$DBT" --version 2>&1)"
assert_contains "dbt CLI reports $DBT_VERSION" "$DBT_VERSION" "$VER"
FREEZE="$("$VENV/bin/pip" freeze)"
assert_absent "no dbt-databricks adapter package needed" "dbt-databricks" "$FREEZE"

cp -r "$HERE/fixture" "$FIX"
D() { (cd "$FIX" && "$DBT" "$@" --project-dir "$FIX" --profiles-dir "$FIX" --no-manage-state 2>&1); }

say "1. profiles.yml: databricks adapter + Unity Catalog three-level namespace"
OUT="$(D parse)"
assert_contains "dbt parse succeeds on a UC project" "successfully" "$OUT"
OUT="$(D debug --connection)"
assert_contains "adapter resolved as databricks" "adapter type: databricks" "$OUT"

say "2. catalogs.yml: Unity Catalog write integration (type: unity)"
BAD="$(printf 'catalogs:\n  - name: x\n    type: zzz\n    table_format: iceberg\n    config: {}\n' > "$FIX/catalogs.yml.bak" ; cp "$FIX/catalogs.yml" "$FIX/catalogs.keep"; \
      printf 'catalogs:\n  - name: x\n    type: zzz\n    table_format: iceberg\n    config: {}\n' > "$FIX/catalogs.yml"; D parse)"
assert_contains "engine advertises 'unity' catalog type" "unity" "$BAD"
cp "$FIX/catalogs.keep" "$FIX/catalogs.yml"; rm -f "$FIX/catalogs.keep" "$FIX/catalogs.yml.bak"
OUT="$(D parse)"
assert_contains "type: unity write integration validates" "successfully" "$OUT"

say "3. Relation resolution: catalog.schema.object"
JSON="$(D list --output json --output-keys name resource_type database schema alias | grep '^{')"
rel() { python3 -c "
import sys,json
want=sys.argv[1]
for l in sys.stdin:
    r=json.loads(l)
    if r.get('name')==want:
        print('.'.join(x for x in [r.get('database'),r.get('schema'),r.get('alias') or r.get('name')] if x)); break
" "$1" <<<"$JSON"; }
assert_contains "model targets profile catalog"      "main.dbt_bastgau.dim_customers"        "$(rel dim_customers)"
assert_contains "source reads a foreign UC catalog"  "raw_prod.crm.customers"                "$(rel customers)"
assert_contains "cross-catalog model honours catalog" "gold_prod.dbt_bastgau_analytics"      "$(rel cross_catalog)"
assert_contains "snapshot honours target_catalog"    "main.snapshots.snap_customers"         "$(rel snap_customers)"
assert_contains "iceberg model stays in real catalog" "main.dbt_bastgau_lakehouse"           "$(rel iceberg_customers)"

say "4. Compiled SQL is fully qualified with UC three-part names"
OUT="$(D compile)"
COMPILED="$(cat "$FIX"/target/compiled/uc_compat/models/marts/mv_customers.sql 2>/dev/null)"
assert_contains "refs render as \`catalog\`.\`schema\`.\`table\`" '`main`.`dbt_bastgau`.`stg_customers`' "$COMPILED"
SRC="$(cat "$FIX"/target/compiled/uc_compat/models/staging/src_customers.sql 2>/dev/null)"
assert_contains "sources render with their own catalog" '`raw_prod`.`crm`.`customers`' "$SRC"
# Introspective models (is_incremental) legitimately need a live warehouse.
assert_contains "introspection is the only offline blocker" "dim_customers" "$OUT"

say "5. Databricks-specific model configs accepted by the v2 schema"
probe_cfg() { # $1 = jinja kwargs
  printf "{{ config(materialized='table', %s) }}\nselect cast(1 as bigint) as id\n" "$1" > "$FIX/models/probe.sql"
  local out; out="$(D parse)"; rm -f "$FIX/models/probe.sql"
  if grep -qE "Unknown key|Ignored unexpected key" <<<"$out"; then ko "config $1 rejected"; else ok "config $1"; fi
}
for c in "file_format='delta'" "location_root='s3://b/p'" "partition_by=['id']" \
         "clustered_by=['id'],buckets=8" "liquid_clustered_by=['id']" "auto_liquid_cluster=true" \
         "zorder=['id']" "tblproperties={'delta.enableChangeDataFeed':'true'}" "table_format='iceberg'" \
         "databricks_compute='wh2'" "include_full_name_in_path=true" "databricks_tags={'env':'dev'}" \
         "incremental_strategy='merge',merge_exclude_columns=['id']" \
         "incremental_strategy='replace_where',incremental_predicates=['1=1']" \
         "incremental_strategy='microbatch',event_time='ts',batch_size='day',begin='2024-01-01'" \
         "grants={'select':['\`account users\`']}" "persist_docs={'relation':true,'columns':true}"; do
  probe_cfg "$c"
done
# Control: the schema must still reject nonsense, otherwise the checks above prove nothing.
printf "{{ config(materialized='table', zzz_bogus_key=1) }}\nselect 1 as id\n" > "$FIX/models/probe.sql"
OUT="$(D parse)"; rm -f "$FIX/models/probe.sql"
assert_contains "control: unknown config key is reported" "zzz_bogus_key" "$OUT"

say "6. Databricks SQL dialect understood by the local static analyser"
probe_sql() { # $1 = label, $2 = sql
  printf '%s\n' "$2" > "$FIX/models/probe.sql"
  local out; out="$(D compile -s probe)"; rm -f "$FIX/models/probe.sql"
  if grep -qE "SyntaxInvalid|dbt0101" <<<"$out"; then ko "dialect: $1"; else ok "dialect: $1"; fi
}
probe_sql "qualify"          "select id, row_number() over (order by id) rn from {{ ref('stg_customers') }} qualify rn = 1"
probe_sql "lateral view"     "select id, x from {{ ref('stg_customers') }} lateral view explode(array(1,2)) t as x"
probe_sql "json : accessor"  "select raw:field::string as f from (select '{\"field\":1}' as raw)"
probe_sql "read_files()"     "select * from read_files('/Volumes/main/default/vol/*.csv', format => 'csv')"
probe_sql "time travel"      "select * from {{ ref('stg_customers') }} version as of 3"
probe_sql "stream()"         "select * from stream(raw_prod.crm.customers)"
probe_sql "IDENTIFIER()"     "select * from identifier('main.dbt_bastgau.stg_customers')"
probe_sql "variant/try_cast" "select try_cast('1' as bigint) as a, parse_json('{}') as v"
printf 'selct 1 as id from from\n' > "$FIX/models/probe.sql"
OUT="$(D compile -s probe)"; rm -f "$FIX/models/probe.sql"
assert_contains "control: invalid SQL is reported" "dbt0101" "$OUT"

say "7. Authentication paths reach the right Databricks endpoints"
OUT="$(D debug --connection --target oauth_m2m)"
assert_contains "OAuth M2M calls the workspace OIDC token endpoint" "/oidc/oauth2/v2.0/token" "$OUT"
OUT="$(D debug --connection)"
assert_contains "PAT opens a SQL warehouse session on http_path" "httpPath=/sql/1.0/warehouses" "$OUT"

say "8. Known gotchas (regression guards)"
cp "$FIX/catalogs.yml" "$FIX/catalogs.keep"
printf 'catalogs:\n  - name: uc_native\n    type: unity\n    table_format: iceberg\n    config:\n      databricks:\n        file_format: parquet\n' > "$FIX/catalogs.yml"
printf "{{ config(materialized='table', catalog_name='uc_native', file_format='delta') }}\nselect 1 as id\n" > "$FIX/models/probe.sql"
OUT="$(D parse)"; rm -f "$FIX/models/probe.sql"; cp "$FIX/catalogs.keep" "$FIX/catalogs.yml"; rm -f "$FIX/catalogs.keep"
assert_contains "gotcha 1: file_format=delta conflicts with native UC Iceberg" "requires file_format: parquet" "$OUT"
printf "{{ config(materialized='table', catalog_name='uc_iceberg_uniform') }}\nselect 1 as id\n" > "$FIX/models/probe.sql"
JSON="$(D list --output json --output-keys name database | grep '^{')"; rm -f "$FIX/models/probe.sql"
DB="$(python3 -c "
import sys,json
for l in sys.stdin:
    r=json.loads(l)
    if r.get('name')=='probe': print(r.get('database'))
" <<<"$JSON")"
assert_contains "gotcha 2: catalog_name leaks into the UC catalog when catalog is unset" "uc_iceberg_uniform" "$DB"

say "Result: $PASS passed, $FAIL failed   (workdir: $WORK)"
[ "$FAIL" -eq 0 ]
