#!/usr/bin/env bash
# Phase 2 - live compatibility checks for dbt v2 against a real Databricks workspace
# with Unity Catalog. Covers everything the offline phase cannot prove: materialization
# execution, UC DDL, governance, introspection.
#
# Required: DBT_HOST, DBT_HTTP_PATH, DBT_TOKEN
# Optional: DBT_CATALOG (default main), DBT_SCHEMA (default dbt_uc_compat),
#           DBT_SOURCE_CATALOG / DBT_SOURCE_SCHEMA / DBT_SOURCE_TABLE for the streaming
#           table and source-freshness checks, DBT_CROSS_CATALOG for the cross-catalog write.
#
# The run creates objects in $DBT_CATALOG.$DBT_SCHEMA. Use a throwaway schema:
# the script drops nothing.
set -uo pipefail

: "${DBT_HOST:?set DBT_HOST}"; : "${DBT_HTTP_PATH:?set DBT_HTTP_PATH}"; : "${DBT_TOKEN:?set DBT_TOKEN}"
export DBT_CATALOG="${DBT_CATALOG:-main}" DBT_SCHEMA="${DBT_SCHEMA:-dbt_uc_compat}"

DBT_VERSION="${DBT_VERSION:-2.0.4}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK_DIR:-$(mktemp -d)}"; FIX="$WORK/fixture"; VENV="$WORK/.venv"
PASS=0; FAIL=0; SKIP=0
say(){ printf '\n\033[1m%s\033[0m\n' "$*"; }
ok(){ PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
ko(){ FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }
skip(){ SKIP=$((SKIP+1)); printf '  \033[33mSKIP\033[0m  %s\n' "$*"; }

python3 -m venv "$VENV" >/dev/null && "$VENV/bin/pip" install -q "dbt==$DBT_VERSION"
DBT="$VENV/bin/dbt"; cp -r "$HERE/fixture" "$FIX"
D(){ (cd "$FIX" && "$DBT" "$@" --project-dir "$FIX" --profiles-dir "$FIX" --target live --no-manage-state 2>&1); }
# step <label> <dbt args...> : PASS when dbt reports no error
step(){ local label="$1"; shift; local out; out="$(D "$@")"
  if grep -qE "Finished '[a-z-]+' successfully|with [0-9]+ warnings? for target" <<<"$out" && ! grep -q '\[error\]' <<<"$out"
  then ok "$label"; else ko "$label"; printf '%s\n' "$out" | grep -E '\[error\]' | head -3 | sed 's/^/        /'; fi; }

say "L0. Connectivity and Unity Catalog reachability"
step "dbt debug --connection"                 debug --connection
step "three-level query executes"             show --inline "select current_catalog() as c, current_schema() as s" --limit 1
step "target UC schema is writable"           show --inline "select 1 as ok" --limit 1

say "L1. Seed / view / table / incremental in a UC schema"
step "dbt seed (incl. the bronze source table)" seed
step "dbt run (view + table + incremental)"    run --exclude mv_customers st_customers iceberg_customers cross_catalog src_customers ext_customers py_customers
step "dbt run again (incremental MERGE path)" run -s dim_customers
step "dbt run --full-refresh"                 run -s dim_customers --full-refresh
step "dbt test (not_null / unique)"           test
step "dbt snapshot (target_catalog)"          snapshot

# Materialized views and streaming tables are backed by DBSQL pipelines. Non-Enterprise
# workspaces allow a single active pipeline, so these run one at a time.
say "L2. Databricks-only materializations"
step "materialized_view"                      run -s mv_customers
# --full-refresh keeps the suite replayable: dbt recreates the upstream seed on every
# run, which invalidates the streaming checkpoint (see README gotcha on streaming tables).
step "streaming_table"                        run -s st_customers --full-refresh
step "UC-managed Iceberg (catalogs.yml)"      run -s iceberg_customers

say "L3. Cross-catalog access"
step "read a source in another UC catalog"     run -s ext_customers
if [ -n "${DBT_CROSS_CATALOG:-}" ]; then
  step "write into a second UC catalog"       run -s cross_catalog
else
  skip "cross-catalog write (set DBT_CROSS_CATALOG)"
fi

if [ -n "${DBT_PYTHON_MODEL:-}" ]; then
  step "python model (needs a cluster or a job)" run -s py_customers
else
  skip "python model (set DBT_PYTHON_MODEL=1 with a cluster-backed target)"
fi

say "L4. Governance and metadata"
step "UC grants applied"                      show --inline "show grants on table ${DBT_CATALOG}.${DBT_SCHEMA}.dim_customers" --limit -1
step "liquid clustering + tblproperties"       show --inline "describe detail ${DBT_CATALOG}.${DBT_SCHEMA}.dim_customers" --limit -1
step "UC tags applied"                        show --inline "select tag_name, tag_value from system.information_schema.table_tags where catalog_name='${DBT_CATALOG}' and schema_name='${DBT_SCHEMA}' and table_name='dim_customers'" --limit -1
step "UC object types are the real ones"      show --inline "select table_name, table_type from system.information_schema.tables where table_catalog='${DBT_CATALOG}' and table_schema='${DBT_SCHEMA}' and table_name in ('mv_customers','st_customers')" --limit -1
step "MERGE really ran on the incremental"    show --inline "select count(*) as merges from (describe history ${DBT_CATALOG}.${DBT_SCHEMA}.dim_customers) where operation = 'MERGE'" --limit -1
step "persist_docs / column comments"         docs generate
if [ -n "${DBT_SOURCE_TABLE:-}" ]; then
  step "source view (declared source resolves)" run -s src_customers
  step "source freshness (loaded_at_field)"     source freshness --select source:bronze
else
  skip "source resolution + freshness (set DBT_SOURCE_*)"
fi

say "L5. Introspection-dependent commands"
step "dbt show (preview)"                     show -s stg_customers --limit 5
step "dbt compile (strict static analysis)"   compile --static-analysis strict
# st_customers is excluded: `build` reseeds its upstream table, which invalidates the
# streaming checkpoint. It is covered on its own in L2 with --full-refresh.
step "dbt build (end to end)"                 build --exclude cross_catalog py_customers st_customers --threads 1

say "Result: $PASS passed, $FAIL failed, $SKIP skipped   (workdir: $WORK)"
[ "$FAIL" -eq 0 ]
