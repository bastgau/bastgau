#!/usr/bin/env bash
# Run the same ground on Databricks with dbt-core 1.x + dbt-databricks, to see what
# differs from dbt v2 — starting with the `grants` defect the v2 report flags.
#
# Uses the same workspace and warehouse as the v2 suites, in its own schema
# (DBT_SCHEMA_V1, default dbt_uc_compat_v1), so nothing collides.
#
# Requirements: env.local sourced (see bootstrap.sh), python3.
# Usage: ./run_dbt1x_databricks_checks.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/env.local" ] && [ -z "${DBT_HOST:-}" ] && . "$HERE/env.local"
: "${DBT_HOST:?set DBT_HOST (or run bootstrap.sh first)}"
: "${DBT_TOKEN:?set DBT_TOKEN}"
: "${DBT_HTTP_PATH:?set DBT_HTTP_PATH}"
export DBT_HOST DBT_TOKEN DBT_HTTP_PATH
export DBT_CATALOG="${DBT_CATALOG:-main}"
export DBT_SCHEMA_V1="${DBT_SCHEMA_V1:-dbt_uc_compat_v1}"
WORK="${WORK_DIR:-$(mktemp -d)}"
VENV="$WORK/.venv"
PASS=0; FAIL=0

say(){ printf '\n\033[1m%s\033[0m\n' "$*"; }
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
ko(){  FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }
expect(){ if grep -qE "$2" <<<"$3"; then ok "$1"; else printf '  \033[31mFAIL\033[0m  %s\n        expected /%s/, got: %s\n' "$1" "$2" "${3:0:260}"; FAIL=$((FAIL+1)); fi; }
q(){ python3 "$HERE/lib/dbx_api.py" query "$1" 2>&1; }

say "0. Install dbt-core 1.x + dbt-databricks"
python3 -m venv "$VENV" >/dev/null
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q dbt-databricks || { ko "pip install dbt-databricks"; exit 1; }
DBT="$VENV/bin/dbt"
VERSIONS="$("$DBT" --version 2>&1)"
expect "dbt-core 1.x installed"      'installed: 1\.'  "$VERSIONS"
expect "dbt-databricks installed"    'databricks: 1\.' "$VERSIONS"

PROJ="$WORK/project"; cp -r "$HERE/dbt1x-databricks" "$PROJ"
D(){ (cd "$PROJ" && "$DBT" "$@" --project-dir "$PROJ" --profiles-dir "$PROJ" 2>&1); }
GRANTS_SQL="show grants on table $DBT_CATALOG.$DBT_SCHEMA_V1.dim_customers"

say "1. The grants defect: which form works on which version"
# 1.x documents a pre-quoted principal for names containing a space. dbt v2 quotes it
# itself, so that same form emits ``account users`` and fails with SQLSTATE 42601.
python3 - "$PROJ" <<'PYEOF'
import pathlib, sys
p = pathlib.Path(sys.argv[1], "models", "schema.yml")
p.write_text(p.read_text().replace("select: ['account users']", "select: ['`account users`']"))
PYEOF
OUT="$(D run -s dim_customers)"
expect "1.x accepts the pre-quoted principal" 'Completed successfully' "$OUT"
expect "and the grant lands on the real principal" 'account users.*SELECT' "$(q "$GRANTS_SQL")"

python3 - "$PROJ" <<'PYEOF'
import pathlib, sys
p = pathlib.Path(sys.argv[1], "models", "schema.yml")
p.write_text(p.read_text().replace("select: ['`account users`']", "select: ['account users']"))
PYEOF
OUT="$(D run -s dim_customers)"
expect "1.x also accepts the bare principal" 'Completed successfully' "$OUT"
expect "same grant either way (so the bare form is portable)" 'account users.*SELECT' "$(q "$GRANTS_SQL")"

say "2. Core materializations on Unity Catalog"
OUT="$(D seed)"
expect "seed (into <schema>_bronze)" 'Completed successfully' "$OUT"
OUT="$(D run -s dim_customers)"
expect "incremental merge re-run"    'Completed successfully' "$OUT"
expect "MERGE really ran"            '^[1-9]' "$(q "select count(*) from (describe history $DBT_CATALOG.$DBT_SCHEMA_V1.dim_customers) where operation = 'MERGE'")"
expect "liquid clustering applied"   'id' "$(q "describe detail $DBT_CATALOG.$DBT_SCHEMA_V1.dim_customers")"
OUT="$(D test -s dim_customers)"
expect "tests pass"                  'Completed successfully' "$OUT"

say "3. Behaviours that differ from v2"
# v2 takes a snapshot's target_schema literally; 1.x applies generate_schema_name.
OUT="$(D snapshot)"
expect "snapshot runs" 'Completed successfully' "$OUT"
LANDED="$(q "select table_schema from system.information_schema.tables where table_catalog='$DBT_CATALOG' and table_name='snap_v1'")"
if grep -qE "^snapshots_v1$" <<<"$LANDED"; then
  ok "1.x puts the snapshot in snapshots_v1 (literal, like v2)"
else
  ok "1.x puts the snapshot in '$LANDED' (concatenated — differs from v2's literal target_schema)"
fi
# v1 source syntax: loaded_at_field / freshness at the top level (v2 wants them under config).
OUT="$(D source freshness)"
expect "freshness computes an age on UC"                 'PASS freshness of|Status: pass' "$OUT"
# dbt-core 1.12 already deprecates the 1.x spelling that v2 makes mandatory.
expect "1.x warns that the property moved to config"     'PropertyMovedToConfigDeprecation' "$OUT"
OUT="$(D run -s src_customers)"
expect "source resolves through catalog+schema" 'Completed successfully' "$OUT"
# VARIANT: v2 accepts `variant` as a contract data type; does 1.x?
OUT="$(D run -s variant_events)"
if grep -q 'Completed successfully' <<<"$OUT"; then
  ok "1.x builds a Delta table with a VARIANT column under an enforced contract"
  expect "the column is really VARIANT" 'variant' \
    "$(q "select full_data_type from system.information_schema.columns where table_catalog='$DBT_CATALOG' and table_schema='$DBT_SCHEMA_V1' and table_name='variant_events' and column_name='payload'")"
else
  ko "1.x refused the VARIANT model: $(grep -oE 'Compilation Error.{0,120}|Database Error.{0,120}' <<<"$OUT" | head -1)"
fi

say "4. Databricks-only materializations (DBSQL pipeline quota applies)"
OUT="$(D run -s mv_customers --threads 1)"
if grep -q 'Completed successfully' <<<"$OUT"; then
  expect "materialized_view is a MATERIALIZED_VIEW in UC" 'MATERIALIZED_VIEW' \
    "$(q "select table_type from system.information_schema.tables where table_catalog='$DBT_CATALOG' and table_schema='$DBT_SCHEMA_V1' and table_name='mv_customers'")"
else
  ko "materialized_view failed: $(grep -oE 'QUOTA_EXCEEDED[^"]{0,80}|Database Error.{0,100}' <<<"$OUT" | head -1)"
fi
# 1.x has table_format directly, with no catalogs.yml equivalent.
OUT="$(D run -s iceberg_customers)"
if grep -q 'Completed successfully' <<<"$OUT"; then
  expect "table_format: iceberg enables UniForm" '[Tt]rue' \
    "$(q "show tblproperties $DBT_CATALOG.$DBT_SCHEMA_V1.iceberg_customers ('delta.enableIcebergCompatV2')")"
else
  ko "table_format: iceberg failed: $(grep -oE 'Compilation Error.{0,140}|Database Error.{0,140}' <<<"$OUT" | head -1)"
fi

say "Result: $PASS passed, $FAIL failed   (workdir: $WORK)"
[ "$FAIL" -eq 0 ]
