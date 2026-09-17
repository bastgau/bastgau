#!/usr/bin/env bash
# Remove everything the protocol created in Unity Catalog.
#
# Destructive: it drops schemas with CASCADE. It prints the plan and stops unless
# you pass --yes. Objects it targets (all derived from DBT_CATALOG / DBT_SCHEMA):
#   <catalog>.<schema>              models, seeds, MV, streaming table
#   <catalog>.<schema>_bronze       source table built by the seed
#   <catalog>.<schema>_lakehouse    UC-managed Iceberg table
#   <catalog>.snapshots             snapshot (dbt takes target_schema literally)
#   <cross catalog>                 only when created by bootstrap --with-cross-catalog
#
# The `snapshots` schema is dropped only with --include-snapshots, because that name
# is generic and may predate this protocol in your catalog.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/env.local" ] && [ -z "${DBT_HOST:-}" ] && . "$HERE/env.local"
: "${DBT_HOST:?set DBT_HOST (or run bootstrap.sh first)}"
: "${DBT_TOKEN:?set DBT_TOKEN}"
export DBT_HOST DBT_TOKEN
CAT="${DBT_CATALOG:-main}"; SCH="${DBT_SCHEMA:-dbt_uc_compat}"
API="python3 $HERE/lib/dbx_api.py"
CONFIRM=0; SNAPSHOTS=0; DROP_CROSS=0

while [ $# -gt 0 ]; do
  case "$1" in
    --yes|-y)            CONFIRM=1; shift ;;
    --include-snapshots) SNAPSHOTS=1; shift ;;
    --drop-cross-catalog) DROP_CROSS=1; shift ;;
    -h|--help)           sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

PLAN=("drop schema if exists \`$CAT\`.\`$SCH\` cascade"
      "drop schema if exists \`$CAT\`.\`${SCH}_bronze\` cascade"
      "drop schema if exists \`$CAT\`.\`${SCH}_lakehouse\` cascade")
[ "$SNAPSHOTS" -eq 1 ] && PLAN+=("drop schema if exists \`$CAT\`.\`snapshots\` cascade")
if [ "$DROP_CROSS" -eq 1 ] && [ -n "${DBT_CROSS_CATALOG:-}" ]; then
  PLAN+=("drop catalog if exists \`$DBT_CROSS_CATALOG\` cascade")
fi

echo "Workspace: $DBT_HOST"
echo "Plan:"
for stmt in "${PLAN[@]}"; do echo "  $stmt"; done

echo
echo "Current contents:"
python3 "$HERE/lib/dbx_api.py" query \
  "select table_schema, count(*) as objects from system.information_schema.tables
   where table_catalog='$CAT' and (table_schema like '${SCH}%' or table_schema='snapshots')
   group by table_schema order by table_schema" | sed 's/^/  /'

if [ "$CONFIRM" -ne 1 ]; then
  echo
  echo "Dry run: nothing dropped. Re-run with --yes to execute."
  exit 0
fi

echo
$API sql "${PLAN[@]}"
echo
echo "Remaining:"
python3 "$HERE/lib/dbx_api.py" query \
  "select table_schema, count(*) as objects from system.information_schema.tables
   where table_catalog='$CAT' and (table_schema like '${SCH}%' or table_schema='snapshots')
   group by table_schema order by table_schema" | sed 's/^/  /'
