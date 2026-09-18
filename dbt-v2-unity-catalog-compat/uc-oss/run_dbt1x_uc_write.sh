#!/usr/bin/env bash
# Write into Unity Catalog OSS from dbt — the path that actually works.
#
# dbt v2 cannot do it (its `type: unity` catalog goes through UC's read-only Iceberg
# REST endpoint, and it has no plugin mechanism). dbt-core 1.x + dbt-duckdb can, by
# writing Delta files and registering the table through UC's **native** REST API.
# `dbt1x/uc_delta.py` is that plugin, ~130 lines.
#
# Requirements: java 17+, mvn, python3. Usage: ./run_dbt1x_uc_write.sh [uc_version]
set -uo pipefail

UC_VERSION="${1:-0.6.0}"
PORT="${UC_OSS_PORT:-8100}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK_DIR:-$(mktemp -d)}"
UC_HOME="$WORK/uc"; VENV="$WORK/.venv"
API="http://127.0.0.1:$PORT/api/2.1/unity-catalog"
CATALOG="dbt_oss"; SCHEMA="analytics"; TABLE="customers_uc"
PASS=0; FAIL=0; SERVER_PID=""

say(){ printf '\n\033[1m%s\033[0m\n' "$*"; }
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
ko(){  FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }
expect(){ if grep -qE "$2" <<<"$3"; then ok "$1"; else printf '  \033[31mFAIL\033[0m  %s\n        expected /%s/, got: %s\n' "$1" "$2" "${3:0:240}"; FAIL=$((FAIL+1)); fi; }
uc(){ curl -sS --max-time 20 --noproxy '*' "$@"; }
cleanup(){ [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null; }
trap cleanup EXIT

say "0. Unity Catalog OSS $UC_VERSION"
mkdir -p "$UC_HOME/run/etc/conf" "$UC_HOME/run/etc/db"
cat > "$UC_HOME/pom.xml" <<XML
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>local</groupId><artifactId>uc</artifactId><version>1</version><dependencies>
<dependency><groupId>io.unitycatalog</groupId><artifactId>unitycatalog-server</artifactId><version>$UC_VERSION</version></dependency>
</dependencies></project>
XML
(cd "$UC_HOME" && mvn -q -B dependency:copy-dependencies -DoutputDirectory="$UC_HOME/lib" -DincludeScope=runtime >/dev/null 2>&1)
printf 'server.env=dev\n' > "$UC_HOME/run/etc/conf/server.properties"
printf 'hibernate.connection.driver_class=org.h2.Driver\nhibernate.connection.url=jdbc:h2:file:./etc/db/h2db;DB_CLOSE_DELAY=-1\nhibernate.dialect=org.hibernate.dialect.H2Dialect\nhibernate.hbm2ddl.auto=update\n' > "$UC_HOME/run/etc/conf/hibernate.properties"
( cd "$UC_HOME/run" && JAVA_TOOL_OPTIONS="" java -cp "$UC_HOME/lib/*" \
    io.unitycatalog.server.UnityCatalogServer --port "$PORT" > "$UC_HOME/server.log" 2>&1 ) &
SERVER_PID=$!
for _ in $(seq 1 30); do uc "$API/catalogs" >/dev/null 2>&1 && break; sleep 2; done
expect "server is up" '"catalogs"' "$(uc "$API/catalogs")"

say "1. dbt-core 1.x + dbt-duckdb + delta-rs"
python3 -m venv "$VENV" >/dev/null
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q dbt-duckdb deltalake pyarrow || { ko "pip install"; exit 1; }
VERSIONS="$("$VENV/bin/dbt" --version 2>&1)"
expect "dbt-core 1.x is installed" 'installed: 1\.' "$VERSIONS"
expect "the duckdb adapter is installed" 'duckdb: 1\.' "$VERSIONS"

say "2. Run the model: Delta written, table registered"
PROJ="$WORK/project"; mkdir -p "$PROJ"; cp -r "$HERE/dbt1x/." "$PROJ/"
cat > "$PROJ/profiles.yml" <<YAML
uc_oss_write:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: $PROJ/local.duckdb
      schema: $SCHEMA
      module_paths:
        - $PROJ
      plugins:
        - module: uc_delta
          alias: uc
          config:
            endpoint: $API
            catalog: $CATALOG
            schema: $SCHEMA
            delta_root: $WORK/delta
YAML
export UC_STAGE="$WORK/stage"; mkdir -p "$UC_STAGE"
OUT="$(cd "$PROJ" && "$VENV/bin/dbt" run --project-dir "$PROJ" --profiles-dir "$PROJ" 2>&1)"
expect "dbt run succeeds" 'Completed successfully' "$OUT"
[ -d "$WORK/delta/$CATALOG/$SCHEMA/$TABLE/_delta_log" ] && ok "Delta log written" || ko "no _delta_log"

say "3. What Unity Catalog OSS now holds"
TABLES="$(uc "$API/tables?catalog_name=$CATALOG&schema_name=$SCHEMA")"
expect "the table is registered"          "\"name\":\"$TABLE\"" "$TABLES"
expect "registered as EXTERNAL DELTA"    '"table_type":"EXTERNAL".*"data_source_format":"DELTA"' "$TABLES"
expect "its storage_location points at the Delta dir" "$TABLE" "$TABLES"
expect "its columns carry UC types"      '"type_name":"LONG"' "$TABLES"

say "4. Read the result back"
OUT="$(cd "$PROJ" && "$VENV/bin/dbt" show --inline "select count(*) as n from delta_scan('file://$WORK/delta/$CATALOG/$SCHEMA/$TABLE')" --limit 1 --project-dir "$PROJ" --profiles-dir "$PROJ" 2>&1)"
expect "delta_scan reads the two rows" '2' "$OUT"

say "Result: $PASS passed, $FAIL failed   (workdir: $WORK)"
[ "$FAIL" -eq 0 ]
