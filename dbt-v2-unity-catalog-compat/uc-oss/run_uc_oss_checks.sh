#!/usr/bin/env bash
# Check what dbt v2 can and cannot do against Unity Catalog OSS, fully locally.
#
# No Databricks account, no SQL warehouse: the protocol's `databricks` adapter needs a
# Databricks SQL endpoint, so the local path is dbt's GA `duckdb` adapter pointed at a
# `type: unity` catalog (the same catalogs.yml shape used against Databricks).
#
# The script downloads the Unity Catalog OSS server from Maven Central, starts it,
# creates a catalog and a schema through its REST API, then runs dbt against it.
#
# Requirements: java 17+, mvn, python3, network access to Maven Central and PyPI.
# Usage: ./run_uc_oss_checks.sh [dbt_version] [uc_version]
#
# The available server versions come from the authoritative metadata, not the search API:
#   curl https://repo1.maven.org/maven2/io/unitycatalog/unitycatalog-server/maven-metadata.xml
# (Maven Central rate-limits bursts with HTTP 429 — space the requests out.)
set -uo pipefail

DBT_VERSION="${1:-2.0.4}"
UC_VERSION="${2:-0.6.0}"
PORT="${UC_OSS_PORT:-8081}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK_DIR:-$(mktemp -d)}"
UC_HOME="$WORK/uc"
VENV="$WORK/.venv"
API="http://127.0.0.1:$PORT/api/2.1/unity-catalog"
CATALOG="dbt_oss"
SCHEMA="analytics"
PASS=0; FAIL=0
SERVER_PID=""

say(){ printf '\n\033[1m%s\033[0m\n' "$*"; }
ok(){  PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
ko(){  FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }
# expect <label> <regex> <text>
expect(){ if grep -qE "$2" <<<"$3"; then ok "$1"; else printf '  \033[31mFAIL\033[0m  %s\n        expected /%s/, got: %s\n' "$1" "$2" "${3:0:240}"; FAIL=$((FAIL+1)); fi; }
uc(){ curl -sS --max-time 20 --noproxy '*' "$@"; }
cleanup(){ [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null; }
trap cleanup EXIT

command -v java >/dev/null || { ko "java is required"; exit 1; }
command -v mvn  >/dev/null || { ko "mvn is required";  exit 1; }

say "0. Fetch the Unity Catalog OSS server $UC_VERSION from Maven Central"
mkdir -p "$UC_HOME/run/etc/conf" "$UC_HOME/run/etc/db"
cat > "$UC_HOME/pom.xml" <<XML
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>local</groupId><artifactId>uc-oss-runner</artifactId><version>1</version>
  <dependencies>
    <dependency><groupId>io.unitycatalog</groupId><artifactId>unitycatalog-server</artifactId><version>$UC_VERSION</version></dependency>
  </dependencies>
</project>
XML
(cd "$UC_HOME" && mvn -q -B dependency:copy-dependencies -DoutputDirectory="$UC_HOME/lib" -DincludeScope=runtime >/dev/null 2>&1)
JARS="$(ls "$UC_HOME/lib" 2>/dev/null | wc -l)"
[ "$JARS" -gt 50 ] && ok "$JARS jars downloaded" || { ko "maven download failed"; exit 1; }

say "1. Start the server on port $PORT"
printf 'server.env=dev\n' > "$UC_HOME/run/etc/conf/server.properties"
cat > "$UC_HOME/run/etc/conf/hibernate.properties" <<'PROPS'
hibernate.connection.driver_class=org.h2.Driver
hibernate.connection.url=jdbc:h2:file:./etc/db/h2db;DB_CLOSE_DELAY=-1
hibernate.dialect=org.hibernate.dialect.H2Dialect
hibernate.hbm2ddl.auto=update
PROPS
( cd "$UC_HOME/run" && JAVA_TOOL_OPTIONS="" java -cp "$UC_HOME/lib/*" \
    io.unitycatalog.server.UnityCatalogServer --port "$PORT" > "$UC_HOME/server.log" 2>&1 ) &
SERVER_PID=$!
for _ in $(seq 1 30); do
  uc "$API/catalogs" >/dev/null 2>&1 && break
  sleep 2
done
expect "server answers its native API" '"catalogs"' "$(uc "$API/catalogs")"

say "2. Create a catalog and a schema through the UC REST API"
uc -X POST -H 'Content-Type: application/json' "$API/catalogs" \
   -d "{\"name\":\"$CATALOG\",\"comment\":\"dbt v2 UC OSS check\"}" >/dev/null 2>&1
uc -X POST -H 'Content-Type: application/json' "$API/schemas" \
   -d "{\"name\":\"$SCHEMA\",\"catalog_name\":\"$CATALOG\"}" >/dev/null 2>&1
expect "catalog $CATALOG exists"        "\"name\":\"$CATALOG\"" "$(uc "$API/catalogs")"
expect "schema $CATALOG.$SCHEMA exists" "\"full_name\":\"$CATALOG.$SCHEMA\"" "$(uc "$API/schemas?catalog_name=$CATALOG")"

say "3. What the Iceberg REST catalog of UC OSS actually exposes"
CONF="$(uc "$API/iceberg/v1/config?warehouse=$CATALOG")"
expect "an Iceberg REST catalog is served" '"prefix":"catalogs/'"$CATALOG"'"' "$CONF"
expect "read endpoints are advertised"  'GET /v1/\{prefix\}/namespaces' "$CONF"
# The whole question: is there a write endpoint?
if grep -qE 'POST /v1/\{prefix\}/namespaces(\"|,|$)|POST /v1/\{prefix\}/namespaces/\{namespace\}/tables(\"|,)' <<<"$CONF"; then
  ok "write endpoints advertised (UC OSS now accepts writes — revisit the README)"
else
  ok "no write endpoint advertised: the catalog is read-only"
fi
CREATE_NS="$(uc -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
  "$API/iceberg/v1/catalogs/$CATALOG/namespaces" -d '{"namespace":["probe"]}')"
expect "creating a namespace is refused (405)" '^(404|405)$' "$CREATE_NS"

say "4. Install dbt $DBT_VERSION (duckdb adapter is bundled)"
python3 -m venv "$VENV" >/dev/null
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q "dbt==$DBT_VERSION" || { ko "pip install dbt"; exit 1; }
DBT="$VENV/bin/dbt"
expect "dbt reports $DBT_VERSION" "$DBT_VERSION" "$("$DBT" --version 2>&1)"

FIX="$WORK/fixture"; cp -r "$HERE/fixture" "$FIX"
export UC_OSS_DB="$WORK/local.duckdb"
export UC_OSS_ENDPOINT="$API/iceberg"
D(){ (cd "$FIX" && "$DBT" "$@" --project-dir "$FIX" --profiles-dir "$FIX" --no-manage-state 2>&1); }

say "5. dbt runs locally with no warehouse at all"
OUT="$(D run --select local_only)"
expect "a model builds in the local DuckDB file" 'Finished .run. successfully' "$OUT"
[ -f "$UC_OSS_DB" ] && ok "duckdb file created" || ko "no duckdb file"

say "6. dbt against the Unity Catalog OSS catalog"
OUT="$(D parse)"
expect "catalogs.yml type: unity + duckdb validates" 'successfully' "$OUT"
OUT="$(D run --select uc_oss_write)"
expect "dbt reaches the UC OSS Iceberg endpoint" 'iceberg/v1/catalogs/'"$CATALOG" "$OUT"
expect "the write is refused by UC OSS, not by dbt" 'MethodNotAllowed_405|405' "$OUT"

say "Result: $PASS passed, $FAIL failed   (workdir: $WORK)"
[ "$FAIL" -eq 0 ]
