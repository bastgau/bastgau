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
skip(){ printf '  \033[33mSKIP\033[0m  %s\n' "$*"; }
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
D(){ (cd "$FIX" && UC_OSS_DB="${UC_OSS_DB}" "$DBT" "$@" --project-dir "$FIX" --profiles-dir "$FIX" --no-manage-state 2>&1); }

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

say "7. VARIANT: DuckDB's own type, and what UC OSS does with it"
# A file-backed DuckDB is created at storage version v1.0.0+, which cannot hold VARIANT,
# and no profile key raises that (config/storage_version are accepted but ignored).
OUT="$(D run --select variant_local)"
expect "a file-backed DuckDB refuses to store VARIANT" 'storage versions prior to v1.5.0' "$OUT"
# In memory, the same model materializes.
OUT="$(D run --select variant_local --target memory)"
expect "an in-memory DuckDB stores VARIANT" 'Finished .run. successfully' "$OUT"
OUT="$(D show --inline "select typeof(cast('{\"a\":1}' as variant)) as t" --limit 1)"
expect "DuckDB reports the type as VARIANT" 'VARIANT' "$OUT"
# JSON -> VARIANT is the real parse_json equivalent; casting a string yields a VARCHAR variant.
OUT="$(D show --inline "select variant_typeof(cast(cast('{\"a\":1}' as json) as variant)) as t" --limit 1)"
expect "JSON cast to VARIANT gives an OBJECT" 'OBJECT' "$OUT"
OUT="$(D show --inline "select variant_typeof(cast('{\"a\":1}' as variant)) as t" --limit 1)"
expect "casting a string to VARIANT silently gives VARCHAR" 'VARCHAR' "$OUT"
OUT="$(D show --inline "select cast(variant_extract(cast(cast('{\"a\":1}' as json) as variant),'a') as int) as a" --limit 1)"
expect "variant_extract reads a key from an OBJECT variant" '1' "$OUT"
# Databricks' variant functions are not DuckDB's: a Databricks model is not portable as is.
OUT="$(D show --inline "select parse_json('{\"a\":1}') as p" --limit 1)"
expect "parse_json (Databricks) is absent from DuckDB" 'parse_json does not exist' "$OUT"
# `v:a` parses, but as a prefix alias: it means `a AS v`, not a path into the variant.
OUT="$(D show --inline "select answer: 42" --limit 1)"
expect "in DuckDB, alias: expr is the prefix-alias syntax" 'answer' "$OUT"
OUT="$(D show --inline "select v:a from (select cast(cast('{\"a\":1}' as json) as variant) as v)" --limit 1)"
expect "so v:a resolves as a column named a, and fails" 'Referenced column .a. not found' "$OUT"
# A VARIANT value cannot cross dbt's Arrow bridge, even though a model can store one.
OUT="$(D show --inline "select cast(cast('{\"a\":1}' as json) as variant) as v" --limit 1)"
expect "dbt show cannot return a VARIANT value" 'C Data interface error|Cannot get schema' "$OUT"
OUT="$(D run --select variant_uc_oss)"
expect "writing a VARIANT into UC OSS fails on the write path, not the type" 'MethodNotAllowed_405|405' "$OUT"
# UC OSS does know the type in its metadata model, which matters once writes land.
expect "UC OSS advertises VARIANT among its column types" 'VARIANT' \
  "$(JAVA_TOOL_OPTIONS='' javap -classpath "$UC_HOME/lib/unitycatalog-server-$UC_VERSION.jar" \
      io.unitycatalog.server.model.ColumnTypeName 2>/dev/null)"

say "8. Reading the Delta format locally"
# A genuine Delta table, written with delta-rs, registered in UC OSS as EXTERNAL DELTA.
DELTA_DIR="$WORK/delta/customers"
if "$VENV/bin/pip" install -q deltalake pyarrow 2>/dev/null; then
  "$VENV/bin/python" - "$DELTA_DIR" <<'PYEOF'
import sys
import pyarrow as pa
from deltalake import write_deltalake
write_deltalake(sys.argv[1], pa.table({
    "id": pa.array([1, 2, 3], pa.int64()),
    "name": pa.array(["alice", "bob", "carol"]),
    "payload_json": pa.array(['{"a":1}', '{"a":2}', '{"a":3}']),
}), mode="overwrite")
PYEOF
  [ -d "$DELTA_DIR/_delta_log" ] && ok "a Delta table was written locally" || ko "delta-rs write failed"

  export UC_OSS_DELTA_PATH="file://$DELTA_DIR"
  OUT="$(D run --select delta_source)"
  expect "dbt reads Delta through delta_scan()" 'Finished .run. successfully' "$OUT"
  OUT="$(D show --inline "select count(*) as n from analytics.delta_source" --limit 1)"
  expect "the three Delta rows landed" '3' "$OUT"

  # Register it in UC OSS, then try the documented DuckDB uc_catalog path.
  COLS='[{"name":"id","type_text":"bigint","type_name":"LONG","type_json":"{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}","position":0}]'
  uc -X POST -H 'Content-Type: application/json' "$API/tables" -d "{\"name\":\"customers\",\"catalog_name\":\"$CATALOG\",\"schema_name\":\"$SCHEMA\",\"table_type\":\"EXTERNAL\",\"data_source_format\":\"DELTA\",\"storage_location\":\"file://$DELTA_DIR\",\"columns\":$COLS}" >/dev/null 2>&1
  expect "the Delta table is registered in UC OSS" '"name":"customers"' "$(uc "$API/tables?catalog_name=$CATALOG&schema_name=$SCHEMA")"
  ATTACH="$(D show --inline "install uc_catalog; load uc_catalog; load delta;
    create or replace secret s (type uc, token 'not-used', endpoint '127.0.0.1:$PORT', aws_region 'us-east-1');
    attach '$CATALOG' as ucx (type uc_catalog, secret s);
    select count(*) as n from ucx.$SCHEMA.customers" --limit 1)"
  # The extension parses UC's column metadata strictly and chokes on type_precision.
  if grep -qE 'Invalid field found while parsing field: type_precision' <<<"$ATTACH"; then
    ok "uc_catalog cannot read UC OSS metadata (type_precision) — known incompatibility"
  elif grep -qE '│ *[0-9]+ *│' <<<"$ATTACH"; then
    ok "uc_catalog now reads UC OSS tables — revisit the README"
  else
    ko "uc_catalog failed for another reason: ${ATTACH:0:160}"
  fi
else
  skip "Delta checks (deltalake/pyarrow could not be installed)"
fi

say "Result: $PASS passed, $FAIL failed   (workdir: $WORK)"
[ "$FAIL" -eq 0 ]
