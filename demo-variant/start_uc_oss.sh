#!/usr/bin/env bash
# Start Unity Catalog OSS 0.6.0 locally, from Maven Central (no Docker needed).
#
#   ./start_uc_oss.sh          # foreground, Ctrl-C to stop
#   ./start_uc_oss.sh --bg     # background, logs in .uc/server.log
#
# The server keeps its state in .uc/run/etc/db (H2). Delete .uc to start over.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UC_VERSION="${UC_VERSION:-0.6.0}"
PORT="${UC_PORT:-8080}"
UC_HOME="$HERE/.uc"

command -v java >/dev/null || { echo "java 17+ is required"; exit 1; }
command -v mvn  >/dev/null || { echo "mvn is required"; exit 1; }

if [ ! -d "$UC_HOME/lib" ]; then
  echo "Fetching Unity Catalog OSS $UC_VERSION from Maven Central…"
  mkdir -p "$UC_HOME/run/etc/conf" "$UC_HOME/run/etc/db"
  cat > "$UC_HOME/pom.xml" <<XML
<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>
<groupId>local</groupId><artifactId>uc-oss</artifactId><version>1</version><dependencies>
<dependency><groupId>io.unitycatalog</groupId><artifactId>unitycatalog-server</artifactId><version>$UC_VERSION</version></dependency>
</dependencies></project>
XML
  (cd "$UC_HOME" && mvn -q -B dependency:copy-dependencies -DoutputDirectory="$UC_HOME/lib" -DincludeScope=runtime)
  printf 'server.env=dev\n' > "$UC_HOME/run/etc/conf/server.properties"
  cat > "$UC_HOME/run/etc/conf/hibernate.properties" <<'PROPS'
hibernate.connection.driver_class=org.h2.Driver
hibernate.connection.url=jdbc:h2:file:./etc/db/h2db;DB_CLOSE_DELAY=-1
hibernate.dialect=org.hibernate.dialect.H2Dialect
hibernate.hbm2ddl.auto=update
PROPS
  echo "  $(ls "$UC_HOME/lib" | wc -l) jars"
fi

start() { cd "$UC_HOME/run" && JAVA_TOOL_OPTIONS="" exec java -cp "$UC_HOME/lib/*" \
    io.unitycatalog.server.UnityCatalogServer --port "$PORT"; }

if [ "${1:-}" = "--bg" ]; then
  ( start > "$UC_HOME/server.log" 2>&1 & )
  for _ in $(seq 1 30); do
    curl -sS --max-time 5 --noproxy '*' "http://127.0.0.1:$PORT/api/2.1/unity-catalog/catalogs" >/dev/null 2>&1 && break
    sleep 2
  done
  echo "Unity Catalog OSS listening on http://127.0.0.1:$PORT (logs: $UC_HOME/server.log)"
else
  echo "Unity Catalog OSS on http://127.0.0.1:$PORT — Ctrl-C to stop"
  start
fi
