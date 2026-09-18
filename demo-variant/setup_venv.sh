#!/usr/bin/env bash
# Create the local venv for the UC OSS side of the demo.
#
#   ./setup_venv.sh          # .venv with pyspark 4, dbt-spark, delta-spark
#   source .venv/bin/activate
#
# Spark 4 is required for the VARIANT type; java 17+ must be on PATH.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="${1:-$HERE/.venv}"
PYSPARK_VERSION="${PYSPARK_VERSION:-4.1.3}"

command -v java >/dev/null || { echo "java 17+ is required"; exit 1; }
java -version 2>&1 | head -1

python3 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q \
    "pyspark==$PYSPARK_VERSION" \
    dbt-spark \
    deltalake pyarrow

echo
echo "venv ready: $VENV"
"$VENV/bin/python" -c "import pyspark; print('pyspark', pyspark.__version__)"
"$VENV/bin/dbt" --version 2>&1 | head -5
echo
echo "next: ./start_uc_oss.sh   then   .venv/bin/python uc_oss/01_create_tables_spark.py"
