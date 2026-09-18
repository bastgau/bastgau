#!/usr/bin/env python3
"""Run dbt against the local Unity Catalog OSS, from a plain python script.

Joins demo_users.core.users with demo_cities.core.cities and flattens the VARIANT
metadata into demo_users.marts.users_enriched — an external Delta table under
./data, written through Unity Catalog.

The trick that makes this work: this script builds the SparkSession **first**, with
the Unity Catalog connector wired in, and dbt-spark's `method: session` then reuses
that active session instead of creating its own.

    ./start_uc_oss.sh --bg
    .venv/bin/python uc_oss/01_create_tables_spark.py
    .venv/bin/python uc_oss/02_run_dbt.py
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA = os.path.join(ROOT, "data")
JARS = os.path.join(ROOT, ".jars")
PROJECT = os.path.join(HERE, "dbt_project")

UC_CONNECTOR = "io.unitycatalog:unitycatalog-spark_2.13:0.4.1"
# Delta 4.4 loads io.unitycatalog.client.delta.model.* by reflection (UC coordinated
# commits). The connector pins client 0.4.1, which does not carry those classes, so the
# client is pinned here to the server version — a direct dependency wins in Maven.
UC_CLIENT = "io.unitycatalog:unitycatalog-client:0.6.0"
DELTA = "io.delta:delta-spark_4.1_2.13:4.4.0"  # Spark-qualified artifact: the 4.1 line


def fetch_jars() -> str:
    """Same download as 01_create_tables_spark.py — kept duplicated on purpose."""
    if os.path.isdir(JARS) and any(f.endswith(".jar") for f in os.listdir(JARS)):
        return JARS
    os.makedirs(JARS, exist_ok=True)
    deps = "".join(
        f"<dependency><groupId>{g}</groupId><artifactId>{a}</artifactId><version>{v}</version></dependency>"
        for g, a, v in (x.split(":") for x in (UC_CONNECTOR, UC_CLIENT, DELTA))
    )
    with open(os.path.join(JARS, "pom.xml"), "w") as handle:
        handle.write(
            '<project xmlns="http://maven.apache.org/POM/4.0.0"><modelVersion>4.0.0</modelVersion>'
            "<groupId>local</groupId><artifactId>jars</artifactId><version>1</version>"
            f"<dependencies>{deps}</dependencies></project>"
        )
    subprocess.run(
        ["mvn", "-q", "-B", "dependency:copy-dependencies", f"-DoutputDirectory={JARS}",
         "-DincludeScope=runtime"],
        cwd=JARS, check=True,
    )
    return JARS


def build_session(uc_uri: str, catalogs: list[str], default_catalog: str | None = None):
    from pyspark.sql import SparkSession

    jars = ",".join(
        os.path.join(fetch_jars(), f) for f in sorted(os.listdir(fetch_jars())) if f.endswith(".jar")
    )
    builder = (
        SparkSession.builder.appName("uc-oss-dbt")
        .master("local[*]")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Delta needs a Delta-aware session catalog; UCSingleCatalog here fails with
        # "uri must be specified for Unity Catalog 'spark_catalog'".
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    if default_catalog:
        # dbt-spark is not catalog-aware: it runs `show table extended in <schema>`. With a
        # default catalog, that resolves inside Unity Catalog and dbt sees its own tables.
        builder = builder.config("spark.sql.defaultCatalog", default_catalog)
    for catalog in catalogs:
        builder = (
            builder.config(f"spark.sql.catalog.{catalog}", "io.unitycatalog.spark.UCSingleCatalog")
            .config(f"spark.sql.catalog.{catalog}.uri", uc_uri)
            .config(f"spark.sql.catalog.{catalog}.token", "")
        )
    return builder.getOrCreate()


TARGET_COLUMNS = """
    user_id bigint, full_name string, email string, city string, signed_up date,
    country string, population bigint, region string,
    latitude double, longitude double, primary_tag string, metadata_json string
"""


def uc_table(uc_uri: str, full_name: str, method: str = "GET"):
    """Ask Unity Catalog about a table without touching its files.

    spark.catalog.tableExists() resolves the Delta table and throws when the files are
    gone, which is exactly the state we need to detect, so the metastore is asked directly.
    """
    req = urllib.request.Request(
        f"{uc_uri}/api/2.1/unity-catalog/tables/{full_name}",
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            try:
                return json.loads(body) if body else {}
            except json.JSONDecodeError:
                return {}  # DELETE answers with a non-JSON body
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise SystemExit(f"Unity Catalog {method} {full_name} -> {exc.code}: {exc.read().decode()[:200]}")


def ensure_target(spark, uc_uri: str, target: str, location: str) -> None:
    """Create the output table empty, as an external Delta table, through Unity Catalog.

    The UC OSS connector (0.4.1) only accepts an external Delta table created in two
    steps: CREATE TABLE ... LOCATION, then INSERT/MERGE. dbt's `table` materialization
    would emit CREATE OR REPLACE ... AS SELECT, whose LOCATION the connector resolves
    against spark_catalog instead of the Unity Catalog one. So the table is created here,
    empty, and the dbt model is incremental (merge).

    Wiping ./data leaves the table registered in Unity Catalog with no Delta log behind
    it, and every later read then fails; unregister that leftover first so the script is
    re-runnable.
    """
    registered = uc_table(uc_uri, target) is not None
    if registered and not os.path.isdir(os.path.join(location, "_delta_log")):
        print(f"  {target} is registered but its files are gone: unregistering it")
        uc_table(uc_uri, target, method="DELETE")  # DROP TABLE would resolve the files first
        registered = False
    if registered:
        return
    shutil.rmtree(location, ignore_errors=True)
    spark.sql(f"create table {target} ({TARGET_COLUMNS}) using delta location 'file://{location}'")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--uc-uri", default=os.environ.get("UC_URI", "http://127.0.0.1:8080"))
    parser.add_argument("--users-catalog", default="demo_users")
    parser.add_argument("--cities-catalog", default="demo_cities")
    parser.add_argument("--src-schema", default="core")
    parser.add_argument("--out-schema", default="marts")
    args = parser.parse_args(argv)

    # dbt-spark renders two-part names only, so the catalog travels inside `schema`.
    os.environ.update(
        {
            "OSS_USERS_SCHEMA": f"{args.users_catalog}.{args.src_schema}",
            "OSS_CITIES_SCHEMA": f"{args.cities_catalog}.{args.src_schema}",
            # Plain schema: the catalog comes from spark.sql.defaultCatalog, so dbt's
            # relation discovery ("show table extended in marts") resolves in Unity Catalog.
            "OSS_OUT_SCHEMA": args.out_schema,
            "OSS_LOCATION_ROOT": f"file://{os.path.join(DATA, args.users_catalog, args.out_schema)}",
        }
    )

    spark = build_session(
        args.uc_uri, [args.users_catalog, args.cities_catalog], default_catalog=args.users_catalog
    )
    print(f"spark {spark.version} | Unity Catalog OSS {args.uc_uri}")
    spark.sql(f"create schema if not exists {args.users_catalog}.{args.out_schema}")

    target = f"{args.users_catalog}.{args.out_schema}.users_enriched"
    location = os.path.join(DATA, args.users_catalog, args.out_schema, "users_enriched")
    ensure_target(spark, args.uc_uri, target, location)
    print(f"  target ready: {target} -> file://{location}")
    # dbt-spark's session method picks up this active session.
    from dbt.cli.main import dbtRunner

    runner = dbtRunner()
    failures = []
    for command in ("run", "test"):
        argv_dbt = [command, "--project-dir", PROJECT, "--profiles-dir", PROJECT]
        print(f"\n→ dbt {' '.join(argv_dbt)}")
        result = runner.invoke(argv_dbt)
        print(f"  success={result.success}")
        if result.exception:
            print(f"  exception: {result.exception}")
            failures.append(command)
            continue
        for node in getattr(result.result, "results", []) or []:
            unique_id = getattr(node, "unique_id", None) or getattr(getattr(node, "node", None), "unique_id", "?")
            status = getattr(node.status, "value", str(node.status))
            print(f"    {status:<8} {unique_id}")
            if status in ("error", "fail", "runtime error"):
                failures.append(unique_id)

    print(f"\n--- {target} ---")
    spark.sql(
        f"select user_id, full_name, city, region, population, latitude, longitude, primary_tag "
        f"from {target} order by user_id limit 5"
    ).show(truncate=30)
    print("rows:", spark.table(target).count())
    spark.stop()

    if failures:
        print("\nFAILED:", failures)
        return 1
    print("\nOK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
