#!/usr/bin/env python3
"""Create the same two tables in a local Unity Catalog OSS, through Unity Catalog.

Two catalogs, two external **Delta** tables written *through* the UC Spark connector —
the data is not dropped into the path behind the catalog's back:

  demo_users.core.users    100 fake users with a city
  demo_cities.core.cities  city reference, metadata column

Files land under ./data/<catalog>/<schema>/<table>.

Run Unity Catalog OSS first:  ./start_uc_oss.sh --bg
Then:                         .venv/bin/python uc_oss/01_create_tables_spark.py

VARIANT needs Spark 4. The UC OSS Spark connector 0.4.1 is published against Spark 3.5,
but it does accept a VARIANT column on Spark 4.0.1 (verified). The script tries VARIANT
first and falls back to a JSON string column, reporting which one it used — pass
--no-variant to skip the attempt.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DATA = os.path.join(ROOT, "data")
JARS = os.path.join(ROOT, ".jars")

UC_CONNECTOR = "io.unitycatalog:unitycatalog-spark_2.13:0.4.1"
# Delta 4.4 loads io.unitycatalog.client.delta.model.* by reflection (UC coordinated
# commits). The connector pins client 0.4.1, which does not carry those classes, so the
# client is pinned here to the server version — a direct dependency wins in Maven.
UC_CLIENT = "io.unitycatalog:unitycatalog-client:0.6.0"
DELTA = "io.delta:delta-spark_4.1_2.13:4.4.0"  # Spark-qualified artifact: the 4.1 line

CITIES = [
    ("Paris", "FR", {"population": 2133111, "region": "Ile-de-France",
                     "coords": {"lat": 48.8566, "lon": 2.3522}, "tags": ["capital", "tier1"]}),
    ("Lyon", "FR", {"population": 522250, "region": "Auvergne-Rhone-Alpes",
                    "coords": {"lat": 45.7640, "lon": 4.8357}, "tags": ["tier2", "gastronomy"]}),
    ("Marseille", "FR", {"population": 873076, "region": "Provence-Alpes-Cote d Azur",
                         "coords": {"lat": 43.2965, "lon": 5.3698}, "tags": ["port", "tier2"]}),
    ("Bordeaux", "FR", {"population": 259809, "region": "Nouvelle-Aquitaine",
                        "coords": {"lat": 44.8378, "lon": -0.5792}, "tags": ["wine", "tier2"]}),
    ("Lille", "FR", {"population": 236234, "region": "Hauts-de-France",
                     "coords": {"lat": 50.6292, "lon": 3.0573}, "tags": ["tier3"]}),
    ("Nantes", "FR", {"population": 320732, "region": "Pays de la Loire",
                      "coords": {"lat": 47.2184, "lon": -1.5536}, "tags": ["tier3", "atlantic"]}),
]


def uc_api(base: str, path: str, payload=None):
    """Create catalogs through UC's own REST API: Spark cannot create a catalog."""
    req = urllib.request.Request(
        f"{base}/api/2.1/unity-catalog{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        # UC OSS answers 400 CATALOG_ALREADY_EXISTS (not 409) when it is already there.
        if "ALREADY_EXISTS" in detail:
            return {}
        raise SystemExit(f"Unity Catalog {path} -> {exc.code}: {detail[:200]}")


def fetch_jars() -> str:
    """Download the connector and Delta once, so Spark does not resolve them on every run."""
    if os.path.isdir(JARS) and os.listdir(JARS):
        return JARS
    os.makedirs(JARS, exist_ok=True)
    pom = os.path.join(JARS, "pom.xml")
    deps = "".join(
        f"<dependency><groupId>{g}</groupId><artifactId>{a}</artifactId><version>{v}</version></dependency>"
        for g, a, v in (x.split(":") for x in (UC_CONNECTOR, UC_CLIENT, DELTA))
    )
    with open(pom, "w") as handle:
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
        SparkSession.builder.appName("uc-oss-demo")
        .master("local[*]")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # Delta refuses to operate unless spark_catalog is Delta-aware. Pointing it at
        # UCSingleCatalog instead fails with "uri must be specified for Unity Catalog
        # 'spark_catalog'", so the session catalog stays DeltaCatalog and only the two
        # demo catalogs are Unity Catalog ones.
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


def create_users(spark, catalog: str, schema: str) -> None:
    location = os.path.join(DATA, catalog, schema, "users")
    spark.sql(f"create schema if not exists {catalog}.{schema}")
    spark.sql(f"drop table if exists {catalog}.{schema}.users")
    spark.sql(f"""
        create table {catalog}.{schema}.users (
            user_id bigint, full_name string, email string, city string, signed_up date
        ) using delta location 'file://{location}'
    """)
    cities = [c[0] for c in CITIES]
    rows = [
        (
            n,
            f"User {n:03d}",
            f"user{n:03d}@example.com",
            cities[n % len(cities)],
            f"2026-01-{(n % 28) + 1:02d}",
        )
        for n in range(1, 101)
    ]
    df = spark.createDataFrame(rows, "user_id bigint, full_name string, email string, city string, signed_up string")
    df.createOrReplaceTempView("users_stage")
    # INSERT through the catalog, so Unity Catalog owns the write.
    spark.sql(f"""
        insert into {catalog}.{schema}.users
        select user_id, full_name, email, city, to_date(signed_up) from users_stage
    """)


def create_cities(spark, catalog: str, schema: str, try_variant: bool) -> str:
    location = os.path.join(DATA, catalog, schema, "cities")
    spark.sql(f"create schema if not exists {catalog}.{schema}")
    rows = [(city, country, json.dumps(meta)) for city, country, meta in CITIES]
    spark.createDataFrame(rows, "city string, country string, payload string").createOrReplaceTempView("cities_stage")

    attempts = (["variant"] if try_variant else []) + ["json_string"]
    last_error = None
    for mode in attempts:
        column = "metadata variant" if mode == "variant" else "metadata string"
        expression = "parse_json(payload)" if mode == "variant" else "payload"
        try:
            spark.sql(f"drop table if exists {catalog}.{schema}.cities")
            spark.sql(f"""
                create table {catalog}.{schema}.cities (
                    city string, country string, {column}
                ) using delta location 'file://{location}'
            """)
            spark.sql(f"""
                insert into {catalog}.{schema}.cities
                select city, country, {expression} from cities_stage
            """)
            return mode
        except Exception as exc:  # noqa: BLE001 - we report the exact reason
            last_error = exc
            print(f"  [{mode}] refused: {str(exc).splitlines()[0][:160]}")
    raise SystemExit(f"could not create the cities table: {last_error}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--uc-uri", default=os.environ.get("UC_URI", "http://127.0.0.1:8080"))
    parser.add_argument("--users-catalog", default="demo_users")
    parser.add_argument("--cities-catalog", default="demo_cities")
    parser.add_argument("--schema", default="core")
    parser.add_argument("--no-variant", action="store_true", help="skip the VARIANT attempt")
    args = parser.parse_args(argv)

    print(f"Unity Catalog OSS at {args.uc_uri}")
    for catalog in (args.users_catalog, args.cities_catalog):
        uc_api(args.uc_uri, "/catalogs", {"name": catalog})
        print(f"  catalog {catalog}")

    spark = build_session(args.uc_uri, [args.users_catalog, args.cities_catalog])
    print(f"  spark {spark.version}")
    try:
        create_users(spark, args.users_catalog, args.schema)
        print(f"  {args.users_catalog}.{args.schema}.users:",
              spark.table(f"{args.users_catalog}.{args.schema}.users").count(), "rows")
        mode = create_cities(spark, args.cities_catalog, args.schema, not args.no_variant)
        print(f"  {args.cities_catalog}.{args.schema}.cities: metadata as {mode}")
        spark.sql(f"select * from {args.cities_catalog}.{args.schema}.cities order by city").show(3, truncate=60)
        print(f"\nmetadata column mode: {mode}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
