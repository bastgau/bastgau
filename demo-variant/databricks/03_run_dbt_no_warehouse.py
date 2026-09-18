# Databricks notebook source
# MAGIC %md
# MAGIC # 3. Run dbt **without a SQL warehouse**
# MAGIC
# MAGIC Same join, same VARIANT flattening as `02_run_dbt`, but with `dbt-spark` in
# MAGIC `method: session` instead of `dbt-databricks`: dbt-spark calls
# MAGIC `SparkSession.builder.getOrCreate()`, which in a notebook returns the notebook's own
# MAGIC session, so **the SQL runs on this compute** — one compute, no warehouse, no
# MAGIC `http_path`, no token.
# MAGIC
# MAGIC The trade-off: dbt-spark is not Unity-Catalog-aware. It renders two-part names only,
# MAGIC so the current catalog is set with `USE CATALOG` and a source in another catalog
# MAGIC carries its catalog inside `schema`. dbt v2 has no session adapter, so this is a
# MAGIC dbt 1.x-only setup.

# COMMAND ----------

dbutils.widgets.text("users_catalog", "demo_users", "Catalog holding users + the output")
dbutils.widgets.text("cities_catalog", "demo_cities", "Catalog holding cities")
dbutils.widgets.text("src_schema", "core", "Schema of the two source tables")
dbutils.widgets.text("out_schema", "marts_session", "Schema for the dbt output")
dbutils.widgets.text("pip_spec", "dbt-spark", "pip spec (dbt-spark: session method)")

# COMMAND ----------

get_ipython().run_line_magic(  # noqa: F821  (provided by the notebook runtime)
    "pip", f"install --quiet {dbutils.widgets.get('pip_spec')}"
)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import shutil
import tempfile

users_catalog = dbutils.widgets.get("users_catalog").strip()
cities_catalog = dbutils.widgets.get("cities_catalog").strip()
src_schema = dbutils.widgets.get("src_schema").strip()
out_schema = dbutils.widgets.get("out_schema").strip()

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()


def resolve(path):
    """A workspace path is reachable as is, or under /Workspace, depending on the runtime."""
    return path if os.path.isdir(path) else os.path.join("/Workspace", path.lstrip("/"))


notebook_dir = resolve(os.path.dirname(ctx.notebookPath().get()))
project_src = os.path.realpath(os.path.join(notebook_dir, "dbt_project_session"))

# dbt writes target/ and logs/ next to the project; workspace files are read-only.
project = os.path.join(tempfile.mkdtemp(prefix="dbt-session-"), "project")
shutil.copytree(project_src, project, ignore=shutil.ignore_patterns("target", "logs"))

os.environ.update(
    {
        "USERS_SCHEMA": f"{users_catalog}.{src_schema}",
        "CITIES_SCHEMA": f"{cities_catalog}.{src_schema}",
        "DBT_SCHEMA": out_schema,
    }
)

# dbt-spark renders two-part names, so the output catalog is the session's current one.
spark.sql(f"use catalog {users_catalog}")  # noqa: F821
spark.sql(f"create schema if not exists {users_catalog}.{out_schema}")  # noqa: F821

print(f"project : {project}")
print(f"session : {type(spark).__module__}.{type(spark).__name__}")  # noqa: F821
print(f"catalog : {spark.sql('select current_catalog()').collect()[0][0]}")  # noqa: F821
print(f"sources : {users_catalog}.{src_schema}.users + {cities_catalog}.{src_schema}.cities")
print(f"output  : {users_catalog}.{out_schema}.users_enriched")

# COMMAND ----------

# MAGIC %md ## Run it — nothing here talks to a warehouse

# COMMAND ----------

from dbt.cli.main import dbtRunner  # dbt 1.x: dbt v2 has no session adapter

runner = dbtRunner()
results = {}
for command in ("run", "test"):
    argv = [command, "--project-dir", project, "--profiles-dir", project]
    print(f"\n→ dbt {' '.join(argv)}")
    res = runner.invoke(argv)
    results[command] = res
    print(f"  success={res.success}")
    if res.exception:
        raise RuntimeError(f"dbt {command} failed: {res.exception}")

# COMMAND ----------

# MAGIC %md ## What dbt produced

# COMMAND ----------

def node_id(result):
    """dbt-core 1.x puts unique_id on result.node."""
    return getattr(result, "unique_id", None) or getattr(getattr(result, "node", None), "unique_id", "?")


def node_status(result):
    status = getattr(result, "status", None)
    return getattr(status, "value", str(status))


rows = [
    [node_id(node), node_status(node), str(round(getattr(node, "execution_time", 0.0) or 0.0, 2))]
    for res in results.values()
    for node in (getattr(res.result, "results", []) or [])
]
display(spark.createDataFrame(rows, schema="node string, status string, seconds string"))  # noqa: F821

display(
    spark.sql(  # noqa: F821
        f"""
        select user_id, full_name, city, region, population, latitude, longitude, primary_tag
        from {users_catalog}.{out_schema}.users_enriched
        order by user_id limit 10
        """
    )
)

failed = [
    node_id(node)
    for res in results.values()
    for node in (getattr(res.result, "results", []) or [])
    if node_status(node) in ("error", "fail", "runtime error")
]
if failed:
    raise RuntimeError(f"dbt reported failures: {failed}")

dbutils.notebook.exit(
    f"{users_catalog}.{out_schema}.users_enriched built and tested, no SQL warehouse involved"
)
