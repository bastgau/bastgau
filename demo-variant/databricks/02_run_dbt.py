# Databricks notebook source
# MAGIC %md
# MAGIC # 2. Run dbt from python, in the notebook
# MAGIC
# MAGIC Joins `demo_users.core.users` with `demo_cities.core.cities` and flattens the
# MAGIC VARIANT metadata into `demo_users.marts.users_enriched` (Delta).
# MAGIC
# MAGIC dbt runs **in this process** through `dbtRunner`, so the run results come back as
# MAGIC Python objects. The SQL itself goes to the **SQL warehouse** named by `http_path`:
# MAGIC the notebook's compute only hosts the dbt process.

# COMMAND ----------

dbutils.widgets.text("http_path", "", "SQL warehouse http_path (required)")
dbutils.widgets.text("users_catalog", "demo_users", "Catalog holding users + the output")
dbutils.widgets.text("cities_catalog", "demo_cities", "Catalog holding cities")
dbutils.widgets.text("src_schema", "core", "Schema of the two source tables")
dbutils.widgets.text("out_schema", "marts", "Schema for the dbt output")
dbutils.widgets.text("secret_scope", "", "Secret scope holding the token (blank = notebook token)")
dbutils.widgets.text("secret_key", "dbt_token", "Secret key holding the token")
dbutils.widgets.text("pip_spec", "dbt-databricks", "pip spec: dbt-databricks (v1) or dbt==2.0.4")

# COMMAND ----------

get_ipython().run_line_magic(  # noqa: F821  (provided by the notebook runtime)
    "pip", f"install --quiet {dbutils.widgets.get('pip_spec')}"
)

# COMMAND ----------

# The engine can be a native extension: restart so the fresh install is the one imported.
dbutils.library.restartPython()

# COMMAND ----------

import os
import shutil
import tempfile

http_path = dbutils.widgets.get("http_path").strip()
users_catalog = dbutils.widgets.get("users_catalog").strip()
cities_catalog = dbutils.widgets.get("cities_catalog").strip()
src_schema = dbutils.widgets.get("src_schema").strip()
out_schema = dbutils.widgets.get("out_schema").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip()
secret_key = dbutils.widgets.get("secret_key").strip()

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()


def resolve(path):
    """A workspace path is reachable as is, or under /Workspace, depending on the runtime."""
    return path if os.path.isdir(path) else os.path.join("/Workspace", path.lstrip("/"))


notebook_dir = resolve(os.path.dirname(ctx.notebookPath().get()))
project_src = os.path.realpath(os.path.join(notebook_dir, "dbt_project"))

host = ""
for source in (
    lambda: spark.conf.get("spark.databricks.workspaceUrl"),  # noqa: F821
    lambda: ctx.browserHostName().get(),
):
    try:
        host = source()
    except Exception:  # pragma: no cover - runtime specific
        host = ""
    if host:
        break
host = host.replace("https://", "").rstrip("/")

if not http_path:
    raise RuntimeError(
        "http_path is required: dbt talks to a SQL warehouse, it cannot use this notebook's compute"
    )
token = dbutils.secrets.get(scope=secret_scope, key=secret_key) if secret_scope else ctx.apiToken().get()

# dbt writes target/ and logs/ next to the project; workspace files are read-only.
project = os.path.join(tempfile.mkdtemp(prefix="dbt-"), "project")
shutil.copytree(project_src, project, ignore=shutil.ignore_patterns("target", "logs"))

os.environ.update(
    {
        "DBT_HOST": host,
        "DBT_HTTP_PATH": http_path,
        "DBT_TOKEN": token,
        "USERS_CATALOG": users_catalog,
        "CITIES_CATALOG": cities_catalog,
        "SRC_SCHEMA": src_schema,
        "DBT_SCHEMA": out_schema,
    }
)
print(f"project : {project}\nhost    : {host}\npath    : {http_path}")
print(f"sources : {users_catalog}.{src_schema}.users + {cities_catalog}.{src_schema}.cities")
print(f"output  : {users_catalog}.{out_schema}.users_enriched")

# COMMAND ----------

# MAGIC %md ## Run it

# COMMAND ----------

try:  # dbt v2
    from dbt.runner import dbtRunner
except ImportError:  # dbt-core 1.x
    from dbt.cli.main import dbtRunner

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
    """dbt v2 puts unique_id on the row; dbt-core 1.x puts it on result.node."""
    return getattr(result, "unique_id", None) or getattr(getattr(result, "node", None), "unique_id", "?")


def node_status(result):
    status = getattr(result, "status", None)
    return getattr(status, "value", str(status))


rows = [
    [node_id(node), node_status(node), str(round(getattr(node, "execution_time", 0.0) or 0.0, 2))]
    for res in results.values()
    for node in (getattr(res.result, "results", []) or [])
]
display(
    spark.createDataFrame(rows, schema="node string, status string, seconds string")  # noqa: F821
)

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

dbutils.notebook.exit(f"{users_catalog}.{out_schema}.users_enriched built and tested")
