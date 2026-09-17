# Databricks notebook source
# MAGIC %md
# MAGIC # Run a dbt v2 job with dbtRunner on Databricks
# MAGIC
# MAGIC Runs a dbt project in-process (no shell-out) and returns the run artifacts as
# MAGIC Python objects. Works as an interactive notebook and as a Databricks Job task.
# MAGIC
# MAGIC **Requirements**
# MAGIC * The dbt project is available to this notebook: a Git folder (Repos), a Workspace
# MAGIC   folder, or a Volume path.
# MAGIC * A SQL warehouse `http_path`, or this notebook's own cluster (see the compute cell).
# MAGIC * A token in a secret scope — never a token typed into a widget: widget values are
# MAGIC   stored with the notebook and shown in the job run details.
# MAGIC
# MAGIC **Widgets**: `project_dir`, `catalog`, `schema`, `http_path`, `secret_scope`,
# MAGIC `secret_key`, `dbt_version`, `commands`, `threads`.

# COMMAND ----------

dbutils.widgets.text("project_dir", "", "Project dir (Repos/Workspace/Volume path)")
dbutils.widgets.text("catalog", "workspace", "UC catalog")
dbutils.widgets.text("schema", "dbt_uc_compat", "UC schema")
dbutils.widgets.text("http_path", "", "SQL warehouse http_path (blank = this cluster)")
dbutils.widgets.text("secret_scope", "", "Secret scope holding the token")
dbutils.widgets.text("secret_key", "dbt_token", "Secret key holding the token")
dbutils.widgets.text("dbt_version", "2.0.4", "dbt version to install")
dbutils.widgets.text("commands", "build", "dbt commands, one per line")
dbutils.widgets.text("threads", "4", "threads")

# COMMAND ----------

# MAGIC %md ## 1. Install dbt v2
# MAGIC v2 bundles every adapter, so `dbt-databricks` must NOT be installed alongside it.

# COMMAND ----------

# MAGIC %pip install --quiet "dbt==$dbt_version"

# COMMAND ----------

# The engine is a native extension: restart Python so the fresh install is the one imported.
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## 2. Resolve the project, the compute and the credentials

# COMMAND ----------

import os
import shutil
import tempfile

project_dir = dbutils.widgets.get("project_dir").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
http_path = dbutils.widgets.get("http_path").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip()
secret_key = dbutils.widgets.get("secret_key").strip()
threads = int(dbutils.widgets.get("threads") or 4)
commands = [line.strip() for line in dbutils.widgets.get("commands").splitlines() if line.strip()]

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()


def resolve(path):
    """A notebook path is reachable as is, or under /Workspace depending on the runtime."""
    return path if os.path.isdir(path) else os.path.join("/Workspace", path.lstrip("/"))


notebook_dir = resolve(os.path.dirname(ctx.notebookPath().get()))

if not project_dir:
    # Default to the sibling of this notebook's folder (…/runner → …/fixture).
    project_dir = os.path.join(notebook_dir, "..", "fixture")
project_dir = os.path.realpath(project_dir)

host = spark.conf.get("spark.databricks.workspaceUrl")

if not http_path:
    # Fall back to this cluster's SQL endpoint. A SQL warehouse is preferable for dbt:
    # materialized views and streaming tables are DBSQL features.
    org_id = ctx.tags().apply("orgId")
    cluster_id = ctx.tags().apply("clusterId")
    http_path = f"/sql/protocolv1/o/{org_id}/{cluster_id}"

if secret_scope:
    token = dbutils.secrets.get(scope=secret_scope, key=secret_key)
else:
    # Notebook-scoped API token of the current user. Fine for interactive runs; for a Job,
    # use a secret scope so the run is not tied to whoever last edited the notebook.
    token = ctx.apiToken().get()

print(f"project : {project_dir}")
print(f"host    : {host}")
print(f"path    : {http_path}")
print(f"target  : {catalog}.{schema}")
print(f"commands: {commands}")

# COMMAND ----------

# MAGIC %md ## 3. Make the project writable
# MAGIC dbt writes `target/` and `logs/` next to the project. Workspace files and Git
# MAGIC folders are read-only at runtime, so the project is copied to local disk.

# COMMAND ----------

work_dir = os.path.join(tempfile.mkdtemp(prefix="dbt-project-"), "project")
shutil.copytree(project_dir, work_dir, ignore=shutil.ignore_patterns("target", "logs", ".venv", "env.local"))
# A profiles.yml shipped in the repo would take precedence over the one generated below.
if os.path.exists(os.path.join(work_dir, "profiles.yml")):
    os.remove(os.path.join(work_dir, "profiles.yml"))
print(work_dir, "->", sorted(os.listdir(work_dir)))

# COMMAND ----------

# MAGIC %md ## 4. Run the job through dbtRunner
# MAGIC `run_dbt_job` is the module next to this notebook: same code path as the CLI usage,
# MAGIC so a job behaves like a local run. It writes a 0600 `profiles.yml` from the
# MAGIC environment into a temporary directory and deletes it when the job ends.

# COMMAND ----------

import sys

if notebook_dir not in sys.path:
    sys.path.insert(0, notebook_dir)

from run_dbt_job import run_dbt_job  # noqa: E402

os.environ.update(
    {
        "DBT_HOST": host,
        "DBT_HTTP_PATH": http_path,
        "DBT_TOKEN": token,
        "DBT_CATALOG": catalog,
        "DBT_SCHEMA": schema,
        # The fixture resolves its declared source through these; harmless otherwise.
        "DBT_SOURCE_CATALOG": catalog,
        "DBT_SOURCE_SCHEMA": f"{schema}_bronze",
        "DBT_SOURCE_TABLE": "seed_bronze_customers",
        "DBT_SOURCE_TS_COLUMN": "_ingested_at",
    }
)

report = run_dbt_job(
    commands=commands,
    project_dir=work_dir,
    generate_profile=True,   # writes profiles.yml from the env vars above
    profile_name="uc_compat",  # must match `profile:` in dbt_project.yml
    target="live",
    threads=threads,
    # A scheduled job must not report success because --select matched nothing.
    fail_on_empty=True,
)

# COMMAND ----------

# MAGIC %md ## 5. Results

# COMMAND ----------

rows = report.rows()
if rows:
    display(spark.createDataFrame(rows))
else:
    print("no node-level results (e.g. `dbt parse`)")

# COMMAND ----------

# Fail the notebook — and therefore the Job task — when dbt failed, so alerting works.
report.raise_for_status()
dbutils.notebook.exit(f"dbt ok: {len(rows)} nodes, exit {report.exit_code}")
