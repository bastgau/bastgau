#!/usr/bin/env python3
"""Execute the Databricks notebook's cells outside Databricks, with stubs.

Lets the notebook be tested without a cluster: it stubs dbutils, spark and display,
then runs every cell. What it really exercises: widget reading, path resolution,
the project copy, env wiring, the dbtRunner call, the result rows and the failure
gate. What it cannot exercise: %pip install, restartPython, real secret scopes,
the cluster http_path fallback and display() rendering.

Usage (with env.local sourced, see ../bootstrap.sh):
    python runner/test_notebook_locally.py runner/databricks_dbt_notebook.py
    python runner/test_notebook_locally.py runner/databricks_dbt_notebook.py "run --select stg_customers"

Exit code 0 means the notebook ran through; a dbt failure propagates as the
RuntimeError the notebook raises to fail its Job task.
"""
import os
import sys
import types

NOTEBOOK = sys.argv[1]
RUNNER_DIR = os.path.dirname(os.path.abspath(NOTEBOOK))

WIDGETS = {
    "project_dir": "",  # exercise the default: sibling ../fixture of the notebook dir
    "catalog": os.environ["DBT_CATALOG"],
    "schema": os.environ["DBT_SCHEMA"],
    "http_path": os.environ["DBT_HTTP_PATH"],
    "secret_scope": "",  # exercise the ctx.apiToken() branch
    "secret_key": "dbt_token",
    "dbt_version": "2.0.4",
    "commands": sys.argv[2] if len(sys.argv) > 2 else "seed --select seed_country\nrun --select stg_customers",
    "threads": "4",
}


class _Widgets:
    def text(self, name, default, label=""):
        WIDGETS.setdefault(name, default)

    def get(self, name):
        return WIDGETS[name]


class _Val:
    def __init__(self, v):
        self._v = v

    def get(self):
        return self._v


class _Tags:
    def apply(self, key):
        return {"orgId": "0", "clusterId": "stub-cluster"}[key]


class _Ctx:
    def notebookPath(self):
        return _Val(os.path.join(RUNNER_DIR, "databricks_dbt_notebook"))

    def apiToken(self):
        return _Val(os.environ["DBT_TOKEN"])

    def tags(self):
        return _Tags()


class _Notebook:
    entry_point = types.SimpleNamespace(
        getDbutils=lambda: types.SimpleNamespace(notebook=lambda: types.SimpleNamespace(getContext=_Ctx))
    )

    def exit(self, message):
        print("dbutils.notebook.exit:", message)


class _DBUtils:
    widgets = _Widgets()
    notebook = _Notebook()
    library = types.SimpleNamespace(restartPython=lambda: print("[stub] restartPython"))
    secrets = types.SimpleNamespace(get=lambda scope, key: os.environ["DBT_TOKEN"])


class _Spark:
    conf = types.SimpleNamespace(get=lambda key: os.environ["DBT_HOST"])

    def createDataFrame(self, rows, schema=None):
        return {"schema": schema, "rows": rows}


# The stub context is built by calling _Ctx(), so patch entry_point accordingly.
_Notebook.entry_point = types.SimpleNamespace(
    getDbutils=lambda: types.SimpleNamespace(
        notebook=lambda: types.SimpleNamespace(getContext=lambda: _Ctx())
    )
)

class _IPython:
    """The notebook installs dbt through the pip magic; here it is a no-op."""

    def run_line_magic(self, magic, line):
        print(f"[stub] %{magic} {line}")


env = {
    "dbutils": _DBUtils(),
    "spark": _Spark(),
    "display": lambda rows: print("display():", rows),
    "get_ipython": lambda: _IPython(),
    "__name__": "__notebook__",
}

source = open(NOTEBOOK).read()
cells = source.split("# COMMAND ----------")
skipped = 0
for i, cell in enumerate(cells):
    body = "\n".join(
        line for line in cell.splitlines() if not line.strip().startswith("# MAGIC") and line.strip() != "# Databricks notebook source"
    )
    if not body.strip():
        continue
    if "%pip" in cell:
        skipped += 1
        print(f"[skip] cell {i}: %pip install")
        continue
    print(f"\n=== cell {i} ===")
    exec(compile(body, f"{NOTEBOOK}#cell{i}", "exec"), env)  # noqa: S102
print(f"\nnotebook simulated ({len(cells)} cells, {skipped} skipped)")
