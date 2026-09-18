#!/usr/bin/env python3
"""Upload the two Databricks notebooks (and the dbt project) and run them as jobs.

Serverless notebook tasks, so no cluster is needed. The SQL still goes to the
warehouse named by --http-path.

    export DBT_HOST=adb-xxx.cloud.databricks.com DBT_TOKEN=dapi...
    python deploy_databricks.py --http-path /sql/1.0/warehouses/xxx
    python deploy_databricks.py --http-path ... --only 01     # just the first notebook
    python deploy_databricks.py --only 03                     # dbt without any warehouse
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
HOST = os.environ.get("DBT_HOST", "").replace("https://", "").rstrip("/")
TOKEN = os.environ.get("DBT_TOKEN", "")
NOTEBOOKS = {"01": "01_create_tables", "02": "02_run_dbt", "03": "03_run_dbt_no_warehouse"}
PROJECTS = ("dbt_project", "dbt_project_session")


def api(path, payload=None, method=None):
    req = urllib.request.Request(
        f"https://{HOST}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method=method or ("POST" if payload is not None else "GET"),
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} on {path}: {exc.read().decode(errors='replace')[:500]}")


def upload(local, remote, notebook=False):
    with open(local, "rb") as handle:
        content = base64.b64encode(handle.read()).decode()
    payload = {"path": remote, "content": content, "overwrite": True,
               "format": "SOURCE" if notebook else "AUTO"}
    if notebook:
        payload["language"] = "PYTHON"
    api("/api/2.0/workspace/import", payload)
    return remote


def deploy(root):
    api("/api/2.0/workspace/mkdirs", {"path": root})
    done = []
    for name in NOTEBOOKS.values():
        # No extension: the workspace then treats the file as a notebook.
        done.append(upload(os.path.join(HERE, "databricks", f"{name}.py"), f"{root}/{name}", notebook=True))
    for project in PROJECTS:
        for folder, _, files in os.walk(os.path.join(HERE, "databricks", project)):
            rel = os.path.relpath(folder, os.path.join(HERE, "databricks"))
            api("/api/2.0/workspace/mkdirs", {"path": f"{root}/{rel}"})
            for f in sorted(files):
                if f.endswith((".pyc",)) or "target" in rel or "logs" in rel:
                    continue
                done.append(upload(os.path.join(folder, f), f"{root}/{rel}/{f}"))
    return done


def upsert_job(name, notebook_path, parameters):
    settings = {
        "name": name,
        "tasks": [{
            "task_key": "demo",
            "notebook_task": {"notebook_path": notebook_path, "base_parameters": parameters},
        }],  # no cluster key: serverless
        "max_concurrent_runs": 1,
        "timeout_seconds": 3600,
    }
    existing = api(f"/api/2.2/jobs/list?name={urllib.parse.quote(name)}").get("jobs") or []
    if existing:
        api("/api/2.2/jobs/reset", {"job_id": existing[0]["job_id"], "new_settings": settings})
        return existing[0]["job_id"]
    return api("/api/2.2/jobs/create", settings)["job_id"]


def run(job_id, timeout=2400):
    run_id = api("/api/2.2/jobs/run-now", {"job_id": job_id})["run_id"]
    print(f"    run {run_id}: https://{HOST}/#job/{job_id}/run/{run_id}")
    deadline = time.time() + timeout
    state = {}
    while time.time() < deadline:
        state = api(f"/api/2.2/jobs/runs/get?run_id={run_id}").get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(15)
    tasks = api(f"/api/2.2/jobs/runs/get?run_id={run_id}").get("tasks") or []
    out = api(f"/api/2.2/jobs/runs/get-output?run_id={tasks[0]['run_id']}") if tasks else {}
    return state, out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--http-path", help="SQL warehouse http_path (required for 02 only)")
    parser.add_argument("--workspace-dir", help="default: /Users/<me>/demo_variant")
    parser.add_argument("--users-catalog", default="demo_users")
    parser.add_argument("--cities-catalog", default="demo_cities")
    parser.add_argument("--src-schema", default="core")
    parser.add_argument("--out-schema", default="marts")
    parser.add_argument("--pip-spec", default="dbt-databricks", help="02: dbt-databricks (v1) or dbt==2.0.4")
    parser.add_argument("--session-pip-spec", default="dbt-spark", help="03: adapter used without a warehouse")
    parser.add_argument("--session-out-schema", default="marts_session", help="03: output schema")
    parser.add_argument("--only", choices=sorted(NOTEBOOKS), help="run a single notebook")
    parser.add_argument("--no-run", action="store_true")
    args = parser.parse_args(argv)

    if not HOST or not TOKEN:
        raise SystemExit("set DBT_HOST and DBT_TOKEN")
    selection = [args.only] if args.only else sorted(NOTEBOOKS)
    if "02" in selection and not args.http_path:
        raise SystemExit("--http-path is required for notebook 02 (dbt-databricks needs a SQL warehouse)")
    me = api("/api/2.0/preview/scim/v2/Me")["userName"]
    root = args.workspace_dir or f"/Users/{me}/demo_variant"

    print(f"Deploying to {root}")
    for path in deploy(root):
        print(f"  {path}")

    shared = {
        "users_catalog": args.users_catalog,
        "cities_catalog": args.cities_catalog,
    }
    params = {
        "01": {**shared, "schema": args.src_schema},
        "02": {**shared, "http_path": args.http_path or "", "src_schema": args.src_schema,
               "out_schema": args.out_schema, "pip_spec": args.pip_spec,
               "secret_scope": "", "secret_key": "dbt_token"},
        # No http_path: 03 runs dbt-spark on the notebook's own compute.
        "03": {**shared, "src_schema": args.src_schema, "out_schema": args.session_out_schema,
               "pip_spec": args.session_pip_spec},
    }
    rc = 0
    for key in selection:
        name = NOTEBOOKS[key]
        job_id = upsert_job(f"demo-variant-{key}", f"{root}/{name}", params[key])
        print(f"\n{key} {name}: job {job_id}")
        if args.no_run:
            continue
        state, out = run(job_id)
        print(f"    {state.get('result_state')} — {(state.get('state_message') or '')[:200]}")
        if out.get("notebook_output"):
            print("    output:", json.dumps(out["notebook_output"])[:300])
        if out.get("error"):
            print("    error:", out["error"][:600])
        if state.get("result_state") != "SUCCESS":
            rc = 1
            break
    return rc


if __name__ == "__main__":
    sys.exit(main())
