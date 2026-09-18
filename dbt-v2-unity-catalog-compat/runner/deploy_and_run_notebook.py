#!/usr/bin/env python3
"""Deploy the dbt project + notebook to a Databricks workspace and run it as a job.

It uploads `runner/` (notebook and module) and `fixture/` (the dbt project) as
workspace files, creates or updates a one-task job whose notebook task runs on
serverless compute, triggers it and reports the outcome.

Credentials: DBT_HOST / DBT_TOKEN. Target: DBT_CATALOG / DBT_SCHEMA / DBT_HTTP_PATH
(a SQL warehouse path — serverless notebook compute has no cluster to borrow).

    python runner/deploy_and_run_notebook.py --commands "seed" --commands "build --exclude st_customers"
    python runner/deploy_and_run_notebook.py --workspace-dir /Users/me/dbt_uc_compat --no-run
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
PROJECT_ROOT = os.path.dirname(HERE)
HOST = os.environ.get("DBT_HOST", "").replace("https://", "").rstrip("/")
TOKEN = os.environ.get("DBT_TOKEN", "")

NOTEBOOK_BASENAME = "databricks_dbt_notebook"
# Files worth shipping: the notebook's module, and the dbt project itself.
UPLOAD = [
    ("runner", ["run_dbt_job.py"]),
    ("fixture", None),          # dbt v2 project
    ("dbt1x-databricks", None), # dbt-core 1.x project
]
SKIP_DIRS = {"target", "logs", "__pycache__", ".venv"}
SKIP_FILES = {"env.local", "profiles.yml"}  # the notebook generates its own profile


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
        raise SystemExit(f"HTTP {exc.code} on {path}: {exc.read().decode(errors='replace')[:600]}")


def upload_file(local_path, remote_path, notebook=False):
    with open(local_path, "rb") as handle:
        content = base64.b64encode(handle.read()).decode()
    payload = {
        "path": remote_path,
        "content": content,
        "overwrite": True,
        # SOURCE + PYTHON makes it a notebook; AUTO keeps everything else a plain file.
        "format": "SOURCE" if notebook else "AUTO",
    }
    if notebook:
        payload["language"] = "PYTHON"
    api("/api/2.0/workspace/import", payload)


def deploy(workspace_dir):
    api("/api/2.0/workspace/mkdirs", {"path": workspace_dir})
    uploaded = []

    # The notebook lands without its .py extension so the workspace treats it as a notebook.
    api("/api/2.0/workspace/mkdirs", {"path": f"{workspace_dir}/runner"})
    upload_file(
        os.path.join(HERE, f"{NOTEBOOK_BASENAME}.py"),
        f"{workspace_dir}/runner/{NOTEBOOK_BASENAME}",
        notebook=True,
    )
    uploaded.append(f"{workspace_dir}/runner/{NOTEBOOK_BASENAME} (notebook)")

    for folder, only in UPLOAD:
        base = os.path.join(PROJECT_ROOT, folder)
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            rel_root = os.path.relpath(root, PROJECT_ROOT)
            remote_root = f"{workspace_dir}/{rel_root}".replace("\\", "/")
            api("/api/2.0/workspace/mkdirs", {"path": remote_root})
            for name in sorted(files):
                if name in SKIP_FILES or name.endswith(".pyc"):
                    continue
                if only is not None and name not in only:
                    continue
                upload_file(os.path.join(root, name), f"{remote_root}/{name}")
                uploaded.append(f"{remote_root}/{name}")
    return uploaded


def upsert_job(name, notebook_path, parameters):
    """Create the job, or reset it if a job with the same name already exists."""
    task = {
        "task_key": "dbt_job",
        "notebook_task": {"notebook_path": notebook_path, "base_parameters": parameters},
        # No cluster key at all: the task runs on serverless compute.
    }
    settings = {"name": name, "tasks": [task], "max_concurrent_runs": 1, "timeout_seconds": 3600}

    existing = api(f"/api/2.2/jobs/list?name={urllib.parse.quote(name)}").get("jobs") or []
    if existing:
        job_id = existing[0]["job_id"]
        api("/api/2.2/jobs/reset", {"job_id": job_id, "new_settings": settings})
        return job_id, "updated"
    return api("/api/2.2/jobs/create", settings)["job_id"], "created"


def run_job(job_id, poll_seconds=1800):
    run_id = api("/api/2.2/jobs/run-now", {"job_id": job_id})["run_id"]
    print(f"  run_id {run_id}: https://{HOST}/#job/{job_id}/run/{run_id}")
    deadline = time.time() + poll_seconds
    state = {}
    while time.time() < deadline:
        run = api(f"/api/2.2/jobs/runs/get?run_id={run_id}")
        state = run.get("state", {})
        if state.get("life_cycle_state") in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        print(f"  {state.get('life_cycle_state')} {state.get('state_message', '')[:80]}")
        time.sleep(15)
    tasks = api(f"/api/2.2/jobs/runs/get?run_id={run_id}").get("tasks") or []
    output = {}
    if tasks:
        output = api(f"/api/2.2/jobs/runs/get-output?run_id={tasks[0]['run_id']}")
    return state, output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workspace-dir", help="target workspace folder (default: /Users/<me>/dbt_uc_compat)")
    parser.add_argument("--job-name", default="dbt-v2-uc-compat-notebook")
    parser.add_argument("--commands", action="append", help="dbt command, repeatable (default: build)")
    parser.add_argument("--dbt-version", default="2.0.4")
    parser.add_argument("--pip-spec", default="", help="pip spec to install instead of dbt==<version>, e.g. dbt-databricks==1.12.5")
    parser.add_argument("--project", default="fixture", help="project folder to run: fixture (v2) or dbt1x-databricks")
    parser.add_argument("--profile-name", default="", help="profile name in dbt_project.yml (default: uc_compat, or uc_compat_v1 for the 1.x project)")
    parser.add_argument("--secret-scope", default="", help="scope holding the token (default: the run-as user's token)")
    parser.add_argument("--no-run", action="store_true", help="deploy only")
    args = parser.parse_args(argv)

    if not HOST or not TOKEN:
        raise SystemExit("set DBT_HOST and DBT_TOKEN")

    me = api("/api/2.0/preview/scim/v2/Me")["userName"]
    workspace_dir = args.workspace_dir or f"/Users/{me}/dbt_uc_compat"

    print(f"Deploying to {workspace_dir} (as {me})")
    for path in deploy(workspace_dir):
        print(f"  {path}")

    profile_name = args.profile_name or ("uc_compat_v1" if args.project == "dbt1x-databricks" else "uc_compat")
    parameters = {
        "project_dir": f"{workspace_dir}/{args.project}",
        "pip_spec": args.pip_spec,
        "profile_name": profile_name,
        "catalog": os.environ.get("DBT_CATALOG", "main"),
        "schema": os.environ.get("DBT_SCHEMA", "dbt_uc_compat"),
        "http_path": os.environ.get("DBT_HTTP_PATH", ""),
        "host": HOST,
        "secret_scope": args.secret_scope,
        "secret_key": "dbt_token",
        "dbt_version": args.dbt_version,
        "commands": "\n".join(args.commands or ["build"]),
        "threads": "4",
    }
    notebook_path = f"{workspace_dir}/runner/{NOTEBOOK_BASENAME}"
    job_id, action = upsert_job(args.job_name, notebook_path, parameters)
    print(f"\nJob {job_id} {action} (serverless notebook task)")

    if args.no_run:
        return 0

    print("\nRunning")
    state, output = run_job(job_id)
    print(f"\nresult: {state.get('result_state')} — {state.get('state_message', '')[:300]}")
    if output.get("notebook_output"):
        print("notebook_output:", json.dumps(output["notebook_output"])[:500])
    if output.get("error"):
        print("error:", output["error"][:1000])
    if output.get("error_trace"):
        print("trace:", output["error_trace"][-1500:])
    return 0 if state.get("result_state") == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
