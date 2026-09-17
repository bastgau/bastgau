#!/usr/bin/env python3
"""Minimal Databricks REST client used by the compatibility protocol scripts.

Only the standard library is used, so it runs anywhere python3 does.

Credentials come from the environment:
  DBT_HOST   workspace hostname, without scheme (e.g. dbc-xxxx.cloud.databricks.com)
  DBT_TOKEN  personal access token

Subcommands:
  whoami                      print the authenticated principal
  metastore                   print the Unity Catalog metastore assignment
  catalogs                    list UC catalogs (name, type, owner)
  warehouses                  list SQL warehouses (id, name, state, serverless)
  pick-warehouse              print the id of the best warehouse to use
  start-warehouse <id>        start a warehouse and wait until RUNNING
  sql <statement> [...]       run statements, print state and rows
  query <statement>           run one statement, print rows as TSV (no header)
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

HOST = os.environ.get("DBT_HOST", "")
TOKEN = os.environ.get("DBT_TOKEN", "")


def _require_credentials():
    if not HOST or not TOKEN:
        sys.exit("DBT_HOST and DBT_TOKEN must be set")


def call(path, payload=None, method=None):
    """Call the Databricks REST API and return the decoded JSON body."""
    req = urllib.request.Request(
        f"https://{HOST}{path}",
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
        method=method or ("POST" if payload is not None else "GET"),
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:500]
        sys.exit(f"HTTP {exc.code} on {path}: {body}")


def warehouses():
    return call("/api/2.0/sql/warehouses").get("warehouses") or []


def pick_warehouse():
    """Prefer a running warehouse, then a serverless one, then anything."""
    whs = warehouses()
    if not whs:
        sys.exit("no SQL warehouse in this workspace: create one, or set DBX_WAREHOUSE")
    for predicate in (
        lambda w: w.get("state") == "RUNNING",
        lambda w: w.get("enable_serverless_compute"),
        lambda w: True,
    ):
        for w in whs:
            if predicate(w):
                return w["id"]


def start_warehouse(wid, timeout=600):
    """Start a warehouse if needed and wait for RUNNING."""
    state = call(f"/api/2.0/sql/warehouses/{wid}").get("state")
    if state != "RUNNING":
        call(f"/api/2.0/sql/warehouses/{wid}/start", payload={})
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = call(f"/api/2.0/sql/warehouses/{wid}").get("state")
        if state == "RUNNING":
            return state
        if state in ("STOPPED", "DELETED"):
            # a start request can race with a shutdown: ask again
            call(f"/api/2.0/sql/warehouses/{wid}/start", payload={})
        time.sleep(5)
    sys.exit(f"warehouse {wid} did not reach RUNNING (last state: {state})")


def run_sql(statement, warehouse=None, poll_seconds=900):
    """Execute one SQL statement and return the final response payload."""
    wid = warehouse or os.environ.get("DBX_WAREHOUSE") or pick_warehouse()
    d = call(
        "/api/2.0/sql/statements",
        {
            "warehouse_id": wid,
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
        },
    )
    sid = d.get("statement_id")
    deadline = time.time() + poll_seconds
    while d.get("status", {}).get("state") in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(5)
        d = call(f"/api/2.0/sql/statements/{sid}")
    return d


def rows_of(resp):
    return (resp.get("result") or {}).get("data_array") or []


def error_of(resp):
    return ((resp.get("status") or {}).get("error") or {}).get("message")


def main(argv):
    if not argv:
        sys.exit(__doc__)
    cmd, args = argv[0], argv[1:]
    _require_credentials()

    if cmd == "whoami":
        me = call("/api/2.0/preview/scim/v2/Me")
        print(me.get("userName"), "| id:", me.get("id"), "| active:", me.get("active"))
    elif cmd == "metastore":
        d = call("/api/2.1/unity-catalog/current-metastore-assignment")
        print(json.dumps({k: d.get(k) for k in ("metastore_id", "default_catalog_name", "workspace_id")}))
    elif cmd == "catalogs":
        for c in call("/api/2.1/unity-catalog/catalogs").get("catalogs") or []:
            print(f"{c['name']}\t{c.get('catalog_type')}\t{c.get('owner')}")
    elif cmd == "warehouses":
        for w in warehouses():
            print(f"{w['id']}\t{w['name']}\t{w.get('state')}\tserverless={w.get('enable_serverless_compute')}")
    elif cmd == "pick-warehouse":
        print(pick_warehouse())
    elif cmd == "start-warehouse":
        print(start_warehouse(args[0] if args else pick_warehouse()))
    elif cmd == "sql":
        failed = 0
        for stmt in args:
            resp = run_sql(stmt)
            state = (resp.get("status") or {}).get("state")
            print(f"[{state}] {stmt.replace(chr(10), ' ')[:90]}")
            err = error_of(resp)
            if err:
                failed += 1
                print("    error:", err[:400])
            for row in rows_of(resp)[:20]:
                print("    ", row)
        return 1 if failed else 0
    elif cmd == "query":
        resp = run_sql(args[0])
        err = error_of(resp)
        if err:
            print("error:", err[:400], file=sys.stderr)
            return 1
        for row in rows_of(resp):
            print("\t".join("" if v is None else str(v) for v in row))
    else:
        sys.exit(f"unknown command: {cmd}\n{__doc__}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
