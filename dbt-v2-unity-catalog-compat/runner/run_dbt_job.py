#!/usr/bin/env python3
"""Run a dbt v2 job in-process through dbtRunner, against Databricks / Unity Catalog.

Why dbtRunner rather than shelling out to the CLI: the engine stays in the current
process, so the caller gets the run artifacts as Python objects (RunResultsArtifact,
Manifest, FreshnessResultsArtifact) instead of parsing stdout, and one runner
instance is reused across commands.

Two ways to point dbt at a warehouse:

1. an existing profiles.yml — pass --profiles-dir;
2. no profiles.yml at all — pass --generate-profile and let this script write one
   from the environment (DBT_HOST, DBT_HTTP_PATH, DBT_TOKEN, DBT_CATALOG,
   DBT_SCHEMA). That is the useful mode on Databricks, where the token comes from
   a secret scope and nothing secret should be written into the repo.

CLI:
    python run_dbt_job.py --project-dir ../fixture --profiles-dir ../fixture \\
        --target live --command "seed" --command "build --exclude st_customers"

    python run_dbt_job.py --project-dir ../fixture --generate-profile \\
        --command build --select "tag:daily" --full-refresh

Library:
    from run_dbt_job import run_dbt_job
    report = run_dbt_job(["build"], project_dir="...", generate_profile=True)
    report.raise_for_status()
"""
from __future__ import annotations

import argparse
import os
import shlex
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from dbt.runner import dbtRunner, dbtRunnerResult

PROFILE_TEMPLATE = """\
{profile_name}:
  target: {target}
  outputs:
    {target}:
      type: databricks
      host: {host}
      http_path: {http_path}
      token: {token}
      catalog: {catalog}
      schema: {schema}
      threads: {threads}
"""


@dataclass
class CommandOutcome:
    """What one dbt command did, flattened out of dbtRunnerResult."""

    command: List[str]
    success: bool
    exit_code: Optional[int]
    exception: Optional[str] = None
    nodes: List[Dict[str, Any]] = field(default_factory=list)
    elapsed_time: float = 0.0

    # dbt node statuses: success/pass are good, skipped is neutral, warn is a
    # non-blocking warning (the command still exits 0 or 2), the rest are failures.
    OK_STATUSES = ("success", "pass", "skipped", "warn", "listed")

    @property
    def failed_nodes(self) -> List[Dict[str, Any]]:
        return [n for n in self.nodes if n["status"] not in self.OK_STATUSES]

    @property
    def warned_nodes(self) -> List[Dict[str, Any]]:
        return [n for n in self.nodes if n["status"] == "warn"]


@dataclass
class JobReport:
    """Aggregate of every command in the job."""

    outcomes: List[CommandOutcome] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(o.success for o in self.outcomes)

    @property
    def exit_code(self) -> int:
        # Worst exit code wins: 0 ok, 1 failure, 2 completed with elevated warnings.
        codes = [o.exit_code or 0 for o in self.outcomes]
        return max(codes) if codes else 0

    def rows(self) -> List[Dict[str, Any]]:
        """Flat rows, ready for a DataFrame or a log line."""
        return [
            {"command": " ".join(o.command), **node}
            for o in self.outcomes
            for node in o.nodes
        ]

    def raise_for_status(self) -> None:
        """Raise when any command failed, so a Databricks Job task fails too."""
        if self.success:
            return
        reasons = [n["unique_id"] for o in self.outcomes for n in o.failed_nodes]
        reasons += [o.exception for o in self.outcomes if o.exception]
        detail = "; ".join(reasons[:10]) or "see the dbt logs"
        raise RuntimeError(f"dbt job failed ({detail})")


def _extract_nodes(result: Any) -> List[Dict[str, Any]]:
    """Normalise a command artifact into rows, whatever the command was."""
    rows: List[Dict[str, Any]] = []
    # `list` returns plain strings; `parse` returns a Manifest with no per-node status.
    if isinstance(result, list):
        return [{"unique_id": str(item), "status": "listed", "execution_time": 0.0,
                 "relation_name": None, "message": None} for item in result]
    for node in getattr(result, "results", []) or []:
        status = getattr(node, "status", None)
        rows.append(
            {
                "unique_id": getattr(node, "unique_id", ""),
                # FreshnessStatus and friends are enums in some commands.
                "status": getattr(status, "value", status),
                "execution_time": round(getattr(node, "execution_time", 0.0) or 0.0, 2),
                "relation_name": getattr(node, "relation_name", None),
                "message": getattr(node, "message", None),
            }
        )
    return rows


def write_profile(
    directory: str,
    profile_name: str = "uc_compat",
    target: str = "live",
    threads: int = 4,
    env: Optional[Dict[str, str]] = None,
) -> str:
    """Write a databricks profiles.yml from the environment and return its directory.

    The token is written to a file with 0600 permissions in a directory the caller
    owns; prefer a temporary directory and delete it when the job is done.
    """
    env = env or os.environ  # type: ignore[assignment]
    missing = [k for k in ("DBT_HOST", "DBT_HTTP_PATH", "DBT_TOKEN") if not env.get(k)]
    if missing:
        raise SystemExit(f"missing environment variables: {', '.join(missing)}")
    path = os.path.join(directory, "profiles.yml")
    content = PROFILE_TEMPLATE.format(
        profile_name=profile_name,
        target=target,
        host=env["DBT_HOST"].replace("https://", "").rstrip("/"),
        http_path=env["DBT_HTTP_PATH"],
        token=env["DBT_TOKEN"],
        catalog=env.get("DBT_CATALOG", "main"),
        schema=env.get("DBT_SCHEMA", "default"),
        threads=threads,
    )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write(content)
    return directory


def run_dbt_job(
    commands: Iterable[str | List[str]],
    project_dir: str,
    profiles_dir: Optional[str] = None,
    target: Optional[str] = None,
    generate_profile: bool = False,
    profile_name: str = "uc_compat",
    threads: Optional[int] = None,
    extra_args: Optional[List[str]] = None,
    stop_on_failure: bool = True,
    fail_on_empty: bool = False,
    log_line=print,
) -> JobReport:
    """Run dbt commands in order through a single dbtRunner instance.

    fail_on_empty: treat a build command that selected nothing as a failure. dbt
    reports an empty selection as a warning and exits 0, so a typo in --select
    otherwise makes a scheduled job succeed without doing anything.
    """
    building = ("run", "build", "seed", "snapshot", "test")
    temp_dir = None
    if generate_profile:
        temp_dir = tempfile.mkdtemp(prefix="dbt-profile-")
        profiles_dir = write_profile(
            temp_dir, profile_name=profile_name, target=target or "live", threads=threads or 4
        )
        target = target or "live"

    runner = dbtRunner()  # reused across commands: the project is parsed once
    report = JobReport()
    try:
        for command in commands:
            argv = shlex.split(command) if isinstance(command, str) else list(command)
            argv += ["--project-dir", project_dir]
            if profiles_dir:
                argv += ["--profiles-dir", profiles_dir]
            if target:
                argv += ["--target", target]
            if threads:
                argv += ["--threads", str(threads)]
            argv += extra_args or []

            log_line(f"→ dbt {' '.join(argv)}")
            result: dbtRunnerResult = runner.invoke(argv)
            outcome = CommandOutcome(
                command=argv,
                success=bool(result.success),
                exit_code=result.exit_code,
                exception=str(result.exception) if result.exception else None,
                nodes=_extract_nodes(result.result),
                elapsed_time=getattr(result.result, "elapsed_time", 0.0) or 0.0,
            )
            report.outcomes.append(outcome)

            if fail_on_empty and outcome.success and not outcome.nodes and argv[0] in building:
                outcome.success = False
                outcome.exit_code = outcome.exit_code or 1
                outcome.exception = f"`dbt {argv[0]}` selected no node (check --select/--exclude)"

            state = "ok" if outcome.success else "FAILED"
            log_line(f"  {state} (exit {outcome.exit_code}, {len(outcome.nodes)} nodes)")
            if outcome.exception:
                log_line(f"  engine error: {outcome.exception}")
            for node in outcome.warned_nodes:
                log_line(f"  ! {node['unique_id']}: warn {node['message'] or ''}")
            for node in outcome.failed_nodes:
                log_line(f"  ✗ {node['unique_id']}: {node['status']} {node['message'] or ''}")
            if not outcome.success and stop_on_failure:
                break
    finally:
        if temp_dir:
            # The generated profile holds a token: remove it even on failure.
            try:
                os.remove(os.path.join(temp_dir, "profiles.yml"))
                os.rmdir(temp_dir)
            except OSError:
                pass
    return report


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project-dir", required=True)
    parser.add_argument("--profiles-dir")
    parser.add_argument("--target")
    parser.add_argument("--profile-name", default="uc_compat",
                        help="profile name written by --generate-profile (must match dbt_project.yml)")
    parser.add_argument("--generate-profile", action="store_true",
                        help="write a temporary databricks profiles.yml from DBT_HOST/DBT_HTTP_PATH/DBT_TOKEN")
    parser.add_argument("--command", action="append", dest="commands",
                        help="dbt command, repeatable (default: build)")
    parser.add_argument("--select")
    parser.add_argument("--exclude")
    parser.add_argument("--full-refresh", action="store_true")
    parser.add_argument("--threads", type=int)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--fail-on-empty", action="store_true",
                        help="fail when a run/build/seed/snapshot/test command selects no node")
    args = parser.parse_args(argv)

    extra: List[str] = []
    if args.select:
        extra += ["--select", args.select]
    if args.exclude:
        extra += ["--exclude", args.exclude]
    if args.full_refresh:
        extra.append("--full-refresh")

    report = run_dbt_job(
        commands=args.commands or ["build"],
        project_dir=args.project_dir,
        profiles_dir=args.profiles_dir,
        target=args.target,
        generate_profile=args.generate_profile,
        profile_name=args.profile_name,
        threads=args.threads,
        extra_args=extra,
        stop_on_failure=not args.continue_on_failure,
        fail_on_empty=args.fail_on_empty,
    )

    print("\n--- summary ---")
    for row in report.rows():
        print(f"  {row['status']:<10} {row['unique_id']:<50} {row['execution_time']}s")
    print(f"job success: {report.success} (exit {report.exit_code})")
    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
