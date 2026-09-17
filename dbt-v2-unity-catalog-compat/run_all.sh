#!/usr/bin/env bash
# Replay the whole protocol end to end.
#
#   ./run_all.sh                       offline phase only (no credentials needed)
#   ./run_all.sh --live                bootstrap + offline + live + UC assertions
#   ./run_all.sh --live --teardown     ... then drop everything it created
#
# Credentials: DBT_HOST / DBT_TOKEN in the environment, or --host / --token,
# or an env.local already written by bootstrap.sh.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIVE=0; TEARDOWN=0; BOOTSTRAP_ARGS=(); DBT_PIN="${DBT_PIN:-2.0.4}"

while [ $# -gt 0 ]; do
  case "$1" in
    --live)      LIVE=1; shift ;;
    --teardown)  TEARDOWN=1; shift ;;
    --version)   DBT_PIN="$2"; shift 2 ;;
    --host)      export DBT_HOST="$2"; BOOTSTRAP_ARGS+=(--host "$2"); shift 2 ;;
    --token)     export DBT_TOKEN="$2"; BOOTSTRAP_ARGS+=(--token "$2"); shift 2 ;;
    *)           BOOTSTRAP_ARGS+=("$1"); shift ;;   # forwarded to bootstrap.sh
  esac
done

banner(){ printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
RC=0

banner "Phase N1+N2 — offline (dbt $DBT_PIN)"
"$HERE/run_offline_checks.sh" "$DBT_PIN" || RC=1

if [ "$LIVE" -eq 1 ]; then
  banner "Bootstrap — discover and prepare the workspace"
  "$HERE/bootstrap.sh" "${BOOTSTRAP_ARGS[@]}" || exit 1
  # shellcheck disable=SC1091
  . "$HERE/env.local"

  banner "Phase N3 — live execution"
  "$HERE/run_live_checks.sh" || RC=1

  banner "Phase N3 — assertions read back from Unity Catalog"
  "$HERE/verify_uc_state.sh" || RC=1

  if [ "$TEARDOWN" -eq 1 ]; then
    banner "Teardown"
    "$HERE/teardown.sh" --yes --include-snapshots --drop-cross-catalog || RC=1
  else
    printf '\nObjects were left in place. Remove them with:\n  source %s/env.local && %s/teardown.sh --yes --include-snapshots\n' "$HERE" "$HERE"
  fi
fi

banner "Overall: $([ "$RC" -eq 0 ] && echo 'all phases passed' || echo 'at least one check failed')"
exit "$RC"
