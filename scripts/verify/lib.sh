#!/usr/bin/env bash
# Shared plumbing for the verify harness. Sourced, never executed.
#
# Two things live here because every tier needs them to agree:
#
#   - REPO_ROOT, so a script works from any cwd. Several of these shell out to
#     `go`, which resolves ./cmd/... relative to the process cwd.
#   - record(), the one way a check result reaches the summary. Results are a
#     TSV, not JSON, so shell and node can both append without either one
#     needing a JSON escaper. The summary is the only reader.

# Exit-code contract, shared by every script in this directory.
#   0 PASS       everything the harness checked is good
#   1 FAIL       the code is broken; there is something to fix
#   2 INFRA_FAIL blocked by the environment; fixing app code will not help,
#                so a caller must NOT treat this as a reason to loop
readonly EXIT_PASS=0
readonly EXIT_FAIL=1
readonly EXIT_INFRA=2

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly REPO_ROOT

readonly PORTAL_PORT="${PORTAL_PORT:-8099}"
readonly PORTAL_BASE="http://127.0.0.1:${PORTAL_PORT}"
readonly VERIFY_DIR="${REPO_ROOT}/.verify"
readonly LAKE_DIR_VERIFY="${VERIFY_DIR}/lake"
readonly PIDFILE="${VERIFY_DIR}/portal.pid"

# RUN_DIR is passed down from run.sh so every tier writes into the same
# evidence directory. Standalone invocations get their own.
if [[ -z "${RUN_DIR:-}" ]]; then
  RUN_DIR="${VERIFY_DIR}/run-$(date -u +%Y%m%dT%H%M%SZ)"
fi
export RUN_DIR
export CHECKS_TSV="${RUN_DIR}/checks.tsv"

mkdir -p "$RUN_DIR"

# record <id> <PASS|FAIL|WARN|SKIP|INFRA> <detail> <artifact>
#
# Tabs and newlines are stripped rather than escaped: a check detail is one
# line by construction, and silently corrupting the column layout is worse
# than losing whitespace.
record() {
  local id="$1" status="$2" detail="${3:-}" artifact="${4:--}"
  printf '%s\t%s\t%s\t%s\n' \
    "$id" "$status" \
    "$(printf '%s' "$detail" | tr '\t\n' '  ')" \
    "$artifact" >>"$CHECKS_TSV"
}

say() { printf '[verify] %s\n' "$*" >&2; }

# infra_fail prints the remediation with the failure. An INFRA_FAIL that does
# not tell you the command to run is just a FAIL you are not allowed to fix.
infra_fail() {
  local id="$1" detail="$2" remedy="$3"
  record "$id" INFRA "${detail} — remedy: ${remedy}" -
  say "INFRA_FAIL: ${detail}"
  say "  run: ${remedy}"
  return "$EXIT_INFRA"
}
