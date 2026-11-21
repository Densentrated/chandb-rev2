#!/usr/bin/env bash
# The verify harness: seed → start → static → http → browser → summary.
#
# One entry point so there is exactly one order of operations and exactly one
# teardown path. Every tier appends to the same checks.tsv and the summary
# derives the exit code from it, so what a human reads and what a caller
# branches on can never disagree.
#
#   0 PASS        nothing to fix
#   1 FAIL        the code is broken
#   2 INFRA_FAIL  blocked by the environment; do NOT loop on this
#
# A failing tier does not stop the run. The value of the harness is a complete
# picture in one pass: stopping at the first red check means the next
# invocation discovers the second problem and the one after that the third.
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

say "run directory: ${RUN_DIR}"
: >"$CHECKS_TSV"

# Teardown on every exit path, including Ctrl-C. A crashed run still holding
# :8099 turns the NEXT run into a spurious INFRA_FAIL.
cleanup() { "${REPO_ROOT}/scripts/verify/stop-portal.sh" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

worst=0

# promote folds a tier's exit code into the run's. INFRA_FAIL outranks FAIL:
# being blocked is not evidence about the code, so it must never be reported
# as a code failure.
promote() {
  local rc="${1:-0}"
  if (( rc == EXIT_INFRA )); then
    worst="$EXIT_INFRA"
  elif (( rc != 0 && worst != EXIT_INFRA )); then
    worst="$EXIT_FAIL"
  fi
  return 0
}

# run_tier <name> <command...> — never aborts the run on a non-zero exit.
#
# A tier that dies partway (an unguarded `grep` with no match under `set -e` is
# the classic) leaves its remaining checks simply unrecorded. Without the crash
# row below, summary.md then reports "0 failed" next to a FAIL verdict, which
# reads as a harness bug and tells you nothing about where it stopped. Only
# synthesise the row when the tier recorded no failure of its own, so a tier
# that cleanly reports its own FAIL is not double-counted.
run_tier() {
  local name="$1"; shift
  local before after rc=0
  before=$(wc -l <"$CHECKS_TSV")
  "$@" || rc=$?
  after=$(wc -l <"$CHECKS_TSV")

  if (( rc != 0 && rc != EXIT_INFRA )) \
     && ! tail -n "+$((before + 1))" "$CHECKS_TSV" | cut -f2 | grep -qx FAIL; then
    record "${name}.crash" FAIL \
      "tier exited ${rc} after recording $((after - before)) check(s); it stopped early" \
      "${name}.txt"
  fi

  promote "$rc"
  say "${name} → exit ${rc}"
  return 0
}

write_summary() {
  ln -sfn "$RUN_DIR" "${VERIFY_DIR}/latest"
  local rc=0
  RUN_DIR="$RUN_DIR" CHECKS_TSV="$CHECKS_TSV" node scripts/verify/summary.mjs || rc=$?
  promote "$rc"
}

# ------------------------------------------------------------------ preflight
for tool in go node npx; do
  command -v "$tool" >/dev/null 2>&1 && continue
  if [[ "$tool" == go ]]; then
    remedy="install Go 1.24+ from https://go.dev/dl/"
  else
    remedy="install Node.js 20+ from https://nodejs.org/"
  fi
  infra_fail "preflight.${tool}" "${tool} is not on PATH" "$remedy" || true
  worst="$EXIT_INFRA"
done
if (( worst == EXIT_INFRA )); then
  write_summary
  say "INFRA_FAIL — see ${RUN_DIR}/summary.md"
  exit "$EXIT_INFRA"
fi

# Docker is not used by this harness, but CI builds the image and the deploy
# path is compose, so its absence is worth stating rather than discovering.
if command -v docker >/dev/null 2>&1; then
  record preflight.docker PASS "docker present (unused here; CI builds the image)" -
else
  record preflight.docker WARN \
    "docker is not installed — unused by this harness, but CI's image job and deploy/ need it" -
fi

# ------------------------------------------------------ step 1: environment up
say "step 1/4 — seeding .verify/lake and starting the portal"
run_tier "seed-lake" scripts/verify/seed-lake.sh
if (( worst == EXIT_FAIL )); then
  say "seeding failed; the portal would serve nothing, so the live tiers are skipped"
else
  run_tier "start-portal" scripts/verify/start-portal.sh
fi

portal_up=0
curl -fsS --max-time 2 "${PORTAL_BASE}/healthz" >/dev/null 2>&1 && portal_up=1

# ------------------------------------------------ step 2: exercise real behaviour
say "step 2/4 — static checks"
run_tier "static-checks" env RUN_DIR="$RUN_DIR" CHECKS_TSV="$CHECKS_TSV" \
  node scripts/verify/static-checks.mjs

if (( portal_up )); then
  say "step 2/4 — go suite and live HTTP checks"
  run_tier "http-checks" scripts/verify/http-checks.sh

  say "step 2/4 — browser tier (10 minute cap)"
  # `timeout` is the belt to drive.mjs's own braces: if the process wedges
  # below the level its internal deadline can observe, this still returns.
  browser_rc=0
  env RUN_DIR="$RUN_DIR" CHECKS_TSV="$CHECKS_TSV" PORTAL_BASE="$PORTAL_BASE" \
    timeout --signal=TERM --kill-after=30s 10m node scripts/verify/drive.mjs \
    || browser_rc=$?
  if (( browser_rc == 124 || browser_rc == 137 )); then
    infra_fail browser.budget "the browser tier exceeded its 10-minute cap" \
      "curl -I https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm  # 34MB bundle, CDN-bound" \
      || true
    worst="$EXIT_INFRA"
  else
    promote "$browser_rc"
  fi
  say "drive.mjs → exit ${browser_rc}"
else
  record http.live SKIP "the portal never came up; live HTTP checks not run" portal.log
  record browser.live SKIP "the portal never came up; browser tier not run" portal.log
fi

# -------------------------------------------------------- step 3 + 4: evidence
say "step 3/4 — tearing down and writing the summary"
cleanup
trap - EXIT INT TERM

write_summary

case "$worst" in
  0) verdict=PASS ;;
  1) verdict=FAIL ;;
  *) verdict=INFRA_FAIL ;;
esac
say "step 4/4 — verdict: ${verdict} (exit ${worst}) — ${RUN_DIR}/summary.md"
exit "$worst"
