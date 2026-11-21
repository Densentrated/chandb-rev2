#!/usr/bin/env bash
# Step 1a: build a deterministic lake at .verify/lake from scratch.
#
# From scratch is the point. The gold watermark in transformGold compares a
# bronze partition's directory mtime against the existing Parquet, so a lake
# left over from a previous run would be skipped rather than rebuilt and the
# next verify would quietly assert against stale data. rm -rf is cheaper to
# reason about than any incremental scheme.
#
# Two stages:
#   1. seedlake expands the committed fixtures into bronze, stamping every
#      timestamp relative to now (see scripts/verify/seedlake/main.go).
#   2. `pipeline -rebuild` runs the REAL gold transform over that bronze, so
#      the Parquet is schema-faithful by construction instead of by hand.
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

if ! command -v go >/dev/null 2>&1; then
  infra_fail seed.toolchain "go is not on PATH" \
    "install Go 1.24+ (https://go.dev/dl/) and re-run" || exit $?
fi

rm -rf "$LAKE_DIR_VERIFY"
mkdir -p "$LAKE_DIR_VERIFY"

say "seeding bronze from testdata/lake"
if ! go run ./scripts/verify/seedlake \
      -testdata testdata/lake -lake "$LAKE_DIR_VERIFY" \
      >"${RUN_DIR}/seed.txt" 2>&1; then
  cat "${RUN_DIR}/seed.txt" >&2
  record seed.bronze FAIL "seedlake failed to expand testdata/lake" seed.txt
  exit "$EXIT_FAIL"
fi
record seed.bronze PASS "$(tail -n 1 "${RUN_DIR}/seed.txt")" seed.txt

say "running the real gold transform (pipeline -rebuild)"
# -rebuild skips ingestAll, so this stays offline, and exits non-zero if gold
# came out empty — which is the signal that the fixture and gold.go disagree.
if ! LAKE_DIR="$LAKE_DIR_VERIFY" go run ./cmd/pipeline -rebuild \
      >>"${RUN_DIR}/seed.txt" 2>&1; then
  tail -n 40 "${RUN_DIR}/seed.txt" >&2
  record seed.gold FAIL \
    "pipeline -rebuild produced no gold; fixture or gold.go schema drift" seed.txt
  exit "$EXIT_FAIL"
fi

parquet=$(find "${LAKE_DIR_VERIFY}/gold" -name '*.parquet' | wc -l)
datasets=$(find "${LAKE_DIR_VERIFY}/gold" -mindepth 2 -maxdepth 2 -type d | wc -l)
record seed.gold PASS \
  "${parquet} parquet file(s) across ${datasets} dataset(s)" seed.txt
say "gold: ${parquet} parquet file(s), ${datasets} dataset(s)"
