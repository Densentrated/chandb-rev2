#!/usr/bin/env bash
# Step 1b: build and start the portal against the seeded lake.
#
# Port 8099 rather than the 8080 default: a developer's own `go run ./cmd/portal`
# is the most likely thing to be running while this harness does, and silently
# verifying against someone else's process — with a different lake — would
# produce results that look real and are not. An occupied 8099 is therefore an
# INFRA_FAIL, never a quiet fallback to another port.
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

if [[ ! -d "${LAKE_DIR_VERIFY}/gold" ]]; then
  record portal.lake FAIL "no gold tree at ${LAKE_DIR_VERIFY}/gold; run seed-lake.sh first" -
  exit "$EXIT_FAIL"
fi

port_busy() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltnH "sport = :${PORTAL_PORT}" 2>/dev/null | grep -q .
  else
    # No ss: a successful connect is good enough evidence something is there.
    (exec 3<>"/dev/tcp/127.0.0.1/${PORTAL_PORT}") 2>/dev/null
  fi
}

if port_busy; then
  infra_fail portal.port "port ${PORTAL_PORT} is already in use" \
    "scripts/verify/stop-portal.sh, or: fuser -k ${PORTAL_PORT}/tcp" || exit $?
fi

mkdir -p "${VERIFY_DIR}/bin"
say "building the portal"
if ! go build -o "${VERIFY_DIR}/bin/portal" ./cmd/portal \
      >"${RUN_DIR}/portal-build.txt" 2>&1; then
  cat "${RUN_DIR}/portal-build.txt" >&2
  record portal.build FAIL "go build ./cmd/portal failed" portal-build.txt
  exit "$EXIT_FAIL"
fi
record portal.build PASS "go build ./cmd/portal" portal-build.txt

LAKE_DIR="$LAKE_DIR_VERIFY" PORTAL_ADDR=":${PORTAL_PORT}" \
  "${VERIFY_DIR}/bin/portal" >"${RUN_DIR}/portal.log" 2>&1 &
echo $! >"$PIDFILE"
say "portal pid $(cat "$PIDFILE"), log ${RUN_DIR}/portal.log"

# Bounded poll. An unbounded wait on a process that died at startup is the
# classic way a verification loop hangs instead of reporting.
deadline=$((SECONDS + 20))
until curl -fsS --max-time 2 "${PORTAL_BASE}/healthz" >/dev/null 2>&1; do
  if ! kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    tail -n 20 "${RUN_DIR}/portal.log" >&2
    record portal.start FAIL "portal exited during startup" portal.log
    exit "$EXIT_FAIL"
  fi
  if (( SECONDS >= deadline )); then
    tail -n 20 "${RUN_DIR}/portal.log" >&2
    record portal.start FAIL "portal did not answer /healthz within 20s" portal.log
    exit "$EXIT_FAIL"
  fi
  sleep 0.3
done

record portal.start PASS "listening on ${PORTAL_BASE} over ${LAKE_DIR_VERIFY}" portal.log
say "portal ready at ${PORTAL_BASE}"
