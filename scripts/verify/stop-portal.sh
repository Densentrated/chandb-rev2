#!/usr/bin/env bash
# Tear down whatever start-portal.sh left running.
#
# Idempotent on purpose: run.sh calls this from a trap, so it fires on success,
# on failure, and on Ctrl-C — including the case where the portal never
# started. Exiting non-zero from a trap would mask the real exit code.
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

[[ -f "$PIDFILE" ]] || exit 0

pid="$(cat "$PIDFILE" 2>/dev/null || true)"
rm -f "$PIDFILE"
[[ -n "$pid" ]] || exit 0

kill -0 "$pid" 2>/dev/null || exit 0

kill "$pid" 2>/dev/null || true
for _ in $(seq 1 20); do
  kill -0 "$pid" 2>/dev/null || { say "portal ${pid} stopped"; exit 0; }
  sleep 0.25
done

# Graceful shutdown has a 10s budget in cmd/portal; past that it is wedged.
kill -9 "$pid" 2>/dev/null || true
say "portal ${pid} killed"
exit 0
