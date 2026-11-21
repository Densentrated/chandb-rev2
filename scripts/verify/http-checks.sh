#!/usr/bin/env bash
# Step 2, tier 1: the Go suite, then real HTTP against the running portal.
#
# The Go tests prove the handlers behave when called directly. These checks
# prove the same thing end to end, through net/http's own path cleaning, header
# writing and Range machinery — which is the layer duckdb-wasm actually talks
# to, and the layer a unit test with httptest.NewRequest skips.
set -euo pipefail

source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"
cd "$REPO_ROOT"

HTTP_LOG="${RUN_DIR}/http.jsonl"
: >"$HTTP_LOG"
rc="$EXIT_PASS"

fail() { rc="$EXIT_FAIL"; }

# log_http appends one exchange to the evidence file. Values are ours (URLs,
# status codes, header fragments), so a hand-built JSON line is safe here.
log_http() {
  printf '{"check":"%s","method":"%s","url":"%s","status":"%s","note":"%s"}\n' \
    "$1" "$2" "$3" "$4" "$5" >>"$HTTP_LOG"
}

# --------------------------------------------------------------- Go CLI tier

say "go vet ./..."
if go vet ./... >"${RUN_DIR}/go-vet.txt" 2>&1; then
  record go.vet PASS "go vet ./..." go-vet.txt
else
  record go.vet FAIL "go vet reported problems" go-vet.txt; fail
fi

say "go build ./..."
if go build ./... >"${RUN_DIR}/go-build.txt" 2>&1; then
  record go.build PASS "go build ./..." go-build.txt
else
  record go.build FAIL "go build failed" go-build.txt; fail
fi

say "go test -race ./..."
if go test -race ./... >"${RUN_DIR}/go-test.txt" 2>&1; then
  record go.test PASS "go test -race ./..." go-test.txt
else
  record go.test FAIL "go test -race reported failures" go-test.txt; fail
fi

# golangci-lint runs in CI but is not part of the Go toolchain, so a local
# machine without it is normal. Missing is a SKIP, not a failure: pretending
# the lint passed would be worse, and failing on it would block every run on a
# fresh checkout.
if command -v golangci-lint >/dev/null 2>&1; then
  say "golangci-lint run"
  if golangci-lint run >"${RUN_DIR}/lint.txt" 2>&1; then
    record go.lint PASS "golangci-lint run" lint.txt
  else
    record go.lint FAIL "golangci-lint reported problems" lint.txt; fail
  fi
else
  printf 'golangci-lint is not installed locally; CI runs it via golangci/golangci-lint-action.\ninstall: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest\n' \
    >"${RUN_DIR}/lint.txt"
  record go.lint SKIP "golangci-lint not installed; CI still runs it" lint.txt
fi

# ------------------------------------------------------------ live HTTP tier

code_of() { curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$@"; }

# /healthz
status=$(code_of "${PORTAL_BASE}/healthz")
log_http http.healthz GET /healthz "$status" ""
if [[ "$status" == 200 ]]; then
  record http.healthz PASS "/healthz is 200" http.json
else
  record http.healthz FAIL "/healthz returned ${status}, want 200" http.json; fail
fi

# /api/datasets — names and non-empty file lists.
#
# What goldTables in cmd/pipeline/gold.go and broadbandTables in broadband.go
# produce. Pinning the list is deliberate: a dataset silently disappearing is
# exactly the regression that leaves the portal up and every page empty.
#
# Add new gold tables here. An unlisted table is served but unguarded — which
# is how the broadband tables shipped green while being entirely untested.
WANT_DATASETS=(
  bart_stop_events
  mbta_stop_events
  mbta_vehicle_positions
  septa_stop_events
  septa_vehicle_positions
  septarail_stop_events
  septarail_vehicle_positions
  broadband_tract_measured
  broadband_tract_coverage
)
datasets_body="${RUN_DIR}/api-datasets.json"
status=$(curl -s -o "$datasets_body" -w '%{http_code}' --max-time 10 \
  "${PORTAL_BASE}/api/datasets")
log_http http.datasets GET /api/datasets "$status" ""

missing=()
for name in "${WANT_DATASETS[@]}"; do
  # Both halves matter: the dataset has to exist AND list at least one file.
  # A present name with "files":[] registers a view over nothing.
  grep -q "\"name\":\"${name}\",\"files\":\[\"" "$datasets_body" || missing+=("$name")
done
if [[ "$status" == 200 && ${#missing[@]} -eq 0 ]]; then
  record http.datasets PASS "${#WANT_DATASETS[@]} datasets, all with files" api-datasets.json
else
  record http.datasets FAIL \
    "status ${status}; missing or empty: ${missing[*]:-none}" api-datasets.json
  fail
fi

# ETag revalidation. The catalog is re-read on a timer; without a working 304
# every page load re-downloads it, and a mismatched ETag means the browser
# never notices a new partition.
etag=$(curl -s -D - -o /dev/null --max-time 10 "${PORTAL_BASE}/api/datasets" \
  | tr -d '\r' | awk 'tolower($1)=="etag:"{print $2}')
status=$(code_of -H "If-None-Match: ${etag}" "${PORTAL_BASE}/api/datasets")
log_http http.etag GET /api/datasets "$status" "If-None-Match ${etag}"
if [[ "$status" == 304 ]]; then
  record http.etag PASS "If-None-Match ${etag} → 304" http.json
else
  record http.etag FAIL "revalidation returned ${status}, want 304 (etag ${etag:-none})" http.json
  fail
fi

# Range requests. This one is load-bearing: duckdb-wasm registers every Parquet
# with DuckDBDataProtocol.HTTP, which means it only ever reads the row groups
# and column chunks a query touches. A server that answers 200-with-everything
# instead of 206 does not fail loudly — it drags the whole archive into the tab
# and the page looks merely slow.
parquet=$(grep -o '"/data/[^"]*\.parquet"' "$datasets_body" | head -n 1 | tr -d '"')
if [[ -z "$parquet" ]]; then
  record http.range FAIL "no parquet URL in /api/datasets to range-request" api-datasets.json
  fail
else
  total=$(curl -s -I --max-time 10 "${PORTAL_BASE}${parquet}" \
    | tr -d '\r' | awk 'tolower($1)=="content-length:"{print $2}')
  headers="${RUN_DIR}/range-headers.txt"
  body="${RUN_DIR}/range-body.bin"
  status=$(curl -s -D "$headers" -o "$body" -w '%{http_code}' --max-time 10 \
    -H 'Range: bytes=0-99' "${PORTAL_BASE}${parquet}")
  got_bytes=$(wc -c <"$body")
  content_range=$(tr -d '\r' <"$headers" \
    | awk 'tolower($1)=="content-range:"{print $2, $3}' | tr -d ' ')
  want_range="bytes0-99/${total}"
  log_http http.range GET "$parquet" "$status" "${content_range}"

  if [[ "$status" == 206 && "$got_bytes" == 100 && "$content_range" == "$want_range" ]]; then
    record http.range PASS \
      "206, 100 bytes, Content-Range bytes 0-99/${total}" range-headers.txt
  else
    record http.range FAIL \
      "status ${status} (want 206), ${got_bytes} bytes (want 100), Content-Range ${content_range:-none} (want ${want_range})" \
      range-headers.txt
    fail
  fi
fi

# Path traversal. http.Dir rejects "..", and dataServer additionally resolves
# symlinks before serving, so neither raw/, bronze/ nor the secrets mount is
# reachable through /data. net/http cleans the path first and answers with a
# redirect, so follow it: what matters is where the request ENDS UP.
traversal_ok=1
for attempt in '/data/../../etc/passwd' '/data/mbta/../../bronze/mbta/trip_updates' '/data/..%2f..%2fetc%2fpasswd'; do
  status=$(curl -s -L -o /dev/null -w '%{http_code}' --max-time 10 --path-as-is \
    "${PORTAL_BASE}${attempt}")
  log_http http.traversal GET "$attempt" "$status" ""
  [[ "$status" == 404 ]] || { traversal_ok=0; say "traversal ${attempt} → ${status}"; }
done
if (( traversal_ok )); then
  record http.traversal PASS "3 traversal attempts all 404" http.json
else
  record http.traversal FAIL "a traversal attempt did not end in 404" http.json; fail
fi

# Cache headers. The hashed URL and the unhashed one serve identical bytes on
# purpose; only the hashed one may be cached, because only its URL changes when
# the bytes do. Getting this backwards reintroduces the stale-module hang that
# buildAssets exists to prevent: a fresh page paired with an old chandb.js is a
# parse-time SyntaxError, so nothing runs and nothing is logged.
#
# The indirection is two levels deep. A page loads its own hashed page module
# (/index.<hash>.js), and that module is what imports the hashed shared runtime
# (/chandb.<hash>.js). Both must be immutable.
#
# `|| true` on each grep is load-bearing: under `set -euo pipefail` a grep that
# matches nothing fails the whole pipeline and kills the script, so the "no
# hashed URL found" branch below would never get to report anything.
cache_of() {
  curl -s -D - -o /dev/null --max-time 10 "$1" | tr -d '\r' \
    | awk 'tolower($1)=="cache-control:"{$1=""; print substr($0,2)}'
}
hashed_page=$(curl -s --max-time 10 "${PORTAL_BASE}/" \
  | grep -o '/index\.[0-9a-f]\{12\}\.js' | head -n 1 || true)
hashed_shared=""
if [[ -n "$hashed_page" ]]; then
  hashed_shared=$(curl -s --max-time 10 "${PORTAL_BASE}${hashed_page}" \
    | grep -o '/chandb\.[0-9a-f]\{12\}\.js' | head -n 1 || true)
fi

cache_ok=1
if [[ -z "$hashed_page" ]]; then
  say "no hashed page-module URL found in /"
  cache_ok=0
elif [[ -z "$hashed_shared" ]]; then
  say "no hashed shared-runtime URL found in ${hashed_page}"
  cache_ok=0
else
  for hashed in "$hashed_page" "$hashed_shared"; do
    cc=$(cache_of "${PORTAL_BASE}${hashed}")
    log_http http.cache GET "$hashed" 200 "$cc"
    [[ "$cc" == *immutable* ]] || { cache_ok=0; say "${hashed} Cache-Control=${cc}"; }
  done
fi
for path in /chandb.js /index.js /board.js /maps.js /broadband.js \
            / /board /maps /broadband; do
  cc=$(cache_of "${PORTAL_BASE}${path}")
  log_http http.cache GET "$path" 200 "$cc"
  [[ "$cc" == *no-cache* ]] || { cache_ok=0; say "${path} Cache-Control=${cc}"; }
done
if (( cache_ok )); then
  record http.cache PASS \
    "${hashed_page} + ${hashed_shared} immutable; plain module and page URLs no-cache" \
    http.json
else
  record http.cache FAIL "a cache header is wrong; see http.json" http.json; fail
fi

# Every page asset must be 2xx. A 404 on portal.css is invisible in a headless
# screenshot diff but means the whole page renders unstyled.
assets_ok=1
page_assets=(/ /board /maps /broadband /portal.css
             /chandb.js /index.js /board.js /maps.js /broadband.js
             /api/datasets /healthz)
for path in "${page_assets[@]}"; do
  status=$(code_of "${PORTAL_BASE}${path}")
  log_http http.assets GET "$path" "$status" ""
  [[ "$status" == 2* ]] || { assets_ok=0; say "${path} → ${status}"; }
done
if (( assets_ok )); then
  record http.assets PASS "${#page_assets[@]} page assets all 2xx" http.json
else
  record http.assets FAIL "a page asset returned non-2xx; see http.json" http.json; fail
fi

# Wrap the exchange log into the http.json the summary points at.
{ printf '[\n'; paste -sd, - <"$HTTP_LOG"; printf ']\n'; } >"${RUN_DIR}/http.json"
rm -f "$HTTP_LOG"

exit "$rc"
