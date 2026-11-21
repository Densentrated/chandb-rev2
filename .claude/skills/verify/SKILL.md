---
name: verify
description: >-
  Run the chandb-rev2 verification harness end to end: seed a deterministic
  lake, start the portal, and drive /, /board and /maps in a real browser while
  capturing screenshots, console output, network failures and a Playwright
  trace. Use when asked to "verify", "run verify", "check the app works", "did
  my change break the frontend", "run the verification loop", "make sure the
  portal still renders", or before handing off any change to cmd/portal,
  cmd/pipeline/gold.go or the page HTML/CSS/JS.
---

# verify

Proves the app actually works, rather than that its unit tests pass. The query
engine in this project runs in the visitor's browser — the server is a static
file host — so almost everything worth breaking breaks *after* the HTML
arrives. A green `go test` says nothing about whether `/maps` drew anything.

```
scripts/verify/run.sh
```

That is the whole invocation. It prints a verdict and writes
`.verify/latest/summary.md`.

## Exit-code contract

| Code | Meaning | What to do |
|---|---|---|
| **0** | PASS | Nothing to fix. |
| **1** | FAIL | **The code is broken.** Fix it and re-run. |
| **2** | INFRA_FAIL | **Blocked, not the code's fault. Do not loop.** Apply the remedy printed in the summary, then re-run **once**. |

Code 2 exists because a jsDelivr hiccup and a broken `boot()` produce the same
visible symptom — a status bar that never stops saying "loading". Treating the
first as a regression sends you hunting a bug that is not there. The harness
distinguishes them by what the page was doing when the clock ran out and
whether the CDN requests themselves failed. **Never re-interpret a 2 as a 1.**

## First-time setup

```
npm install
npx playwright install chromium     # ~150MB, one time
```

`npm run setup` does both. `golangci-lint` is optional locally — the lint check
reports SKIP without it and CI still runs it.

## What it does

### Step 1 — Start the environment cleanly

`scripts/verify/seed-lake.sh` rebuilds `.verify/lake/` from scratch every run
(`rm -rf` first). Incremental seeding does not work here: `transformGold` uses
the bronze partition's directory mtime as a watermark, so a leftover lake gets
*skipped* rather than rebuilt and the next run silently asserts against stale
Parquet.

Two stages, and the split is the important part:

1. `scripts/verify/seedlake/` expands the committed fixtures in `testdata/lake/`
   into **bronze** NDJSON plus the three static GTFS CSVs per agency.
2. `pipeline -rebuild` runs the **real** gold transform over that bronze.

Gold Parquet is never hand-written. A hand-rolled fixture would be a second
copy of the schema in `cmd/pipeline/gold.go`, and the two drift the first time
a column is added — at which point the harness tests its own fixture instead of
the app. `-rebuild` skips `ingestAll` (network) and `prune`, and exits non-zero
if gold came out empty.

**Timestamps are stamped relative to NOW, never frozen.** The fixtures carry
`@+300@` / `@-86400@` tokens that the seeder replaces with `now + offset`. This
is load-bearing: `/board` treats `max(last_seen)` as "now" and shows a
−10min..+60min window, and `/maps` shows the latest 24 hours. A frozen fixture
renders two empty pages and produces false failures.

`scripts/verify/start-portal.sh` builds `./cmd/portal`, runs it with
`LAKE_DIR=.verify/lake PORTAL_ADDR=:8099`, writes `.verify/portal.pid` and
polls `/healthz` for up to 20s. Port 8099 rather than the 8080 default because
a developer's own portal is the most likely thing already running; an occupied
8099 is an INFRA_FAIL, never a quiet fallback. `stop-portal.sh` is idempotent
and `run.sh` calls it from a `trap`, so a crashed run cannot leave a server
behind and poison the next one.

### Step 2 — Exercise real behaviour

Three tiers, all required.

**HTTP/CLI** (`http-checks.sh`): `go vet`, `go build`, `go test -race`,
`golangci-lint` if present. Then real requests against the running portal —
`/healthz`, the `/api/datasets` names and file lists, ETag → 304, a
`Range: bytes=0-99` against a real Parquet returning **206** with exactly 100
bytes and a correct `Content-Range`, traversal attempts ending in 404, and the
cache headers (`immutable` on the hashed module URL, `no-cache` on everything
else).

**Browser** (`drive.mjs`, Playwright, headless chromium, 1440x900): one browser
context for all three pages so the ~34MB DuckDB-WASM bundle downloads once per
run rather than three times.

- `/` — wait for the status bar to leave its busy state, click the
  *"Which trains were late?"* example, assert ≥1 data row, then toggle the
  epoch/human time switch and assert a formatted cell's text changes. The
  column list comes from `chandb.js`'s own `INSTANT_COLS`/`DURATION_COLS` via a
  dynamic import, so the check cannot drift from the page.
- `/board` — assert ≥1 station card, then click an agency filter and a mode
  filter, assert the visible count changes for each, and assert **Clear
  filters** restores the original count.
- `/maps` — read `getImageData` from each of the four canvases inside
  `#map-world`, `#map-boston`, `#map-bay`, `#map-philadelphia`. Two counts per
  canvas: *painted* (anything that is not the `#101014` background) and *heat*
  (vivid pixels from the `hsl(190..330 88% 64%)` palette, which excludes the
  graticule and its labels). Zero painted is a blank canvas and a hard fail.
- Throughout: every `console` message, every `pageerror`, every failed or
  4xx/5xx request.

**Static** (`static-checks.mjs`): `node --check` on `chandb.js`, each page's
module and the harness's own modules; plus a CSS custom-property resolver that
collects every `var(--x)` across `cmd/portal/*.html` and `portal.css` and
asserts each one is defined.

The page modules are found by following each page's `<script type="module"
src=…>`, not by assuming the filenames. A page that lost its script tag, or
whose tag loads nothing, is reported as a failure rather than quietly having
nothing to parse — which is exactly how this check passed vacuously for one run
after the modules were extracted out of the HTML.

### Step 3 — Capture evidence

Everything lands in `.verify/run-<timestamp>/`, with `.verify/latest`
symlinked at it:

```
summary.md          PASS/FAIL/WARN table, one row per check, each naming its artifact
checks.tsv          the raw rows summary.md is built from
screenshots/{index,board,maps}.png
console.json        every console message and pageerror, tagged by page
network.json        every failed or 4xx/5xx request, tagged by page
trace.zip           npx playwright show-trace .verify/latest/trace.zip
http.json           every exchange the CLI tier made against the portal
go-test.txt  go-vet.txt  go-build.txt  lint.txt
static-checks.txt   node --check results and the CSS resolution
seed.txt            seedlake output and the pipeline -rebuild log
portal.log          the portal process output for this run
```

### Step 4 — Failure and unblock rules

**INFRA_FAIL (exit 2) — stop, do not loop.** Each prints its remedy:

| Condition | Remedy printed |
|---|---|
| jsDelivr/DuckDB-WASM unreachable, or the WASM download times out | `curl -I https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/+esm` |
| Port 8099 occupied by something else | `scripts/verify/stop-portal.sh`, or `fuser -k 8099/tcp` |
| Chromium missing | `npx playwright install chromium` (~150MB) |
| `playwright` package missing | `npm install && npx playwright install chromium` |
| Go toolchain missing | install Go 1.24+ from <https://go.dev/dl/> |
| Node missing | install Node.js 20+ |
| Browser tier exceeds its 10-minute cap | check CDN latency, re-run once with a warm cache |

**Hard FAIL (exit 1).** Any of these fails the run:

- any `go vet` / `go build` / `go test -race` / `golangci-lint` failure
- any `pageerror`, or any console message at level `error`
- any undefined CSS custom property
- any page still busy after 90s (and not still downloading the WASM bundle)
- the `/` example query returning 0 rows
- `/board` rendering 0 stations
- any `/maps` canvas fully blank
- a Range request that is not 206 (or not exactly 100 bytes, or with a wrong
  `Content-Range`)
- a traversal attempt that does not end in 404
- any non-2xx on a page asset

**WARN only — does not fail the run:**

- first contentful paint > 5s (CDN-bound, not our code)
- an individual city map with no activity *when the page itself reports "No
  positions published"* — the fixture has no rows for that agency
- console messages at `warning` level
- docker not installed (unused by this harness; CI's image job needs it)

**Escape rule.** At most **2** fix-and-rerun attempts per invocation, and a
**10-minute** wall-clock cap on the browser tier. On exhaustion, STOP and
report the failing checks, their artifact paths, and what was attempted. Never
loop a third time. An INFRA_FAIL does not consume an attempt and does not
justify a retry loop — apply the remedy or report the block.

## Where to look first, per check

`summary.md` prints this for whichever checks failed. The full map:

| Check | Look at |
|---|---|
| `seed.bronze` | `scripts/verify/seedlake/main.go`, `testdata/lake/` — a malformed fixture or `@offset@` token |
| `seed.gold` | `cmd/pipeline/gold.go` (`goldTables`, the declared JSON column lists) against the bronze field names in `testdata/lake/*.ndjson` |
| `static.cssvars` | the `:root` token set in `cmd/portal/portal.css`, then the page's own `<style>` block — every `var(--x)` must resolve to a definition there |
| `static.syntax` | the named module; a parse error means no handler ever runs and the page hangs with nothing logged |
| `index.ready`, `board.ready`, `maps.ready` | `boot()` in `cmd/portal/chandb.js`, then `console.json` for the first error |
| `index.query` returning 0 rows | the seeder's time window, or gold schema drift in `cmd/pipeline/gold.go` |
| `index.timetoggle` | the `#tmode` handler and `cell()` in `cmd/portal/index.js`; `INSTANT_COLS`/`DURATION_COLS` in `chandb.js` |
| `board.stations` | `agencySQL` in `cmd/portal/board.js` — "now" is `max(last_seen)`, window −10min..+60min; re-seed if the fixture aged out |
| `board.filters` | `renderAgencyFilter` / `renderTypeFilter` / `render()` in `cmd/portal/board.js` |
| `maps.*` | `addActivity` in `cmd/portal/maps.js` — projection bounds, `heatColor` and the heat loop |
| `http.range` ≠ 206 | `dataServer` in `cmd/portal/main.go`; `http.FileServer` must handle Range natively or DuckDB-WASM pulls whole files |
| `http.traversal` ≠ 404 | the `EvalSymlinks` prefix guard in `dataServer` |
| `http.cache` | `servePage` / `serveImmutable` / `buildAssets` in `cmd/portal/main.go` |
| `http.datasets` | `scanCatalog` in `cmd/portal/main.go`, and `goldTables` in `cmd/pipeline/gold.go` |
| `browser.console` | `console.json`; a pageerror is an uncaught exception — start at the top frame |
| `browser.network` | `network.json`; a 404 on a page asset means `portalRoutes` lost a route |

## What this harness has already caught

Both of these were live bugs on `frontend-review` when the harness landed, and
both are fixed. They are recorded here because they are the two failure shapes
this project actually produces, and because each explains why a check exists.

**`static.cssvars`** — `--fg`, `--muted`, `--panel`, `--panel-2` and `--focus`
were referenced in the inline `<style>` blocks of `index.html` and `board.html`
but defined nowhere; `portal.css` shipped a different vocabulary (`--ink`,
`--bg-raise`, …) and the inline blocks were never migrated. An unresolved
`var()` drops its whole declaration, so those rules silently did nothing — no
error, no warning, just styling that quietly is not there. There is now one
stylesheet and one vocabulary, and this check guards it.

**`maps.*`** — `/maps` threw on the first heat dot it drew. `heatColor()`
returned the CSS Color 4 form `hsl(289 88% 64%)` and `addActivity` appended hex
alpha to it (`` `${color}cc` ``), producing `hsl(289 88% 64%)cc`, which is not a
parseable color. `addColorStop` threw, `run()`'s single `try/catch` swallowed
it into the status bar, and all four maps read "Unavailable" — through three
subsequent commits, because nothing looked at the page.

The important part: **`browser.console` passed the whole time.** The page
caught its own exception, so nothing ever reached `console.error`. Only the
canvas pixel check saw it. When adding a check here, assume the page will
swallow its own failure and report something reassuring; assert on what was
actually rendered, not on what the page says about itself.

## Running a single tier

```
scripts/verify/seed-lake.sh      # rebuild .verify/lake only
scripts/verify/start-portal.sh   # then browse http://127.0.0.1:8099 by hand
scripts/verify/http-checks.sh
node scripts/verify/static-checks.mjs
RUN_DIR=.verify/scratch node scripts/verify/drive.mjs
scripts/verify/stop-portal.sh
```

Only `run.sh` honours the exit-code contract end to end; a single tier reports
its own result.
