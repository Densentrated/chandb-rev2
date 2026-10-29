# How chandb-rev2 works

chandb-rev2 archives live transit data from **MBTA** (Boston), **BART** (SF
Bay Area) and **SEPTA** (Philadelphia) and publishes it as a public website. On that site anyone can run SQL
against the archive, look at a live departure board, or view train positions on world and city maps.

The whole system is two Go binaries built into one container image, a directory
of files on disk (the "lake"), and some deploy plumbing:

```
                       ┌──────────────────────── VPS (Ubuntu 24.04) ─────────────────────────┐
                       │                                                                      │
 MBTA / BART / 511     │   pipeline container               /lake  (host disk)                │
 GTFS-RT + static ───────▶ (cmd/pipeline)  ── writes ──▶  raw/ ─▶ bronze/ ─▶ gold/          │
 feeds over HTTPS      │   every 30 min                                    │                  │
                       │                                                   │ read-only mount  │
                       │   portal container  ◀──────────────────────────────┘                  │
                       │   (cmd/portal)  :8080, no published port                             │
                       │        ▲                                                             │
                       │        │ docker network "edge"                                       │
                       │   cloudflared ──── outbound tunnel ────────────────────────────┐     │
                       └────────────────────────────────────────────────────────────────│─────┘
                                                                                        ▼
                                                                                    Cloudflare
                                                                                        ▲
                                          Browser: DuckDB-WASM runs the SQL ────────────┘
                                          reading gold Parquet via HTTP range requests
```

The most important design decision: **the server never runs a query.** The
portal is a static file server. The visitor's browser downloads DuckDB compiled
to WebAssembly and queries the Parquet files directly. The security section
explains why.

---

## Repository map

| Path | What it is |
|---|---|
| [cmd/pipeline/main.go](../cmd/pipeline/main.go) | Feed metadata, scheduler loop, HTTP fetch → **raw** |
| [cmd/pipeline/bronze.go](../cmd/pipeline/bronze.go) | Pure protobuf decoding plus filesystem publication → NDJSON (**bronze**) |
| [cmd/pipeline/static.go](../cmd/pipeline/static.go) | Unzips static GTFS lookup tables into bronze |
| [cmd/pipeline/gold.go](../cmd/pipeline/gold.go) | DuckDB SQL joins → daily Parquet (**gold**) |
| [cmd/pipeline/prune.go](../cmd/pipeline/prune.go) | Retention: deletes old raw/bronze partitions |
| [cmd/portal/main.go](../cmd/portal/main.go) | HTTP server: pages, dataset listing, Parquet files |
| [cmd/portal/chandb.js](../cmd/portal/chandb.js) | Shared browser runtime (boots DuckDB-WASM, registers datasets) |
| [cmd/portal/index.html](../cmd/portal/index.html) | SQL viewer page (`/`) |
| [cmd/portal/board.html](../cmd/portal/board.html) | Departure board page (`/board`) |
| [cmd/portal/maps.html](../cmd/portal/maps.html) | World and city train maps (`/maps`) |
| [Dockerfile](../Dockerfile) | Builds both binaries into one image |
| [deploy/docker-compose.yml](../deploy/docker-compose.yml) | Production stack: portal, pipeline, cloudflared |
| [infra/ansible/playbook.yml](../infra/ansible/playbook.yml) | One-time VPS provisioning |
| [.github/workflows/ci.yml](../.github/workflows/ci.yml) | Lint/test/build on every PR and push |
| [.github/workflows/deploy.yml](../.github/workflows/deploy.yml) | Build image → push to GHCR → deploy over SSH |
| `docs/gitops-plan.md` | The long-form deploy plan (gitignored, local only) |

**Stale files:** [Makefile](../Makefile) and
[docker-compose.dev.yml](../docker-compose.dev.yml) come from rev1 (Python,
Postgres, Metabase). They point at a root `docker-compose.yml` and a `pipeline/`
Python package, and neither exists anymore. Don't use them to run this code.

---

## The lake: three layers

All data lives under `/lake` in the same partition layout,
`<layer>/<agency>/<table>/dt=YYYY-MM-DD/…`. This is Hive-style partitioning:
DuckDB can tell from the directory names which days a query touches.

| Layer | Format | Contents | Kept for |
|---|---|---|---|
| `raw/` | `.pb`, `.zip` | Response bodies saved byte-for-byte, never parsed | 14 days (`RAW_RETENTION_DAYS`) |
| `bronze/` | `.ndjson`, `.txt` | Each raw snapshot parsed into one JSON row per entity | 2 days (`BRONZE_RETENTION_DAYS`) |
| `gold/` | `.parquet` | Joined with static GTFS, one compacted file per day | **Forever** |

Why it's split this way:

- **Raw is a replay buffer.** Ingest never parses anything, so a parsing bug
  can't lose data. To fix a field mapping, you change the code and the pipeline
  re-derives bronze and gold from raw.
- **Bronze is a scratch layer.** It's about 5× larger than raw (≈190 MB/day
  against ≈37 MB/day), so it expires quickly.
- **Gold is the archive.** A day of gold is well under 1 MB. It's also the only
  layer the portal serves. The pruner never deletes gold. Raw expires after 14
  days, so anything deleted from gold after that can't be rebuilt.

Every file in every layer is written to a `.partial-*` temp file in the same
directory and then `rename`d into place. Rename is atomic, so a reader or a
later cycle never sees a half-written file.

---

## The pipeline (`cmd/pipeline`)

### Running modes

```
pipeline          # serve: run a cycle now, then on every INGEST_INTERVAL boundary
pipeline -once    # one cycle, exit non-zero if any feed failed (backfill/testing)
```

In production it runs as a long-lived compose service and keeps its own
schedule. Nothing external triggers it. GitHub Actions cron was rejected
because it drifts 5–20 minutes and gets disabled after 60 days without repo
activity. `untilNext` lines cycles up with wall-clock multiples of the interval
(`:00` and `:30` for the default `30m`), whenever the container started.

### One cycle

[`cycle()`](../cmd/pipeline/main.go#L215) runs four stages in a fixed order:

1. **`ingestAll`** fetches every feed in the `feeds` table using a bounded
   worker pool (four concurrent requests).
2. **`transformBronze`** parses any raw snapshot that has no bronze file yet.
3. **`transformGold`** builds or rebuilds gold Parquet partitions.
4. **`prune`** deletes expired raw and bronze partitions.

The order is load-bearing. Prune runs last so raw is never deleted before it
reaches gold. Each stage is idempotent and works on everything on disk, not just
the files from this cycle, so a missed cycle or a fresh deploy over old raw data
catches up on its own. A cycle never returns an error. One agency's CDN failing
gets logged and retried on the next tick.

Independent feed fetches and bronze decodes use bounded worker pools. Bronze
decoding is deliberately separate from publication: protobuf bytes are mapped
to rows first, then the result is written atomically. Existing outputs are
skipped, and gold uses the bronze partition directory mtime as a watermark, so
an idle cycle does not repeatedly rebuild the same Parquet. There is no
persistent incremental state: deleting any derived layer still permits a full
rebuild from the retained raw snapshots.

Each cycle emits structured `log/slog` records. Feed records include the
agency, feed name, byte count, and destination path; failures include the
feed and error; and the final `cycle ok`, `cycle finished with failures`, or
`cycle interrupted` record includes total elapsed milliseconds. This makes
fetch failures and end-to-end cycle time measurable without parsing the lake.
Stage-level timings are intentionally not yet exposed: the pipeline and portal
refactor currently changes the stage functions, so adding instrumentation there
would overlap that work. The safe current boundary is the cycle timer; a future
stage result can add `ingest_ms`, `bronze_ms`, `gold_ms`, and `prune_ms` while
preserving the same structured fields.

The performance model is deliberately asymmetric. Network fetches are
parallelized with a fixed four-worker pool, while bronze and gold remain
bounded by local CPU, disk, and DuckDB work. Bronze converts each protobuf
entity to a typed row and writes one NDJSON line; gold scans only the
partitioned bronze/static inputs and writes compact Parquet. Atomic temporary
files mean readers never observe partial output. The
`BenchmarkBronzeVehicleRows` and `BenchmarkBronzeNDJSON` benchmarks isolate the
two CPU/allocation hotspots; run them with:

```
go test ./cmd/pipeline -run '^$' -bench 'Bronze' -benchmem
```

### Stage 1: ingest → raw

The [`feeds`](../cmd/pipeline/main.go#L74) table defines everything that gets
fetched:

| Agency | Feed | Notes |
|---|---|---|
| mbta | `vehicle_positions` | Real lat/lon, no API key |
| mbta | `trip_updates` | Arrival predictions |
| bart | `trip_updates` | BART's own GTFS-RT has **no** vehicle positions |
| bart | `alerts` | Archived to raw; not parsed yet |
| bart | `vehicle_positions` | Through 511.org. Needs `/run/secrets/511_api_key`, and is **skipped** (not failed) if the key is missing |
| septa | `vehicle_positions`, `trip_updates` | Bus, metro and trolley. No key |
| septarail | `vehicle_positions`, `trip_updates` | Regional Rail. No key. Trip updates contain only a delay per trip (see Known gaps) |
| all four | `gtfs_static` | The schedule zip (`daily: true`): fetched once per day, not every cycle |

SEPTA is modelled as two agencies because it publishes two independent GTFS
feeds with separate ID spaces. Both are shipped in one `gtfs_public.zip` that
contains `google_bus.zip` and `google_rail.zip`. The zip is fetched once for
each agency, and `nestedStatic` in [static.go](../cmd/pipeline/static.go) tells
`extractStatic` which inner zip belongs to which agency.

`fetchFeed` sends a GET with a 30 s timeout, caps the body at 32 MB, rejects an
empty body, and writes the file atomically to
`raw/<agency>/<feed>/dt=…/20261004T183000Z.pb`.

### Stage 2: raw → bronze

[`transformBronze`](../cmd/pipeline/bronze.go#L74) walks `raw/`:

- **`.pb` files** are decoded as a GTFS-RT `FeedMessage` and written as NDJSON
  to the same path under `bronze/`:
  - `vehicle_positions` produces one `vehicleRow` per vehicle.
  - `trip_updates` produces one `stopTimeRow` per stop-time update. A single
    trip turns into one row for each upcoming stop.
  - Every other feed (alerts) is skipped and left in raw for later.
- **`.zip` files** (static GTFS) go to
  [`extractStatic`](../cmd/pipeline/static.go#L32). It pulls out only
  `routes.txt`, `stops.txt` and `trips.txt`. The zip is ~24 MB, mostly
  `shapes.txt`.

If a bronze file already exists, that snapshot is skipped. Raw files in
partitions older than bronze retention are also skipped, so the pipeline
doesn't rebuild what the pruner just deleted.

The bronze structs use pointer fields with `omitempty`, so a missing value
stays distinguishable from zero. A latitude of `0` is a real place, and a delay
of `0s` means something different from no prediction at all.

### Stage 3: bronze → gold

[`transformGold`](../cmd/pipeline/gold.go#L210) opens an in-memory DuckDB (via
cgo) with `memory_limit=512MB` and `threads=2`, and spills to
`/lake/.duckdb-tmp`. The spill goes to real disk because `/tmp` in the
container is RAM-backed tmpfs. Then, for each agency:

1. `latestStaticDir` finds the newest bronze `gtfs_static/dt=…` that has all
   three tables.
2. `loadStatic` loads only the needed columns into temp tables (`gtfs_routes`,
   `gtfs_trips`, `gtfs_stops`) once. Re-reading the CSVs for every partition
   ran the container out of memory.
3. Each `dt=` partition of the matching bronze table goes through one
   `COPY (SELECT …) TO 'part-0.parquet'` statement.

The [`goldTables`](../cmd/pipeline/gold.go#L202) list defines the gold tables:

| Gold table | From | What a row is |
|---|---|---|
| `mbta/vehicle_positions` | mbta bronze `vehicle_positions` | A vehicle at one instant, with route name, headsign, stop name and decoded `vehicle_type` |
| `mbta/stop_events` | mbta bronze `trip_updates` | One (trip, stop) with the **last** prediction seen |
| `bart/stop_events` | bart bronze `trip_updates` | Same, for BART |
| `septa/vehicle_positions`, `septarail/vehicle_positions` | SEPTA vehicle positions | Same shape as MBTA's |
| `septa/stop_events`, `septarail/stop_events` | SEPTA trip updates | Same shape as MBTA's. For septarail, stop and time columns are NULL |

How the queries are built:

- **LEFT JOINs everywhere.** MBTA adds realtime `ADDED-` trips that aren't in
  the static schedule, and inner joins would drop exactly those.
- **IDs are forced to `VARCHAR`.** They look numeric, so type sniffing guesses
  INT64 and then fails on the first `ADDED-…` id.
- **JSON columns are declared up front.** With `omitempty`, a field that's
  absent all day never becomes a column, and BART never sends `stop_sequence`.
- **`stop_events` dedupes** with `row_number() OVER (PARTITION BY trip_id,
  stop_id, stop_sequence ORDER BY feed_timestamp DESC)` and keeps the latest
  forecast. Agencies drop a stop from the feed once it has been served, so the
  last forecast seen is the closest proxy for what actually happened. Table
  size depends on the timetable, not on how often we poll.
- **`lead_time_s`** (= `arrival_time - feed_timestamp`) measures how much to
  trust a row. A small value means the forecast was made just before the event.
  A large value means we sampled too slowly to know. At a 30-minute interval,
  many rows are stale, so filter on this column.
- **BART trip updates have no `route_id`.** The query falls back to the route
  that `trips.txt` assigns to the trip.

**Partition finality:** once a past day's `part-0.parquet` exists, it isn't
rebuilt. Today's partition is rebuilt on every cycle.

`COPY … TO` can't take a bound parameter, so the output path is inserted into
the SQL string directly. That's safe because every partition name has passed
the `dtPartition` regex (`^dt=\d{4}-\d{2}-\d{2}$`) and nothing in that path
comes from a user.

### Stage 4: prune

[`pruneLayer`](../cmd/pipeline/prune.go#L75) deletes `raw/` and `bronze/`
partitions older than their retention period. Only directories that match the
`dt=` regex exactly are considered, so an unexpected directory is never
deleted. `gold/` is never touched.

### Pipeline configuration

| Env var | Default | Purpose |
|---|---|---|
| `INGEST_INTERVAL` | `30m` | Cycle period (Go duration) |
| `LAKE_DIR` | `/lake` | Lake root |
| `SECRETS_DIR` | `/run/secrets` | Directory of secret *files* |
| `RAW_RETENTION_DAYS` | `14` | |
| `BRONZE_RETENTION_DAYS` | `2` | |

---

## The portal (`cmd/portal`)

### Routes

| Route | Handler | Caching |
|---|---|---|
| `GET /` | SQL viewer (`index.html`, embedded) | `no-cache` |
| `GET /board` | Departure board (`board.html`, embedded) | `no-cache` |
| `GET /maps` | World and city train maps (`maps.html`, embedded) | `no-cache` |
| `GET /chandb.<hash>.js` | Shared JS module | `max-age=1y, immutable` |
| `GET /chandb.js` | Same file without the hash, for pages cached from an older deploy | `no-cache` |
| `GET /api/datasets` | Lists gold Parquet files, grouped as `<agency>_<table>` | 60 s |
| `GET /data/…` | `http.FileServer` over **`/lake/gold` only** | 60 s for today's partition, 1 day + immutable for past ones |
| `GET /healthz` | Returns `ok` | `no-store` |

The four frontend files are compiled into the binary with `//go:embed`.
`buildAssets()` hashes `chandb.js` and rewrites the `/chandb.js` references in
both pages to the hashed URL. This prevents a bug where a browser pairs a fresh
page with a stale cached module. The mismatch fails as a parse-time
SyntaxError, and the page hangs silently.

`http.FileServer` supports HTTP Range requests natively, and the browser-side
engine depends on that. `http.Dir` rejects `..`, so a request can't reach
`raw/`, `bronze/` or the secrets mount.

`portal -healthcheck` sends a request to its own `/healthz` and exits 0 or 1.
That exit code is the container's Docker healthcheck. The image has no curl, the
filesystem is read-only, and no port is published, so nothing else can check
health.

### What happens in the browser

[`boot()`](../cmd/portal/chandb.js#L17) in `chandb.js`:

1. Loads DuckDB-WASM from jsDelivr. The bundle is ~34 MB on the first visit
   and cached after that, and download progress is shown in the status bar.
2. Fetches `/api/datasets`.
3. Registers each Parquet URL with `DuckDBDataProtocol.HTTP`, which makes reads
   use range requests. A query then downloads only the row groups and columns
   it needs.
4. Creates one view per dataset (`mbta_vehicle_positions`, `bart_stop_events`,
   `septa_stop_events`, `septarail_vehicle_positions`, …). Each view is
   `read_parquet([...every partition...])`.

The pages are built to tolerate schema drift. The image (with the pages) and
gold (rebuilt by the pipeline) deploy independently, so for a while after a
deploy they can disagree about which columns exist. `columnsOf()` runs
`DESCRIBE`, and the pages then:

- hide example queries whose `needs` columns are missing (`index.html`);
- write their SQL to match the columns that are present, for example by
  falling back from `vehicle_type` to `route_type` (`board.html`).

The two pages:

- **`/` (SQL viewer):** a textarea with example queries (where trains are now,
  lateness by hour, dwell times, data quality by `lead_time_s`, and more). A
  toggle switches between epoch seconds and human-readable times.
- **`/board` (departure board):** builds one `SELECT` per `*_stop_events` table
  and combines them with `UNION ALL`. The board's "now" is that table's
  `max(last_seen)`, not the browser clock, because snapshots can be up to 30
  minutes old. It shows arrivals from 10 minutes before now to 60 minutes
  after, groups them by (agency, stop), sorts rail ahead of bus and then by
  soonest arrival, and draws at most 60 stations. Filters cover agency, mode
  and stop name.

`rowsOf()`/`num()` convert DuckDB-WASM's `BigInt` columns to `Number`. Mixing
the two types in arithmetic or in `Array.sort` throws an error.

All upstream text goes into the DOM through `textContent`, never `innerHTML`.

---

## Security model

The design rests on a few rules:

1. **No server-side queries.** A public, unauthenticated DuckDB endpoint would
   have to defend against `read_blob('/proc/self/environ')`, `ATTACH`,
   `COPY TO`, `INSTALL httpfs` and filling the disk through temp spill. Running
   DuckDB in the browser removes all of those. The worst a hostile query can do
   is crash the visitor's own tab.
2. **Secrets are files, never environment variables.** CI writes them to
   `/opt/chandb/secrets` on the host, which is mounted read-only at
   `/run/secrets`. The only secret in an env var is the cloudflared tunnel
   token, and that container never runs untrusted code.
3. **No published ports.** `deploy/docker-compose.yml` has no `ports:` key.
   The only way in is the outbound Cloudflare tunnel, so `CF-Connecting-IP` can
   be trusted for rate limiting. The host firewall allows only SSH.
4. **Containers are locked down.** Both run as UID 10001 with a read-only root
   filesystem, `cap_drop: [ALL]`, `no-new-privileges`, and PID and memory
   limits. The portal mounts `/lake` read-only; the pipeline mounts it
   read-write.
5. **Defensive input handling in the pipeline.** Bodies are size-capped. Zip
   extraction matches member names by base name (blocking zip-slip) and caps
   decompressed size at 256 MB (blocking zip bombs). Partition names are
   checked by regex before they go into SQL.

---

## Build, CI and deploy

### Image

The [Dockerfile](../Dockerfile) builds in two stages:

- **Build:** `golang:1.24-bookworm`, with `CGO_ENABLED=1`. The DuckDB driver
  needs cgo, and cross-compiling quietly turns cgo off, which surfaces as an
  `undefined: conn` error.
- **Runtime:** `debian:bookworm-slim`, with `ca-certificates` and user 10001.

The image holds both binaries. `ENTRYPOINT` is the portal, and compose
overrides it for the pipeline service.

### CI ([ci.yml](../.github/workflows/ci.yml))

Runs on every PR and every push to `main`:

- `go vet`, `go build` and `go test -race`;
- `golangci-lint`;
- a Docker build with no push;
- `ansible-lint` on the playbook.

The single required check is `ci-ok`, which has `if: always()`. Without that,
a failed dependency would leave it *skipped*, and GitHub treats a skipped
required check as passing.

### Deploy ([deploy.yml](../.github/workflows/deploy.yml))

On every push to `main`:

1. Build the image and push it to `ghcr.io/densentrated/chandb-rev2:<sha>` and
   `:latest`. The package is public.
2. SSH to the VPS as `deploy`. The host key is pinned via `SSH_KNOWN_HOSTS`.
3. `rsync --delete` copies `deploy/` to `/opt/chandb/`, excluding `secrets/`.
   Don't hand-edit files there; the next deploy overwrites them.
4. Write `/opt/chandb/secrets/511_api_key` (if `API_511_KEY` is set) and
   `/opt/chandb/.env` (image tag and tunnel token) over SSH stdin.
5. Run `docker compose pull && up -d`, poll the portal's health status for up
   to ~60 s, then prune images older than 7 days.

**Rollback:** run the workflow manually with `image_tag=<old sha>`. It skips
the build and deploys that tag.

### Host provisioning ([playbook.yml](../infra/ansible/playbook.yml))

Run by hand from your laptop, not from CI:

```
cd infra/ansible
cp inventory.ini.example inventory.ini   # fill in host + admin user
ansible-galaxy collection install -r requirements.yml
ansible-playbook playbook.yml --check --diff
ansible-playbook playbook.yml --diff
```

The playbook:

- installs Docker CE (removing the Ubuntu-packaged Docker first);
- creates the `portal` (10001) and `deploy` users and installs the CI SSH key
  from `~/.ssh/chandb_ci.pub`;
- creates `/opt/chandb`, `/opt/chandb/secrets` (mode `2750`: setgid, so files
  CI writes there are readable by the containers) and `/lake`, owned by 10001;
- turns off SSH password and root login, after first checking that you have a
  key installed so you can't lock yourself out;
- sets up ufw (allow 22 only), fail2ban and unattended-upgrades;
- finishes by connecting to port 22 from your machine, to confirm you can still
  get in.

---

## Working on it locally

```
go test -race ./...                      # everything is unit tested
mkdir -p /tmp/lake
LAKE_DIR=/tmp/lake go run ./cmd/pipeline -once     # real fetch + all transforms
LAKE_DIR=/tmp/lake PORTAL_ADDR=:8080 go run ./cmd/portal
# open http://localhost:8080 and http://localhost:8080/board
```

The pipeline needs a C toolchain because DuckDB uses cgo. Without
`SECRETS_DIR/511_api_key`, the BART vehicle-positions feed is skipped.

The tests cover the invariants the comments describe:

- Hive paths and UTC normalisation;
- atomic writes that leave no partial files;
- protobuf → NDJSON mapping;
- zip-slip, and skipping incomplete static snapshots;
- `dtPartition` rejecting SQL injection;
- retention that never touches gold;
- Range requests, `..` traversal, and the content-hash caching scheme.

### Adding things

- **A new realtime feed:** add an entry to `feeds` in `main.go`. Raw capture
  starts on the next cycle with no other changes.
- **Parsing a new feed type:** add a case in `transformFile` (`bronze.go`).
  Raw from the last 14 days is backfilled automatically.
- **A new gold table:** add a query and a `goldTables` entry (`gold.go`). The
  portal and both pages discover it through `/api/datasets`.
- **A new agency:** add realtime feeds, a `gtfs_static` feed and `goldTables`
  entries. Static lookups are always per agency, never shared, because ID
  collisions across agencies would produce wrong joins.

---

## Known gaps and quirks

- **Not in gold yet:** BART `alerts` and BART `vehicle_positions` (from 511)
  reach raw (and, for vehicle positions, bronze), but no gold table uses them.
- **SEPTA Regional Rail isn't on the departure board.** Its trip updates send
  one `stop_time_update` per trip with only `stop_sequence` and `delay`: no
  `stop_id` and no arrival time. Rows reach `septarail_stop_events` with NULL
  stop and time columns, and the board skips them. To fix this, join
  `stop_times.txt` (2.7 MB for rail) on `(trip_id, stop_sequence)` and apply
  the delay to scheduled times.
- **A failed last cycle of the day is never rebuilt.** Once a day is in the
  past, its gold file counts as final. If the 23:30 UTC cycle failed, that day
  is missing those snapshots until someone deletes the Parquet file and lets
  the pipeline rebuild it (raw is still there for 14 days).
- **Static zips are re-extracted every cycle.** In `transformBronze`, the zip
  branch runs before the bronze-retention check. Raw static zips from day 3 to
  day 14 get re-extracted into bronze on every cycle, and `prune` deletes them
  again. This is wasted work, not data loss.
- **`stop_events` is split by UTC day.** A trip that crosses UTC midnight can
  show up in two partitions.
- **Some code comments disagree with compose.** Comments in `gold.go` describe
  a 512 MB container and a 48 MB tmpfs. Compose sets `mem_limit: 1g` and a 64 MB
  `/tmp`. The DuckDB limit is 512 MB either way.
- **The Makefile and `docker-compose.dev.yml` are from rev1** (see the
  repository map).
