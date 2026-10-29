# Adding a data source that fits the existing schema

This guide is for adding another agency or transit provider whose realtime
feed can be represented by the existing `vehicle_positions` or `trip_updates`
schema. It uses the current MBTA/BART/SEPTA implementation as the example.
It does **not** cover inventing a new gold schema (for example, alerts or
fare data).

Start with the architecture overview in
[`how-it-works.md`](how-it-works.md), then inspect the implementation in
[`cmd/pipeline/main.go`](../cmd/pipeline/main.go),
[`bronze.go`](../cmd/pipeline/bronze.go), [`static.go`](../cmd/pipeline/static.go),
and [`gold.go`](../cmd/pipeline/gold.go).

## Decide whether this is configuration or code

There is no external feed registry in this repository. The `feeds` slice in
`main.go` is the source registry, so adding an endpoint is a **code change**.
The following are usually code changes too:

* adding an agency/feed entry and its URL, extension, and polling policy;
* adding a new static-feed layout to `nestedStatic`;
* mapping a new GTFS-RT entity or field in `bronze.go`;
* adding a gold table entry or changing the SQL in `gold.go`;
* adding tests and fixtures.

Operational values are **data-source configuration**, not application code:
the API key file on the host, `INGEST_INTERVAL`, `LAKE_DIR`, and `SECRETS_DIR`.
The deployment workflow supplies secrets; do not put credentials, URLs with
real keys, or a downloaded feed in git. The compose service mounts
`/opt/chandb/secrets` read-only at `/run/secrets`; see
[`deploy/docker-compose.yml`](../deploy/docker-compose.yml) and
[`deploy/.env.example`](../deploy/.env.example).

## 1. Establish agency identity and ID boundaries

Before editing code, obtain the provider's GTFS static zip and GTFS-RT samples.
Confirm that `agency_id`, `route_id`, `trip_id`, and `stop_id` are in the same
ID space. A human-facing agency name is not sufficient: the pipeline joins
realtime IDs to static IDs.

Use a new stable, lowercase directory key (for example `newagency`) unless
the feed is genuinely the same ID space as an existing agency. Do **not**
merge two operators merely because a portal calls them one system. IDs such as
`1`, `A`, or `10004` commonly collide across feeds. Each agency gets its own
`raw/<agency>`, `bronze/<agency>`, and `gold/<agency>` tree, and gold loads
static tables per agency specifically to prevent cross-agency joins. SEPTA's
bus and Regional Rail feeds demonstrate this split as `septa` and
`septarail`; their outer static zip is the same, but their inner ID spaces are
not.

## 2. Register realtime and static feeds

Add entries to `feeds` in `main.go`. A normal GTFS-RT source looks like:

```go
{
    agency: "newagency",
    name:   "vehicle_positions", // or "trip_updates"
    url:    "https://example.invalid/gtfs-rt/vehicle-positions.pb",
},
```

The `name` is a schema key, not a display label. Only `vehicle_positions` and
`trip_updates` are decoded by bronze today. An `alerts` entry is accepted and
raw snapshots are retained, but it intentionally produces no bronze or gold
dataset until an alerts mapping is implemented.

For a static GTFS download, use the same agency and the reserved name
`gtfs_static`:

```go
{
    agency: "newagency",
    name:   "gtfs_static",
    url:    "https://example.invalid/gtfs.zip",
    ext:    "zip",
    daily:  true,
},
```

`daily: true` means at most one snapshot per UTC `dt=` partition. This is
appropriate for a static feed that changes daily or less often and avoids
downloading the same zip every 30 minutes. Do not use it for realtime feeds.
The fetcher writes `raw/<agency>/<feed>/dt=YYYY-MM-DD/<timestamp>.<ext>` and
publishes the file with a same-directory temp file followed by `rename`.
Preserve that atomic-write convention; a partial response must never look like
a complete snapshot.

If the provider requires authentication, put `{key}` in the URL and name a
secret file:

```go
url:    "https://api.example.invalid/vehicles?api_key={key}",
secret: "newagency_api_key",
```

`readSecret` trims a non-empty file from `SECRETS_DIR`; a missing/empty file
skips that feed rather than failing the complete cycle. Keep keys out of
environment variables and logs. Add a repository/Actions secret and a matching
file-writing branch to the “Install ingest secrets” step (the checked-in
example is `API_511_KEY` → `511_api_key`) in
[`.github/workflows/deploy.yml`](../.github/workflows/deploy.yml). The compose
comment and [`gitops-plan.md`](gitops-plan.md) document the host-side secret
directory. This workflow edit is deployment configuration code; the key value
itself is not checked in.

## 3. Verify the static package shape

`static.go` extracts only `routes.txt`, `stops.txt`, and `trips.txt` into
bronze. Gold uses those columns:

* routes: `route_id`, names, `route_type`, `route_color`;
* trips: `trip_id`, `route_id`, `trip_headsign`;
* stops: `stop_id`, `stop_name`, `stop_lat`, `stop_lon`.

The IDs are deliberately read as `VARCHAR` in DuckDB. Do not “clean up”
numeric-looking IDs or remove leading zeroes. If the zip has a wrapper
directory, `findMember` already matches by basename. If it contains an inner
zip, add an agency-specific entry to `nestedStatic` (as for SEPTA) and test
that each agency selects the correct inner archive. Never make one agency use
another agency's static tables.

If the provider cannot supply these three tables or its IDs do not line up,
it does not fit this guide's existing gold schema. Stop and design a
provider-specific transform instead of silently producing null names.

## 4. Decode and map bronze

`transformBronze` walks raw `.pb` snapshots and writes one NDJSON file with the
same agency/feed/date path. `decodeSnapshot` dispatches by feed name:

* `vehicle_positions` maps vehicle, trip, route, direction, position,
  status, stop, and vehicle timestamp fields;
* `trip_updates` maps one row per stop-time update, including trip/vehicle
  IDs, stop sequence, absolute times, delays, and schedule relationship.

Inspect a real sample before assuming fields exist. Missing protobuf fields are
represented as omitted JSON fields (pointers distinguish absent values from
zero), and the gold SQL pins its input types because feed-specific sparsity
must not change the table shape. A provider may publish trip updates without
`stop_id`, `arrival_time`, `route_id`, or `stop_sequence`; do not fabricate
values. Such rows can still be archived, but their joins and board usefulness
will be limited. BART and SEPTA Regional Rail are existing examples of this
behavior.

If the feed uses a new GTFS-RT entity or needs a different mapping, add the
mapping in `bronze.go` and a focused fixture test. Do not parse at fetch time:
raw bytes are the authoritative replay buffer.

## 5. Register the gold dataset

Add one `goldTable` per supported source/agency combination in `goldTables`,
choosing `goldQuery` for vehicle positions or `stopEventsQuery` for trip
updates:

```go
{agency: "newagency", source: "vehicle_positions",
    out: "vehicle_positions", query: goldQuery},
```

The gold transform discovers agencies from this slice, finds the newest
**complete** static snapshot, loads the three CSV lookup tables once, and
writes `gold/<agency>/<out>/dt=YYYY-MM-DD/part-0.parquet`. Gold uses LEFT JOINs
so realtime-only/added trips are not silently discarded. Gold files are
published through a temporary Parquet path and rename.

The portal does not have a hard-coded dataset list. Its catalog scans gold and
names datasets as `<agency>_<out>`; it serves only the gold tree. Therefore a
new `goldTables` entry becomes visible at `/api/datasets` after a successful
partition build. Raw and bronze must remain unexposed. If a new output name is
not one of the existing page queries, update the portal UI deliberately; that
is a separate schema/UI change, not a feed registration shortcut.

## 6. Add fixtures and tests before deployment

Use small checked-in fixtures or in-memory messages, not live network calls.
Follow the existing tests in
[`main_test.go`](../cmd/pipeline/main_test.go),
[`bronze_test.go`](../cmd/pipeline/bronze_test.go), and
[`static_test.go`](../cmd/pipeline/static_test.go). Cover at least:

1. feed registry uniqueness (`agency/name`) and `{key}`/`secret` consistency;
2. expected raw and bronze paths, including `.zip` and daily behavior;
3. representative protobuf fields, including absent optional fields;
4. static CSVs with string IDs, leading zeros, commas, and provider-specific
   wrapper/nested-zip layout;
5. the correct agency's static join and a deliberate collision with another
   agency;
6. incomplete static extraction being skipped, not used by gold;
7. atomic output behavior and malformed/oversized input failure.

Run the targeted package and the same checks CI runs:

```bash
go test ./cmd/pipeline
go test -race ./...
go vet ./...
go build ./...
```

For a local end-to-end smoke test, create a writable lake in the repository
(not in git), then run the real one-shot pipeline:

```bash
mkdir -p .local-lake
LAKE_DIR="$PWD/.local-lake" SECRETS_DIR="$PWD/.local-secrets" \
  go run ./cmd/pipeline -once
```

This performs live fetches and can fail when an upstream is unavailable; it
is not a deterministic unit test. Remove `.local-lake` and
`.local-secrets` afterward. The deployed equivalent is
`docker compose -f deploy/docker-compose.yml run --rm pipeline -once`
from the host's `/opt/chandb` directory.

## 7. Validate retention and deployment

The pipeline order is fetch, bronze, gold, then prune. Raw is the replay
buffer (default 14 days); bronze is intermediate (default 2 days); gold is the
archive and is never pruned. Operators can override raw/bronze retention with
`RAW_RETENTION_DAYS` and `BRONZE_RETENTION_DAYS`. A static feed marked daily
therefore leaves one raw zip per day, bounded by raw retention, while the
latest complete static snapshot supports joins.

Before rollout, validate the compose file and inspect the diff:

```bash
PORTAL_IMAGE=example.invalid/chandb:validation \
  CLOUDFLARE_TUNNEL_TOKEN=validation-only \
  docker compose -f deploy/docker-compose.yml config --quiet
git diff --check
git status --short
```

Deploy code through the normal GitHub Actions workflow; do not hand-edit
`/opt/chandb`. The workflow builds the image, rsyncs `deploy/` while excluding
`secrets/`, installs configured secret files, and runs Compose. After rollout,
check the pipeline logs and verify that:

* raw snapshots exist under the new agency/feed path;
* bronze files exist only for decoded feed types;
* a complete `bronze/<agency>/gtfs_static/dt=...` exists;
* gold Parquet exists under `gold/<agency>/...`;
* `/api/datasets` lists the expected `<agency>_<out>` name.

Common failure modes are an agency key collision, a feed whose IDs differ from
its static zip, missing GTFS-RT optional fields, treating a daily static zip
as realtime, writing files directly instead of atomically, and adding a
gold-table entry without a compatible bronze shape. Diagnose those explicitly
instead of weakening joins or type declarations.
