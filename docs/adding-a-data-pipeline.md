# Adding a new data pipeline or schema

This guide is for adding a **new kind of data** to chandb-rev2, not merely
adding another MBTA-compatible GTFS-RT endpoint. It describes the repository's
current file-backed lake:

```text
source -> cmd/pipeline -> raw/ -> bronze/ -> gold/ -> cmd/portal -> browser
```

The pipeline container writes the lake and the portal container mounts only
`gold/` read-only. The browser runs DuckDB-WASM and reads published Parquet
with HTTP range requests; the server does not execute visitor SQL. Start with
[How chandb-rev2 works](how-it-works.md), especially its [repository
map](how-it-works.md#repository-map), before changing this design.

## 1. Decide whether this belongs in the existing pipeline

First write a one-page data contract: source, owner, refresh cadence,
identity, expected volume, retention, sensitivity, consumers, and rebuild
story. Then choose one of these paths:

* **Extend the existing transit pipeline** when the source is transit-shaped,
  can use the existing agency/feed/dt partition convention, and benefits from
  the existing GTFS joins, retention, DuckDB gold transform, and portal
  discovery.
* **Add a feed to an existing agency** when it has the same domain identity
  space and static reference data. Do not call two unrelated ID spaces one
  agency just because the URLs have the same owner.
* **Add a new agency/domain namespace** when the data is transit-related but
  has independent identifiers or static schedules. The existing code already
  models SEPTA bus and Regional Rail as `septa` and `septarail` because their
  IDs must never be joined.
* **Isolate it from the transit pipeline** when it has a different protocol or
  lifecycle, needs different credentials or retention, has materially
  different volume/latency, requires a separate failure budget, or would make
  transit gold rebuilds and portal behavior ambiguous. Isolation can mean a
  separate command/package and lake subtree, or a separate service and
  deployment. Prefer isolation over making `transformFile` and `goldTables`
  a collection of unrelated special cases.

Isolation is particularly important for personally identifying, regulated, or
high-cardinality data: the public portal exposes every Parquet file under
`gold/`. A dataset that cannot be public is not suitable for the existing
public gold tree without a separately designed access boundary.

## 2. Know the layer contract

The repository uses Hive-style paths:

```text
<layer>/<domain-or-agency>/<table>/dt=YYYY-MM-DD/<snapshot-or-part>.ext
```

| Layer | Put here | Contract and default lifetime |
| --- | --- | --- |
| `raw/` | The source response, byte-for-byte (`.pb`, `.zip`, JSON, CSV, ...) | Replayable input; 14 days by `RAW_RETENTION_DAYS` |
| `bronze/` | Parsed, typed, one-record-per-entity snapshots (`.ndjson` or reference `.txt`) | Recomputable intermediate; 2 days by `BRONZE_RETENTION_DAYS` |
| `gold/` | Consumer-facing, normalized and joined Parquet | Stable public contract; never pruned by the current pruner |

Raw must not be silently “cleaned up” during ingest. Keeping the original
bytes allows a decoder or mapping bug to be fixed and replayed. Bronze should
retain source provenance and distinguish missing values from real zero values.
Gold should be compact (normally one `part-0.parquet` per day), typed, documented,
and safe for a browser to query.

If the source is already durable static/reference data, it still belongs in
raw first. Extract only the tables needed by gold into bronze, as
[`static.go`](../cmd/pipeline/static.go) does for `routes.txt`, `stops.txt`,
and `trips.txt`. Never publish raw or bronze through the portal.

## 3. Define identity and a stable schema before coding

### Domain identity

Choose names that are stable, lowercase, URL/path safe, and unambiguous:

* `agency` identifies one ID namespace, not merely an organization.
* `feed` identifies one source/product (`vehicle_positions`,
  `trip_updates`, or a new domain-specific name).
* `table` identifies the semantic row type, not the source filename.
* `dt` is the UTC calendar date used for partitioning.
* Include `source` or `provider` in gold when several sources can produce the
  same semantic table.

Write down the row identity (for example `(provider_id, observed_at)`,
`(trip_id, stop_id, stop_sequence)`, or a natural event ID), whether duplicates
are expected, and how updates are resolved. A timestamp alone is not an
identity.

### Schema rules

For every field specify type, nullability, units, timezone, meaning, source,
and whether it is safe to change. Prefer:

* `VARCHAR` for external identifiers, even if they look numeric. GTFS IDs can
  later contain values such as `ADDED-*`.
* UTC epoch seconds for instants when matching existing transit gold; use
  explicit `*_time`/`*_timestamp` names and document the unit.
* `DOUBLE` only where the source is genuinely fractional; do not turn missing
  values into `0`.
* separate code and display fields (`route_id` and `route_short_name`);
* an explicit schema in DuckDB `read_json`, rather than inference from a
  single sparse snapshot.

Treat gold columns as an API. Adding a nullable column is usually compatible;
renaming, changing units, changing identity, or changing the meaning of an
existing column requires a versioning and rebuild plan. If incompatible
versions must coexist, publish a new table name or versioned path rather than
surprising existing SQL queries.

## 4. Add feed and configuration metadata

For a feed that fits the existing scheduler, add a `feed` entry in
[`cmd/pipeline/main.go`](../cmd/pipeline/main.go). Set:

* a unique `agency` + `name`;
* the HTTPS `url`;
* `ext` for non-protobuf input (for example `zip`);
* `daily: true` only when one snapshot per UTC day is sufficient;
* `secret` for a secret file placeholder, never a credential in source.

The existing fetcher caps responses at 32 MiB, uses a 30-second timeout, writes
the response atomically, and records a timestamped file under `raw/`. A new
source larger than that, requiring pagination, checksums, authentication
refresh, or a cadence other than the shared `INGEST_INTERVAL` is evidence for
an isolated ingester rather than a new flag on the common fetcher.

Check the endpoint's license, robots/terms, rate limit, update semantics,
redirect behavior, and whether it contains secrets or personal data. Use a
small fixture instead of a production response for tests.

## 5. Implement decoding and normalization

Add a focused case to `transformFile` in
[`cmd/pipeline/bronze.go`](../cmd/pipeline/bronze.go), or create a separate
decoder for a genuinely different format. Keep these responsibilities
separate:

1. decode bytes and validate the source envelope;
2. normalize names, IDs, units, timestamps, and nullability;
3. attach provenance (`agency`, `feed`, source timestamp, and schema/version);
4. publish the complete NDJSON file atomically.

Do not publish rows incrementally: a failed decode must leave no file that
looks complete. Preserve unknown or forward-compatible source values where
possible, and reject malformed records with a useful feed/path error rather
than crashing the whole cycle. Keep an unsupported feed in raw until its
decoder exists; that is the established behavior for unmodelled alerts.

For static data, use a bounded archive reader like
[`extractStatic`](../cmd/pipeline/static.go). Match safe member names, cap
decompression, reject path traversal, and only mark a snapshot usable when all
required reference tables exist. Keep reference data in the same domain
namespace as the facts it describes; never join two agencies' lookup tables.

## 6. Add the gold transformation and publish Parquet

Add a query and an entry to `goldTables` in
[`cmd/pipeline/gold.go`](../cmd/pipeline/gold.go), or isolate the transform if
it cannot share the existing DuckDB lifecycle. The transform should:

* declare every JSON column and its type;
* use `VARCHAR` for external IDs;
* use `LEFT JOIN` when source facts may legitimately lack reference rows;
* define deterministic deduplication with a window function and a documented
  ordering;
* retain source timestamps and a freshness/quality measure where applicable;
* use a fixed, deterministic column list and ordering;
* write `FORMAT PARQUET, COMPRESSION ZSTD` through a temporary file followed by
  atomic rename;
* validate `dt=` with the existing `dtPartition` guard before interpolating a
  path into SQL.

The current implementation loads reference CSVs once into temporary DuckDB
tables, then writes one compact Parquet file per day. Preserve that shape
unless the new data's cardinality makes it unsafe. Do not rebuild an immutable
past partition just because a cycle runs: understand the existing “today is
mutable, past days are final” rule and provide an explicit backfill/rebuild
procedure when late data matters.

Before publishing, decide what “complete” means: all required source fields,
all reference tables, a watermark, or merely a valid empty result. An empty
gold partition can be meaningful, but it must not be confused with a failed
transform.

## 7. Expose it through the portal deliberately

The portal automatically scans `gold/` in
[`cmd/portal/main.go`](../cmd/portal/main.go), groups files as
`<first-directory>_<second-directory>`, and returns them from
`GET /api/datasets`. `chandb.js` registers each group as a DuckDB-WASM view.
Therefore a normal new `<domain>/<table>/dt=.../*.parquet` needs no catalog
code change.

Still verify that:

* the generated dataset name is useful and does not collide with an existing
  one;
* every column is queryable in DuckDB-WASM;
* HTTP range requests work and the files are not needlessly huge;
* the data is safe for unauthenticated public download;
* SQL examples or the departure board should be updated only if the new
  dataset is a first-class user experience.

If the dataset needs a label, description, owner, units, license, or schema
version, the current catalog has no metadata fields beyond name, files, and
bytes. Decide whether to add that metadata to the catalog contract (and test
both pages' compatibility) or keep documentation beside the dataset. Do not
invent a second undocumented catalog endpoint.

## 8. Retention, rebuilds, and versioning

Document the complete recovery matrix before merging:

* Can gold be rebuilt from retained raw after bronze is deleted?
* Is the raw retention long enough for the source's correction window?
* Does static/reference data need a historical snapshot for reproducibility?
* Which partitions are mutable, and how is a backfill invoked?
* What happens after a schema or normalization bug?

The current [`prune.go`](../cmd/pipeline/prune.go) deletes only matching old
`dt=` directories in raw and bronze; gold is the archive. Never lower retention
or add gold deletion without an explicit storage and recovery decision. If the
new pipeline has a different retention policy, isolate it rather than silently
coupling it to the transit defaults.

For a breaking gold change, choose one of:

1. rebuild all affected gold from retained raw and replace the existing
   contract (only when consumers can tolerate it);
2. publish `table_v2` or a versioned namespace and deprecate the old one;
3. keep both schemas with a migration note and an end date.

Record schema version and code/source provenance in the gold documentation or
columns. A deployment rollback restores binaries, not necessarily already
published Parquet, so compatibility must work during mixed-version operation.

## 9. Tests and fixtures

Add small, deterministic fixtures under the relevant package's test data (not
real credentials or large production captures). At minimum test:

* raw path and UTC `dt` normalization;
* successful decode for a representative record;
* missing, zero, malformed, duplicate, and unknown values;
* stable JSON/NDJSON schema when optional fields are absent;
* atomic publication and no output on failure;
* archive traversal and decompression limits for zip-like input;
* reference snapshot selection and incomplete-reference behavior;
* joins with missing lookup rows and IDs containing letters;
* deduplication/watermark/finality rules;
* Parquet columns, types, row count, and nullability;
* retention never deleting gold;
* catalog discovery, HTTP range reads, and portal schema drift if the public
  surface changes.

Follow the existing table-driven style in
[`bronze_test.go`](../cmd/pipeline/bronze_test.go),
[`static_test.go`](../cmd/pipeline/static_test.go),
[`prune_test.go`](../cmd/pipeline/prune_test.go), and
[`cmd/portal/main_test.go`](../cmd/portal/main_test.go). Test the pure decoder
without network access; reserve end-to-end fetches for an explicit local
exercise.

## 10. Observability and operations

Every stage should emit structured `log/slog` fields for domain, feed/table,
partition, source timestamp, output path, input/output row counts, bytes,
duration, schema version, and error. Distinguish:

* source unavailable or unauthorized;
* decode/validation failure;
* incomplete reference data;
* gold query failure;
* publication/rename failure;
* an intentionally empty result.

The shared cycle must continue processing independent feeds, but a `-once`
run must return non-zero when a feed fails. Make warnings actionable and avoid
logging credentials or raw sensitive payloads. Add a freshness and row-count
check that can alert on “successful but empty” data, not just process liveness.

## 11. Security and deployment

Use secret files under `SECRETS_DIR`; never commit keys or put them in URLs,
logs, fixtures, or ordinary environment variables. Enforce HTTPS, timeouts,
response-size bounds, archive limits, safe path construction, and allowlisted
source hosts where appropriate. Treat source content as hostile.

The public boundary is the gold tree. Verify that no raw/bronze/scratch path
can be reached through `/data/`, that symlinks cannot escape the lake, and that
Parquet contains no confidential columns. Preserve the portal's read-only
mount and container hardening in [`deploy/docker-compose.yml`](../deploy/docker-compose.yml).

Update deployment only when needed: the normal code change is built by
[`deploy.yml`](../.github/workflows/deploy.yml), pushed to GHCR, and rolled out
by compose. A new secret requires the deployment secret-writing path and host
permissions to be updated. A new service requires an explicit compose service,
resource limits, health check, lake mount mode, and rollback plan. Do not
hand-edit `/opt/chandb`; deployment rsync overwrites it.

## Step-by-step implementation checklist

- [ ] Write the data contract, public/privacy decision, row identity, units,
      nullability, schema version, retention, and rebuild plan.
- [ ] Decide extend-versus-isolate; if extending, choose a non-colliding
      domain/agency/feed/table namespace.
- [ ] Add feed/configuration metadata, secret-file handling, cadence, and
      size limits.
- [ ] Add raw path tests and a representative, sanitized fixture.
- [ ] Implement bounded decoding, normalization, provenance, and atomic bronze
      publication.
- [ ] Implement bounded static/reference extraction if required.
- [ ] Add explicit gold schema, joins, deduplication, quality fields, and
      atomic compressed Parquet publication.
- [ ] Define late-arrival/finality behavior and a reproducible backfill command
      or documented operational procedure.
- [ ] Confirm retention and pruning do not destroy required rebuild inputs.
- [ ] Verify catalog grouping, public safety, range requests, and optional UI
      integration.
- [ ] Add unit, fixture, transform, retention, and portal tests.
- [ ] Add structured logs, row/byte/freshness metrics or checks, and failure
      alerts.
- [ ] Update deployment/secrets/resource limits only if the new source needs
      them; document rollback and mixed-version compatibility.
- [ ] Run the validation commands below and review the diff for credentials,
      accidental large fixtures, and unrelated application changes.

## Acceptance criteria

A new pipeline is ready when:

1. A reviewer can identify its row identity, schema version, domain boundary,
   retention, and rebuild source without reading implementation details.
2. A failed or malicious source cannot publish a partial file, escape its
   namespace, exhaust disk through decompression, or expose a secret.
3. A clean run produces the documented raw, bronze, and gold paths; a repeated
   run is idempotent; and a rebuild from retained raw produces equivalent gold.
4. Gold has explicit, typed columns and deterministic deduplication, and its
   Parquet can be read with DuckDB both server-side in tests and through the
   browser portal.
5. Missing reference rows and optional fields are handled intentionally, with
   no accidental loss caused by an inner join or schema inference.
6. Retention tests prove required replay inputs survive and gold is not pruned.
7. Logs identify source, partition, counts, duration, and failure class without
   sensitive data; freshness/empty-output regressions are detectable.
8. CI, deployment, rollback, and any required secret provisioning are
   documented and pass without relying on stale rev1 files.

## Repository validation commands

Run from the repository root. These commands do not fetch production data:

```sh
go test -race ./...
go vet ./...
go build ./...
git diff --check
```

For a local lake exercise, use a project-local directory (not a system
temporary directory), and remove it after inspection:

```sh
mkdir -p .local/lake
LAKE_DIR="$PWD/.local/lake" go run ./cmd/pipeline -once
find .local/lake -maxdepth 6 -type f | sort
rm -rf .local
```

The last exercise performs real upstream fetches and may require
`SECRETS_DIR/511_api_key`; use fixtures and package tests for deterministic CI.
For portal validation, build and run the binary with a fixture gold tree, then
check `/healthz`, `/api/datasets`, and a Parquet URL with a `Range` request.
The portal's own tests already cover catalog grouping, traversal protection,
and range-serving behavior.

Related references: [pipeline architecture](how-it-works.md#the-pipeline-cmdpipeline),
[gold transformation](how-it-works.md#stage-3-bronze--gold), [portal catalog](how-it-works.md#routes),
[security model](how-it-works.md#security-model), and [working locally](how-it-works.md#working-on-it-locally).
