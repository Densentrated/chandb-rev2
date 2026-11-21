# chandb-rev2

Archives live transit data from **MBTA** (Boston), **BART** (SF Bay Area) and
**SEPTA** (Philadelphia), and publishes it as a public website where anyone can
run SQL against the archive, read a live departure board, or look at where
trains actually run.

**The server never runs a query.** It is a static file server. Your browser
downloads DuckDB compiled to WebAssembly and queries the Parquet files directly
over HTTP range requests, so there is no query endpoint to attack and the worst
a hostile query can do is crash the tab it runs in.

```
GTFS-RT feeds ──▶ pipeline ──▶ raw/ ──▶ bronze/ ──▶ gold/ (Parquet, kept forever)
                                                      │
                               portal (static files) ◀┘
                                      │
                      browser: DuckDB-WASM runs the SQL
```

Two Go binaries in one container image, a directory of files on disk, and a
Cloudflare tunnel.

## Quick start

```
go test -race ./...
make setup          # one time: Playwright + chromium for the verify harness
make verify         # seed a lake, boot the portal, drive all three pages
```

To run it against real data:

```
mkdir -p /tmp/lake
LAKE_DIR=/tmp/lake go run ./cmd/pipeline -once     # real fetch + all transforms
LAKE_DIR=/tmp/lake PORTAL_ADDR=:8080 go run ./cmd/portal
# open http://localhost:8080, /board and /maps
```

The pipeline needs a C toolchain — DuckDB uses cgo. `make help` lists every
target.

## Verifying a change

`go test` covers the server, but the query engine runs in the browser, so a
green test suite is compatible with every page rendering blank. `make verify`
seeds a deterministic lake, boots the portal, drives all three pages in
headless chromium, and writes screenshots, a console log, a Playwright trace
and a PASS/FAIL table to `.verify/latest/`. It exits **0** pass, **1** broken,
**2** blocked by infrastructure.

See [.claude/skills/verify/SKILL.md](.claude/skills/verify/SKILL.md) for every
check and what to do when one fails.

## Docs

| | |
|---|---|
| [docs/how-it-works.md](docs/how-it-works.md) | Architecture, the three lake layers, security model, build and deploy |
| [docs/adding-a-data-source.md](docs/adding-a-data-source.md) | Adding an agency that fits the existing schema |
| [docs/adding-a-data-pipeline.md](docs/adding-a-data-pipeline.md) | Adding a feed that needs a new gold schema |
