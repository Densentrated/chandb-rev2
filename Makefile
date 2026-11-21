# Task runner for chandb-rev2.
#
# Two Go binaries and a lake of files on disk. There is no local Postgres, no
# Metabase and no Python: the query engine is DuckDB-WASM in the visitor's
# browser, and cmd/portal is a static file server. See docs/how-it-works.md.

.PHONY: help duckdb-ext verify verify-seed verify-portal verify-stop verify-static \
        verify-http verify-browser setup test lint ci dev-portal dev-pipeline \
        rebuild clean

LAKE ?= .verify/lake

help:          ## list targets
	@grep -hE '^[a-z-]+:.*?##' $(MAKEFILE_LIST) \
	  | sed 's/:.*##/\t/' | sort | awk -F'\t' '{printf "  %-16s %s\n", $$1, $$2}'

# --------------------------------------------------------------------- verify
# The harness that proves the app actually works. A green `go test` says
# nothing about whether /maps drew anything — the engine runs in the browser.
# Full documentation lives in .claude/skills/verify/SKILL.md.

setup:         ## one-time: Playwright chromium + DuckDB extensions
	npm install && npx playwright install chromium
	$(MAKE) duckdb-ext

duckdb-ext:    ## install httpfs + spatial locally (baked into the image in CI)
	DUCKDB_EXTENSION_DIR=$$HOME/.duckdb/extensions go run ./cmd/pipeline -install-extensions

verify:        ## seed, boot, drive all three pages, write .verify/latest/summary.md
	scripts/verify/run.sh

verify-seed:   ## rebuild $(LAKE) from testdata fixtures only
	scripts/verify/seed-lake.sh

verify-portal: ## serve $(LAKE) on :8099 (pair with verify-stop)
	scripts/verify/start-portal.sh

verify-stop:   ## tear down the :8099 portal by pidfile
	scripts/verify/stop-portal.sh

verify-static: ## node --check every module + resolve every CSS custom property
	node scripts/verify/static-checks.mjs

verify-http:   ## go suite plus live HTTP checks against a running portal
	scripts/verify/http-checks.sh

verify-browser: ## drive /, /board and /maps in headless chromium
	RUN_DIR=.verify/scratch node scripts/verify/drive.mjs

# ------------------------------------------------------------------- go basics
test:          ## go test -race ./...
	go test -race ./...

lint:          ## golangci-lint (same version CI runs)
	golangci-lint run

ci:            ## the checks CI runs, locally
	go vet ./... && go build ./... && go test -race ./... && $(MAKE) verify

# ---------------------------------------------------------------------- local
# Both read LAKE_DIR. Point it anywhere; /lake is only the container default.

dev-pipeline:  ## one real fetch + all transforms into $(LAKE) (needs network)
	mkdir -p $(LAKE)
	LAKE_DIR=$(LAKE) go run ./cmd/pipeline -once

rebuild:       ## re-derive bronze and gold from existing raw; no network
	LAKE_DIR=$(LAKE) go run ./cmd/pipeline -rebuild

dev-portal:    ## serve $(LAKE) on :8080
	LAKE_DIR=$(LAKE) PORTAL_ADDR=:8080 go run ./cmd/portal

clean:         ## remove verify artifacts; never touches a real lake
	scripts/verify/stop-portal.sh || true
	rm -rf .verify
