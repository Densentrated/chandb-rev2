.PHONY: up down logs ingest sweep du psql metabase reset dev-up dev-down dev-ingest dev-reset tunnel

up:            ## start postgres + metabase
	docker compose up -d postgres metabase

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ingest:        ## run the pipeline once (one-shot job, not a daemon)
	docker compose run --rm pipeline python -m pipeline.main ingest

sweep:         ## retention + compaction + storage accounting
	docker compose run --rm pipeline python -m pipeline.main sweep

du:            ## per-stream disk usage as the pipeline sees it
	docker compose run --rm pipeline python -m pipeline.main du
	@echo "--- host view ---"
	@du -sh ./lake 2>/dev/null || true

psql:
	docker compose exec postgres psql -U postgres -d lakehouse

metabase:      ## open the dashboard
	@echo "http://localhost:3000"

reset:         ## DESTROYS postgres state. The lake on disk is left alone.
	docker compose down -v

runner-setup:  ## one-time host setup for the self-hosted GitHub Actions runner
	mkdir -p $$HOME/.config/lakehouse $$HOME/lakehouse-data
	cp -n .env $$HOME/.config/lakehouse/env 2>/dev/null || true
	chmod 600 $$HOME/.config/lakehouse/env
	@echo "env  -> $$HOME/.config/lakehouse/env"
	@echo "lake -> $$HOME/lakehouse-data"
	@echo "Now add the runner: repo Settings > Actions > Runners > New self-hosted"
	@echo "IMPORTANT: label it 'lakehouse', and keep the repo PRIVATE."

ci:            ## run the same checks CI runs, locally
	ruff check pipeline/ && python -m compileall -q pipeline/ && docker compose config --quiet

# ---------------------------------------------------------------------
# Local dev machine only. Separate project, ports, and lake — never
# touches host data.
# ---------------------------------------------------------------------
DEV := docker compose -p lakehouse-dev -f docker-compose.yml -f docker-compose.dev.yml

dev-up:        ## scratch stack on :3001 with lake-dev/
	$(DEV) up -d postgres metabase
	@echo "metabase (dev): http://localhost:3001"

dev-ingest:    ## run connectors against samples/ into lake-dev/
	$(DEV) run --rm pipeline python -m pipeline.main ingest

dev-down:
	$(DEV) down

dev-reset:     ## wipe the scratch stack AND lake-dev/
	$(DEV) down -v
	rm -rf lake-dev/* && touch lake-dev/.gitkeep

tunnel:        ## reach the host's Metabase from here: make tunnel HOST=user@box
	ssh -N -L 3000:localhost:3000 $(HOST)
