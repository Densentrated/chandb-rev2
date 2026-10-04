# Multi-stage build for the chandb-rev2 portal and pipeline binaries.
# See docs/gitops-plan.md Phase 4.
#
# The Debian base images are correct even though the host is Ubuntu: a container
# ships its own userland, and the host distro constrains nothing but the kernel.
# Don't "fix" these to ubuntu:24.04 — the build and runtime images need to match
# each other for the cgo/glibc reasoning to hold, and they do.

FROM golang:1.24-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=1 is pinned explicitly. Cross-compiling silently disables cgo and
# the symptom is a baffling `undefined: conn` from the DuckDB driver.
ENV CGO_ENABLED=1 GOOS=linux GOARCH=amd64
RUN go build -trimpath -ldflags="-s -w" -o /out/portal   ./cmd/portal \
 && go build -trimpath -ldflags="-s -w" -o /out/pipeline ./cmd/pipeline

FROM debian:bookworm-slim

# Links the GHCR package to the repo, so the image shows up on the repository
# page and inherits its access settings.
LABEL org.opencontainers.image.source="https://github.com/Densentrated/chandb-rev2"

# ca-certificates for API ingest over TLS.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# UID/GID 10001 must match infra/ansible/playbook.yml, which owns /lake to the
# same numbers on the host. Bind mounts map by number, not by name.
RUN groupadd -g 10001 portal \
 && useradd -u 10001 -g 10001 -M -s /usr/sbin/nologin portal \
 && install -d -o 10001 -g 10001 -m 0700 /var/tmp/duckdb-spill

COPY --from=build /out/portal   /usr/local/bin/portal
COPY --from=build /out/pipeline /usr/local/bin/pipeline

USER 10001:10001
ENTRYPOINT ["/usr/local/bin/portal"]
