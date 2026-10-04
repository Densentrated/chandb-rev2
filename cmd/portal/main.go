// Command portal serves the chandb data viewer.
//
// This is a walking skeleton. It exists to prove the deploy pipeline end to
// end — image build, GHCR push, rsync, compose up, the health gate, and
// Cloudflare Tunnel ingress — without the DuckDB query path. The lifecycle
// bits (flag handling, timeouts, graceful shutdown, the /healthz contract)
// are the parts worth keeping; replace the handlers.
//
// Two things here are load-bearing for the deployment and must survive:
//
//   - The -healthcheck flag. The runtime image has no curl, the container is
//     read_only, and no port is published, so `docker inspect` health is the
//     only signal the deploy gate can read. The compose healthcheck runs this
//     binary against itself.
//   - Binding to PORTAL_ADDR. deploy/docker-compose.yml sets it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	defaultAddr    = ":8080"
	defaultLakeDir = "/lake"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false,
		"probe the local instance over HTTP and exit 0 if healthy")
	flag.Parse()

	if *healthcheck {
		os.Exit(runHealthcheck())
	}

	if err := run(); err != nil {
		slog.Error("portal exited", "err", err)
		os.Exit(1)
	}
}

// listenAddr is what the server binds. Container sets PORTAL_ADDR.
func listenAddr() string {
	if a := os.Getenv("PORTAL_ADDR"); a != "" {
		return a
	}
	return defaultAddr
}

func lakeDir() string {
	if d := os.Getenv("LAKE_DIR"); d != "" {
		return d
	}
	return defaultLakeDir
}

// healthURL derives the self-probe URL from the listen address. A wildcard
// bind (":8080", "0.0.0.0:8080") is not dialable as-is, so it becomes
// loopback — the probe always runs inside the same network namespace.
func healthURL() string {
	host, port, err := net.SplitHostPort(listenAddr())
	if err != nil {
		host, port = "", "8080"
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz"
}

func runHealthcheck() int {
	client := &http.Client{Timeout: 2 * time.Second}

	resp, err := client.Get(healthURL())
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}

func run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /", handleIndex(lakeDir()))

	srv := &http.Server{
		Addr:    listenAddr(),
		Handler: mux,
		// Modest and explicit. The architecture plan wants a low header cap
		// and a short query timeout so we generate errors rather than letting
		// Cloudflare time out at 524.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("portal listening", "addr", srv.Addr, "lake", lakeDir())
		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintln(w, "ok")
}

// handleIndex lists the lake root. This is a deliberate smoke test rather
// than a feature: seeing the directory contents over the tunnel proves the
// :ro bind mount works and that uid 10001 can read it, which is the part of
// the host/container UID alignment most likely to be wrong.
//
// Responses are text/plain on purpose. Rendering untrusted filenames as HTML
// is the exact XSS surface the architecture plan calls out; the real portal
// has to solve that properly, and a scaffold should not pretend to.
func handleIndex(dir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")

		var b strings.Builder
		b.WriteString("chandb portal — scaffold\n\n")
		fmt.Fprintf(&b, "lake: %s\n", dir)

		names, err := listDir(dir)
		switch {
		case err != nil:
			fmt.Fprintf(&b, "status: unreadable (%v)\n", err)
		case len(names) == 0:
			b.WriteString("status: readable, empty\n")
		default:
			fmt.Fprintf(&b, "status: readable, %d entries\n\n", len(names))
			for _, n := range names {
				fmt.Fprintf(&b, "  %s\n", n)
			}
		}

		_, _ = w.Write([]byte(b.String()))
	}
}

func listDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}
