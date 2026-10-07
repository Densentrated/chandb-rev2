// Command portal serves the chandb data viewer.
//
// The query engine runs in the visitor's browser, not here. This process is a
// static file server with a directory listing: it hands out Parquet over HTTP
// range requests and duckdb-wasm does the work client-side.
//
// That is a deliberate security choice. The architecture plan's largest
// section exists because a server-side DuckDB behind a public, unauthenticated
// endpoint has to survive read_blob('/proc/self/environ'), ATTACH, COPY TO,
// INSTALL httpfs, stacked statements, and an unauthenticated disk-fill via the
// temp spill. Moving the engine into the browser deletes that entire threat
// model: there is no server-side query to inject into, and the worst a hostile
// query can do is crash the tab it runs in.
//
// What stays load-bearing here:
//
//   - The -healthcheck flag. No curl in the image, read_only container, no
//     published port, so `docker inspect` health is the deploy gate's only
//     signal.
//   - Serving ONLY the gold subtree. Raw and bronze are not exposed.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	defaultAddr    = ":8080"
	defaultLakeDir = "/lake"
)

//go:embed index.html
var indexHTML []byte

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
	goldDir := path.Join(lakeDir(), "gold")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /api/datasets", handleDatasets(goldDir))
	mux.Handle("GET /data/", http.StripPrefix("/data/", dataServer(goldDir)))
	mux.HandleFunc("GET /", handleIndex)

	srv := &http.Server{
		Addr:    listenAddr(),
		Handler: mux,
		// Short header read and a low header cap: the architecture plan wants
		// us generating errors rather than letting Cloudflare time out at 524.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      120 * time.Second, // Parquet range reads can be large
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		slog.Info("portal listening", "addr", srv.Addr, "gold", goldDir)
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

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	_, _ = w.Write(indexHTML)
}

// dataServer exposes the gold tree, and only the gold tree. http.Dir rejects
// paths containing "..", so a crafted URL cannot climb out into /lake/raw or
// the secrets mount.
//
// http.FileServer handles Range requests natively, which is the whole premise
// of the browser-side engine: duckdb-wasm fetches only the row groups and
// column chunks a query actually touches instead of the whole file.
func dataServer(goldDir string) http.Handler {
	fileSrv := http.FileServer(http.Dir(goldDir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Past partitions never change once written, so they can be cached
		// hard. Today's is still being rebuilt every ingest cycle.
		today := "dt=" + time.Now().UTC().Format("2006-01-02")
		if strings.Contains(r.URL.Path, today) {
			w.Header().Set("Cache-Control", "public, max-age=60")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=86400, immutable")
		}
		w.Header().Set("Access-Control-Allow-Origin", "*")
		fileSrv.ServeHTTP(w, r)
	})
}

type dataset struct {
	Name  string   `json:"name"`
	Files []string `json:"files"`
	Bytes int64    `json:"bytes"`
}

// handleDatasets tells the browser what exists, so the page can build a view
// over every partition without guessing at filenames.
func handleDatasets(goldDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sets := map[string]*dataset{}

		err := fs.WalkDir(os.DirFS(goldDir), ".",
			func(p string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
					return nil //nolint:nilerr // a missing gold tree is not fatal
				}
				// gold/<agency>/<table>/dt=.../part-0.parquet
				parts := strings.Split(p, "/")
				if len(parts) < 3 {
					return nil
				}
				name := parts[0] + "_" + parts[1]

				ds, ok := sets[name]
				if !ok {
					ds = &dataset{Name: name}
					sets[name] = ds
				}
				ds.Files = append(ds.Files, "/data/"+p)
				if info, err := d.Info(); err == nil {
					ds.Bytes += info.Size()
				}
				return nil
			})
		if err != nil {
			slog.Warn("dataset walk failed", "err", err)
		}

		out := make([]dataset, 0, len(sets))
		for _, ds := range sets {
			sort.Strings(ds.Files)
			out = append(out, *ds)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "public, max-age=60")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			slog.Warn("encode datasets failed", "err", err)
		}
	}
}
