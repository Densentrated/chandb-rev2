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
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultAddr    = ":8080"
	defaultLakeDir = "/lake"
)

//go:embed index.html
var indexHTML []byte

//go:embed board.html
var boardHTML []byte

// Shared browser runtime. Both pages import it, so the DuckDB-WASM setup and
// dataset registration exist once rather than per page.
//
//go:embed chandb.js
var chandbJS []byte

//go:embed portal.css
var portalCSS []byte

// Content-hashed asset URL, e.g. /chandb.3f9a1c2b4d5e.js
//
// The pages and the module they import are separate HTTP responses with
// separate cache lifetimes. A browser holding a stale chandb.js while fetching
// a fresh page gets an import/export mismatch — which fails as a SyntaxError
// at PARSE time, so no handler runs and the page hangs on its initial text
// with nothing logged. Hashing the URL makes that impossible: new content is a
// new URL, and the stale copy is simply never requested again.
var (
	hashedJSPath string
	indexPage    []byte
	boardPage    []byte
)

func buildAssets() {
	sum := sha256.Sum256(chandbJS)
	hashedJSPath = "/chandb." + hex.EncodeToString(sum[:])[:12] + ".js"

	ref := []byte("/chandb.js")
	indexPage = bytes.ReplaceAll(indexHTML, ref, []byte(hashedJSPath))
	boardPage = bytes.ReplaceAll(boardHTML, ref, []byte(hashedJSPath))
}

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

	buildAssets()

	const jsType = "text/javascript; charset=utf-8"

	mux := portalRoutes(goldDir, jsType)

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

func portalRoutes(goldDir, jsType string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("GET /board", servePage(boardPage, "text/html; charset=utf-8"))
	mux.HandleFunc("GET "+hashedJSPath, serveImmutable(chandbJS, jsType))
	mux.HandleFunc("GET /chandb.js", servePage(chandbJS, jsType))
	mux.HandleFunc("GET /portal.css", servePage(portalCSS, "text/css; charset=utf-8"))
	mux.HandleFunc("GET /api/datasets", newCatalog(goldDir).serveHTTP)
	mux.Handle("GET /data/", http.StripPrefix("/data/", dataServer(goldDir)))
	mux.HandleFunc("GET /", handleIndex)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintln(w, "ok")
}

// servePage serves HTML and other entry points with revalidation on every
// request. These are a few KB and they are what point at every hashed asset,
// so they must never be stale; a 304 is cheap.
func servePage(body []byte, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(body)
	}
}

// serveImmutable is for content-hashed URLs only: the bytes cannot change
// without the URL changing, so a year is safe.
func serveImmutable(body []byte, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		_, _ = w.Write(body)
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(indexPage)
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
	root, _ := filepath.EvalSymlinks(goldDir)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// http.Dir cleans "..", but a symlink inside gold could otherwise point
		// at raw data or another mounted secret.
		relative := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		target := filepath.Join(goldDir, filepath.FromSlash(relative))
		resolved, err := filepath.EvalSymlinks(target)
		if err != nil || root == "" || (resolved != root && !strings.HasPrefix(resolved, root+string(os.PathSeparator))) {
			http.NotFound(w, r)
			return
		}
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
type catalog struct {
	body []byte
	etag string
}

type catalogCache struct {
	goldDir string
	mu      sync.Mutex
	current catalog
	expires time.Time
	// Refreshes are infrequent because published partitions are immutable.
	refreshAfter time.Duration
}

func newCatalog(goldDir string) *catalogCache {
	return &catalogCache{goldDir: goldDir, refreshAfter: time.Minute}
}

func (c *catalogCache) serveHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	if c.current.body == nil || time.Now().After(c.expires) {
		next, err := scanCatalog(c.goldDir)
		if err != nil {
			slog.Warn("dataset walk failed", "err", err)
		} else {
			c.current = next
		}
		c.expires = time.Now().Add(c.refreshAfter)
	}
	current := c.current
	c.mu.Unlock()

	if current.body == nil {
		current = catalog{body: []byte("[]\n"), etag: `"empty"`}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.Header().Set("ETag", current.etag)
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == current.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	_, _ = w.Write(current.body)
}

func scanCatalog(goldDir string) (catalog, error) {
	sets := map[string]*dataset{}
	err := fs.WalkDir(os.DirFS(goldDir), ".",
		func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(p, ".parquet") {
				return nil
			}
			parts := strings.Split(p, "/")
			if len(parts) < 3 {
				return nil
			}
			name := parts[0] + "_" + parts[1]
			ds := sets[name]
			if ds == nil {
				ds = &dataset{Name: name}
				sets[name] = ds
			}
			ds.Files = append(ds.Files, "/data/"+p)
			if info, infoErr := d.Info(); infoErr == nil {
				ds.Bytes += info.Size()
			}
			return nil
		})
	if os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		return catalog{}, err
	}
	out := make([]dataset, 0, len(sets))
	for _, ds := range sets {
		sort.Strings(ds.Files)
		out = append(out, *ds)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	body, err := json.Marshal(out)
	if err != nil {
		return catalog{}, err
	}
	body = append(body, '\n')
	sum := sha256.Sum256(body)
	return catalog{body: body, etag: `"` + hex.EncodeToString(sum[:]) + `"`}, nil
}

func handleDatasets(goldDir string) http.HandlerFunc {
	return newCatalog(goldDir).serveHTTP
}
