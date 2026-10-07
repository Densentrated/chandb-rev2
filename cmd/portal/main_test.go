package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHealthURLRewritesWildcardBinds(t *testing.T) {
	cases := map[string]string{
		":8080":         "http://127.0.0.1:8080/healthz",
		"0.0.0.0:8080":  "http://127.0.0.1:8080/healthz",
		"127.0.0.1:900": "http://127.0.0.1:900/healthz",
		"garbage":       "http://127.0.0.1:8080/healthz",
	}
	for addr, want := range cases {
		t.Setenv("PORTAL_ADDR", addr)
		if got := healthURL(); got != want {
			t.Errorf("healthURL() with PORTAL_ADDR=%q = %q, want %q", addr, got, want)
		}
	}
}

// The compose healthcheck depends on /healthz returning exactly 200. If this
// breaks, every deploy hangs at the health gate and then rolls back.
func TestHealthzReturns200(t *testing.T) {
	rec := httptest.NewRecorder()
	handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "ok" {
		t.Errorf("body = %q, want %q", got, "ok")
	}
}

func TestIndexServesHTML(t *testing.T) {
	rec := httptest.NewRecorder()
	handleIndex(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content-type = %q, want text/html", ct)
	}
	// The engine setup lives in the shared module now, not inline.
	if !strings.Contains(rec.Body.String(), "/chandb.js") {
		t.Error("page should import the shared runtime")
	}
}

func TestBoardPageIsServed(t *testing.T) {
	rec := httptest.NewRecorder()
	servePage(boardHTML, "text/html; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/board", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "/chandb.js") {
		t.Error("board should import the shared runtime")
	}
	if !strings.Contains(body, "stop_events") {
		t.Error("board should query a stop_events table")
	}
}

// A module delivered with the wrong Content-Type is refused by every browser,
// and both pages are ES modules importing this one.
func TestSharedRuntimeServedAsJavaScript(t *testing.T) {
	rec := httptest.NewRecorder()
	servePage(chandbJS, "text/javascript; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/chandb.js", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/javascript") {
		t.Errorf("content-type = %q, want text/javascript", ct)
	}
	for _, want := range []string{"duckdb-wasm", "export async function boot", "fmtEta"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("shared runtime missing %q", want)
		}
	}
}

func TestIndexDoesNotSwallowUnknownPaths(t *testing.T) {
	rec := httptest.NewRecorder()
	handleIndex(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func writeGold(t *testing.T, gold, rel string, body string) {
	t.Helper()
	p := filepath.Join(gold, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDatasetsGroupsByAgencyAndTable(t *testing.T) {
	gold := t.TempDir()
	writeGold(t, gold, "mbta/vehicle_positions/dt=2026-10-06/part-0.parquet", "aaa")
	writeGold(t, gold, "mbta/vehicle_positions/dt=2026-10-07/part-0.parquet", "bb")
	writeGold(t, gold, "bart/trip_updates/dt=2026-10-07/part-0.parquet", "c")
	writeGold(t, gold, "mbta/vehicle_positions/dt=2026-10-07/notes.txt", "ignored")

	rec := httptest.NewRecorder()
	handleDatasets(gold)(rec, httptest.NewRequest(http.MethodGet, "/api/datasets", nil))

	var sets []dataset
	if err := json.Unmarshal(rec.Body.Bytes(), &sets); err != nil {
		t.Fatal(err)
	}
	if len(sets) != 2 {
		t.Fatalf("got %d datasets, want 2: %+v", len(sets), sets)
	}

	byName := map[string]dataset{}
	for _, s := range sets {
		byName[s.Name] = s
	}

	vp, ok := byName["mbta_vehicle_positions"]
	if !ok {
		t.Fatalf("missing mbta_vehicle_positions: %+v", sets)
	}
	if len(vp.Files) != 2 {
		t.Errorf("got %d files, want 2 (non-parquet must be ignored)", len(vp.Files))
	}
	if vp.Bytes != 5 {
		t.Errorf("bytes = %d, want 5", vp.Bytes)
	}
	// Sorted so partitions line up chronologically for the browser's view.
	if !strings.Contains(vp.Files[0], "dt=2026-10-06") {
		t.Errorf("files not sorted: %v", vp.Files)
	}
	for _, f := range vp.Files {
		if !strings.HasPrefix(f, "/data/") {
			t.Errorf("file %q must be served under /data/", f)
		}
	}
}

// A missing gold tree means "nothing published yet", not an error page.
func TestDatasetsEmptyGoldReturnsEmptyList(t *testing.T) {
	rec := httptest.NewRecorder()
	handleDatasets(filepath.Join(t.TempDir(), "absent"))(
		rec, httptest.NewRequest(http.MethodGet, "/api/datasets", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var sets []dataset
	if err := json.Unmarshal(rec.Body.Bytes(), &sets); err != nil {
		t.Fatalf("body is not valid JSON: %v", err)
	}
	if len(sets) != 0 {
		t.Errorf("got %d datasets, want 0", len(sets))
	}
}

func TestDataServerServesGoldFiles(t *testing.T) {
	gold := t.TempDir()
	writeGold(t, gold, "mbta/vehicle_positions/dt=2026-10-07/part-0.parquet", "PAR1data")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/mbta/vehicle_positions/dt=2026-10-07/part-0.parquet", nil)
	dataServer(gold).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "PAR1data" {
		t.Errorf("body = %q", rec.Body.String())
	}
}

// Range support is the entire premise of the browser-side engine: without it
// duckdb-wasm downloads whole files instead of the row groups it needs.
func TestDataServerSupportsRangeRequests(t *testing.T) {
	gold := t.TempDir()
	writeGold(t, gold, "mbta/vehicle_positions/dt=2026-10-07/part-0.parquet",
		"0123456789")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/mbta/vehicle_positions/dt=2026-10-07/part-0.parquet", nil)
	req.Header.Set("Range", "bytes=2-5")
	dataServer(gold).ServeHTTP(rec, req)

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rec.Code)
	}
	if rec.Body.String() != "2345" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "2345")
	}
}

// Only the gold subtree is public. Raw snapshots and the secrets mount must
// stay unreachable no matter what path is requested.
func TestDataServerRejectsTraversal(t *testing.T) {
	gold := t.TempDir()
	lake := filepath.Dir(gold)
	if err := os.WriteFile(filepath.Join(lake, "secret.txt"), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{
		"../secret.txt",
		"../../etc/passwd",
		"mbta/../../secret.txt",
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.URL.Path = p
		dataServer(gold).ServeHTTP(rec, req)

		if rec.Code == http.StatusOK && strings.Contains(rec.Body.String(), "nope") {
			t.Errorf("path %q escaped the gold directory", p)
		}
	}
}

func TestDataServerCachesPastPartitionsHarder(t *testing.T) {
	gold := t.TempDir()
	writeGold(t, gold, "mbta/vehicle_positions/dt=2020-01-01/part-0.parquet", "old")

	rec := httptest.NewRecorder()
	dataServer(gold).ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/mbta/vehicle_positions/dt=2020-01-01/part-0.parquet", nil))

	if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "immutable") {
		t.Errorf("Cache-Control = %q, want immutable for a finished partition", cc)
	}
}
