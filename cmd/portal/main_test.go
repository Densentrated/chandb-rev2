package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// A page must never be cached independently of the module it imports: that
// skew is what produces a silent SyntaxError and a page stuck on its first
// status line.
func TestPagesAreNeverCachedStale(t *testing.T) {
	a := buildAssets()
	for route, body := range a.pages {
		h := servePage(body, "text/html; charset=utf-8")
		if route == "/" {
			h = handleIndex(body, "text/html; charset=utf-8")
		}
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, route, nil))
		if cc := rec.Header().Get("Cache-Control"); cc != "no-cache" {
			t.Errorf("%s Cache-Control = %q, want no-cache", route, cc)
		}
	}
}

// moduleByPath finds a built module by its plain name, e.g. "/board.js".
func moduleByPath(t *testing.T, a assets, path string) module {
	t.Helper()
	for _, m := range a.modules {
		if m.path == path {
			return m
		}
	}
	t.Fatalf("no module built for %s", path)
	return module{}
}

func TestPagesReferenceTheHashedModules(t *testing.T) {
	a := buildAssets()
	for _, m := range a.modules {
		stem := strings.TrimSuffix(m.path, ".js")
		if !strings.HasPrefix(m.hashedPath, stem+".") ||
			!strings.HasSuffix(m.hashedPath, ".js") ||
			len(m.hashedPath) != len(stem)+16 {
			t.Errorf("hashed path for %s looks wrong: %q", m.path, m.hashedPath)
		}
	}

	shared := moduleByPath(t, a, "/chandb.js")
	for route, pageModule := range map[string]string{
		"/": "/index.js", "/board": "/board.js", "/maps": "/maps.js",
		"/broadband": "/broadband.js",
	} {
		page := string(a.pages[route])
		own := moduleByPath(t, a, pageModule)
		if !strings.Contains(page, own.hashedPath) {
			t.Errorf("%s does not load the hashed %s", route, pageModule)
		}
		// The page references its own module; that module is what imports the
		// shared runtime, so the hashed chandb path must appear there.
		if !strings.Contains(string(own.body), shared.hashedPath) {
			t.Errorf("%s does not import the hashed shared runtime", pageModule)
		}
		if strings.Contains(string(own.body), "'/chandb.js'") {
			t.Errorf("%s still imports the unhashed shared runtime", pageModule)
		}
	}
}

// Changing a module must change its URL, or caches keep the old bytes. It must
// also change every dependent's URL: a page module that inlines the shared
// runtime's hashed path has new content the moment that path moves.
func TestHashedPathTracksContent(t *testing.T) {
	first := buildAssets()

	orig := chandbJS
	chandbJS = append(append([]byte{}, orig...), []byte("\n// changed\n")...)
	changed := buildAssets()
	chandbJS = orig
	restored := buildAssets()

	for _, path := range []string{
		"/chandb.js", "/index.js", "/board.js", "/maps.js", "/broadband.js",
	} {
		a, b := moduleByPath(t, first, path), moduleByPath(t, changed, path)
		if a.hashedPath == b.hashedPath {
			t.Errorf("%s: editing the shared runtime did not change its URL", path)
		}
		if c := moduleByPath(t, restored, path); c.hashedPath != a.hashedPath {
			t.Errorf("%s: restoring the content did not restore the URL", path)
		}
	}
}

func TestHashedModuleIsImmutable(t *testing.T) {
	a := buildAssets()
	for _, m := range a.modules {
		rec := httptest.NewRecorder()
		serveImmutable(m.body, jsType)(
			rec, httptest.NewRequest(http.MethodGet, m.hashedPath, nil))

		if cc := rec.Header().Get("Cache-Control"); !strings.Contains(cc, "immutable") {
			t.Errorf("%s Cache-Control = %q, want immutable", m.hashedPath, cc)
		}
	}
}

func TestIndexServesHTML(t *testing.T) {
	a := buildAssets()
	rec := httptest.NewRecorder()
	handleIndex(a.pages["/"], "text/html; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("content-type = %q, want text/html", ct)
	}
	if !strings.Contains(rec.Body.String(), moduleByPath(t, a, "/index.js").hashedPath) {
		t.Error("page should load the hashed page module")
	}
}

func TestBoardPageIsServed(t *testing.T) {
	a := buildAssets()
	rec := httptest.NewRecorder()
	servePage(a.pages["/board"], "text/html; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/board", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// Markup lives in the page; behaviour lives in the module. Assert each
	// against the artifact that actually carries it.
	body := rec.Body.String()
	for _, want := range []string{"timeline", "Back to board", "Filter stations"} {
		if !strings.Contains(body, want) {
			t.Errorf("board markup missing %q", want)
		}
	}
	js := string(moduleByPath(t, a, "/board.js").body)
	for _, want := range []string{"stop_events", "Show all departures", "departure_delay"} {
		if !strings.Contains(js, want) {
			t.Errorf("board module missing %q", want)
		}
	}
}

func TestMapsPageIsServed(t *testing.T) {
	a := buildAssets()
	rec := httptest.NewRecorder()
	servePage(a.pages["/maps"], "text/html; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/maps", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"map-world", "map-boston", "map-bay", "map-philadelphia",
		"24-hour activity", "San Francisco Bay Area", "dark grid",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("maps markup missing %q", want)
		}
	}
	js := string(moduleByPath(t, a, "/maps.js").body)
	for _, want := range []string{"activity cells", "canvas", "vehicle_positions"} {
		if !strings.Contains(js, want) {
			t.Errorf("maps module missing %q", want)
		}
	}
}

// The broadband page's whole reason to exist is the distinction between "slow"
// and "not measured", and that distinction lives in prose and class names that
// nothing else would catch if they were edited away. Assert the markup carries
// the caveat and the no-data treatment, and the module carries the honesty
// machinery: evidence tiers, the unmeasured branch, and the graceful-degrade
// hooks for the FCC and ACS tables that do not exist yet.
func TestBroadbandPageIsServed(t *testing.T) {
	a := buildAssets()
	rec := httptest.NewRecorder()
	servePage(a.pages["/broadband"], "text/html; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/broadband", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"ledger", "tile-unmeasured", "dist-unmeasured", "chart-evidence",
		"tracts-boston", "tracts-bayarea", "table-caption", "unmeasured-list",
		// The caveat must stay ABOVE the data and must stay worded as a
		// warning; a reader who stops after the first chart has still read it.
		"Read this before the numbers",
		"A measured speed is not an availability map",
		"No tests means unmeasured, never unserved",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("broadband markup missing %q", want)
		}
	}
	// Nav consistency: every page offers every other page.
	for _, page := range []string{"/", "/board", "/maps", "/broadband"} {
		for _, route := range []string{`href="/"`, `href="/board"`, `href="/maps"`, `href="/broadband"`} {
			if !strings.Contains(string(a.pages[page]), route) {
				t.Errorf("page %s nav is missing %s", page, route)
			}
		}
	}

	js := string(moduleByPath(t, a, "/broadband.js").body)
	for _, want := range []string{
		"broadband_tract_coverage", "broadband_tract_measured",
		"unmeasured", "tierOf", "share_tests_over_100",
		// Declared but absent upstream. These must stay listed so the columns
		// appear the day the credentials land, and must stay OPTIONAL so the
		// page keeps rendering until then.
		"broadband_tract_claimed", "broadband_tract_demographics",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("broadband module missing %q", want)
		}
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
	handleIndex(buildAssets().pages["/"], "text/html; charset=utf-8")(
		rec, httptest.NewRequest(http.MethodGet, "/nope", nil))
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
	newCatalog(gold).serveHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/datasets", nil))

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
	newCatalog(filepath.Join(t.TempDir(), "absent")).serveHTTP(
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

func TestCatalogCachesAndSupportsETag(t *testing.T) {
	gold := t.TempDir()
	writeGold(t, gold, "mbta/vehicle_positions/dt=2026-10-07/part-0.parquet", "one")
	cache := newCatalog(gold)
	cache.refreshAfter = time.Hour

	first := httptest.NewRecorder()
	cache.serveHTTP(first, httptest.NewRequest(http.MethodGet, "/api/datasets", nil))
	etag := first.Header().Get("ETag")
	if etag == "" {
		t.Fatal("catalog did not publish an ETag")
	}
	writeGold(t, gold, "bart/trip_updates/dt=2026-10-07/part-0.parquet", "two")

	cached := httptest.NewRecorder()
	cache.serveHTTP(cached, httptest.NewRequest(http.MethodGet, "/api/datasets", nil))
	if strings.Contains(cached.Body.String(), "bart_trip_updates") {
		t.Fatal("catalog rescanned before its refresh interval")
	}

	notModified := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/datasets", nil)
	req.Header.Set("If-None-Match", etag)
	cache.serveHTTP(notModified, req)
	if notModified.Code != http.StatusNotModified {
		t.Fatalf("status = %d, want 304", notModified.Code)
	}

	cache.refreshAfter = -time.Second
	cache.expires = time.Time{}
	refreshed := httptest.NewRecorder()
	cache.serveHTTP(refreshed, httptest.NewRequest(http.MethodGet, "/api/datasets", nil))
	if !strings.Contains(refreshed.Body.String(), "bart_trip_updates") {
		t.Fatal("expired catalog did not refresh")
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
