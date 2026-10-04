package main

import (
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
			t.Errorf("healthURL() with PORTAL_ADDR=%q = %q, want %q",
				addr, got, want)
		}
	}
}

// The compose healthcheck depends on /healthz returning exactly 200. If this
// breaks, every deploy hangs at the health gate and then rolls back.
func TestHealthzReturns200(t *testing.T) {
	rec := httptest.NewRecorder()
	handleHealthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != "ok" {
		t.Errorf("body = %q, want %q", got, "ok")
	}
}

func TestIndexListsLakeContents(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "gold"), 0o700); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	handleIndex(dir)(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{"orders.parquet", "gold/", "2 entries"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q:\n%s", want, body)
		}
	}
}

func TestIndexDoesNotSwallowUnknownPaths(t *testing.T) {
	rec := httptest.NewRecorder()
	handleIndex(t.TempDir())(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestIndexReportsUnreadableLake(t *testing.T) {
	rec := httptest.NewRecorder()
	handleIndex(filepath.Join(t.TempDir(), "does-not-exist"))(
		rec, httptest.NewRequest(http.MethodGet, "/", nil))

	// A missing lake must not 500 — the portal should start and say so, or
	// the container never reaches healthy and the deploy gate fails blind.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if !strings.Contains(rec.Body.String(), "unreadable") {
		t.Errorf("body should report unreadable lake:\n%s", rec.Body.String())
	}
}
