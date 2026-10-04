package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testTime = time.Date(2026, 10, 4, 18, 30, 0, 0, time.UTC)

func TestSnapshotPathIsHivePartitioned(t *testing.T) {
	dir, file := snapshotPath("/lake",
		feed{agency: "mbta", name: "vehicle_positions"}, testTime)

	wantDir := "/lake/raw/mbta/vehicle_positions/dt=2026-10-04"
	if dir != wantDir {
		t.Errorf("dir = %q, want %q", dir, wantDir)
	}
	if file != "20261004T183000Z.pb" {
		t.Errorf("file = %q, want %q", file, "20261004T183000Z.pb")
	}
}

func TestSnapshotPathNormalisesToUTC(t *testing.T) {
	// A non-UTC clock must not land the snapshot in the wrong dt= partition.
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("tzdata unavailable")
	}
	dir, _ := snapshotPath("/lake", feed{agency: "a", name: "b"},
		testTime.In(loc))

	if !strings.HasSuffix(dir, "dt=2026-10-04") {
		t.Errorf("dir = %q, want it to end with dt=2026-10-04", dir)
	}
}

func TestFetchFeedWritesBodyVerbatim(t *testing.T) {
	body := []byte{0x0a, 0x03, 0x32, 0x2e, 0x30} // protobuf-ish bytes
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if got := r.Header.Get("User-Agent"); got != userAgent {
				t.Errorf("User-Agent = %q, want %q", got, userAgent)
			}
			_, _ = w.Write(body)
		}))
	defer srv.Close()

	lake := t.TempDir()
	f := feed{agency: "mbta", name: "vehicle_positions", url: srv.URL}

	path, n, err := fetchFeed(context.Background(), srv.Client(),
		srv.URL, lake, f, testTime)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(body)) {
		t.Errorf("wrote %d bytes, want %d", n, len(body))
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Errorf("content = %v, want %v", got, body)
	}
}

// A crashed fetch must not leave a .partial file behind that a later transform
// could mistake for a real snapshot.
func TestFetchFeedLeavesNoPartialOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
	defer srv.Close()

	lake := t.TempDir()
	f := feed{agency: "bart", name: "trip_updates", url: srv.URL}

	if _, _, err := fetchFeed(context.Background(), srv.Client(),
		srv.URL, lake, f, testTime); err == nil {
		t.Fatal("expected an error on HTTP 500")
	}

	matches, _ := filepath.Glob(filepath.Join(lake, "raw", "**", "*.partial-*"))
	if len(matches) != 0 {
		t.Errorf("left partial files behind: %v", matches)
	}
}

func TestFetchFeedRejectsEmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(
		func(_ http.ResponseWriter, _ *http.Request) {}))
	defer srv.Close()

	_, _, err := fetchFeed(context.Background(), srv.Client(), srv.URL,
		t.TempDir(), feed{agency: "a", name: "b"}, testTime)
	if err == nil {
		t.Fatal("expected an error on an empty body")
	}
}

func TestReadSecretTrimsAndRejectsEmpty(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SECRETS_DIR", dir)

	if err := os.WriteFile(filepath.Join(dir, "good"),
		[]byte("  abc123\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "blank"),
		[]byte("\n  \n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if got, err := readSecret("good"); err != nil || got != "abc123" {
		t.Errorf("readSecret(good) = %q, %v; want %q, nil", got, err, "abc123")
	}
	if _, err := readSecret("blank"); err == nil {
		t.Error("readSecret(blank) should reject a whitespace-only secret")
	}
	if _, err := readSecret("missing"); err == nil {
		t.Error("readSecret(missing) should error")
	}
}

// Snapshots should land on predictable wall-clock boundaries (:00 and :30 for
// a 30m interval) regardless of when the container started, so a restart
// doesn't permanently shift the sampling offset.
func TestUntilNextAlignsToWallClock(t *testing.T) {
	half := 30 * time.Minute
	cases := []struct {
		now  string
		want time.Duration
	}{
		{"2026-10-04T18:00:00Z", 30 * time.Minute},
		{"2026-10-04T18:07:00Z", 23 * time.Minute},
		{"2026-10-04T18:29:59Z", time.Second},
		{"2026-10-04T18:30:00Z", 30 * time.Minute},
		{"2026-10-04T18:31:00Z", 29 * time.Minute},
	}
	for _, c := range cases {
		now, err := time.Parse(time.RFC3339, c.now)
		if err != nil {
			t.Fatal(err)
		}
		if got := untilNext(now, half); got != c.want {
			t.Errorf("untilNext(%s) = %v, want %v", c.now, got, c.want)
		}
	}
}

func TestUntilNextHandlesZeroInterval(t *testing.T) {
	if got := untilNext(time.Now(), 0); got != defaultInterval {
		t.Errorf("untilNext with 0 = %v, want %v", got, defaultInterval)
	}
}

func TestIntervalFallsBackOnBadInput(t *testing.T) {
	for _, v := range []string{"", "nonsense", "-5m", "0"} {
		t.Setenv("INGEST_INTERVAL", v)
		if got := interval(); got != defaultInterval {
			t.Errorf("INGEST_INTERVAL=%q gave %v, want %v",
				v, got, defaultInterval)
		}
	}
	t.Setenv("INGEST_INTERVAL", "5m")
	if got := interval(); got != 5*time.Minute {
		t.Errorf("INGEST_INTERVAL=5m gave %v, want 5m", got)
	}
}

// Every feed must be reachable from a bare URL or declare the secret it needs.
func TestFeedTableIsWellFormed(t *testing.T) {
	seen := map[string]bool{}
	for _, f := range feeds {
		key := f.agency + "/" + f.name
		if seen[key] {
			t.Errorf("duplicate feed %s", key)
		}
		seen[key] = true

		if !strings.HasPrefix(f.url, "https://") {
			t.Errorf("%s: url must be https, got %q", key, f.url)
		}
		if strings.Contains(f.url, "{key}") && f.secret == "" {
			t.Errorf("%s: url has a {key} placeholder but no secret", key)
		}
		if f.secret != "" && !strings.Contains(f.url, "{key}") {
			t.Errorf("%s: declares secret %q but url has no {key}",
				key, f.secret)
		}
	}
}
