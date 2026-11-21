package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A quadkey prefix is only a valid filter if the tiles we care about actually
// sit underneath it. These are real coordinates: if the covering set stops
// containing downtown Boston or downtown San Francisco, the Ookla extract
// silently returns a subset of the metro and every downstream number is quietly
// wrong — with no error anywhere.
func TestCoveringQuadkeysContainKnownLandmarks(t *testing.T) {
	cases := []struct {
		region   string
		name     string
		lon, lat float64
	}{
		{"boston", "Boston Common", -71.0589, 42.3601},
		{"boston", "Logan Airport", -71.0096, 42.3656},
		{"bayarea", "San Francisco", -122.4194, 37.7749},
		{"bayarea", "Oakland", -122.2712, 37.8044},
		{"bayarea", "San Jose", -121.8863, 37.3382},
		{"bayarea", "Santa Rosa", -122.7141, 38.4404},
	}

	for _, c := range cases {
		var r region
		for _, cand := range regions {
			if cand.id == c.region {
				r = cand
			}
		}
		if r.id == "" {
			t.Fatalf("no region %q configured", c.region)
		}

		x, y := lonLatToTile(c.lon, c.lat, 16)
		qk := quadkeyOf(x, y, 16)

		var covered bool
		for _, prefix := range r.coveringQuadkeys(quadkeyZoom) {
			if strings.HasPrefix(qk, prefix) {
				covered = true
				break
			}
		}
		if !covered {
			t.Errorf("%s (%s): z16 quadkey %s is not under any %s prefix %v",
				c.name, c.region, qk, r.id, r.coveringQuadkeys(quadkeyZoom))
		}
	}
}

// A quadkey is base-4 and exactly one character per zoom level. A digit outside
// 0-3 means the bit packing is wrong, and the LIKE filter would match nothing.
func TestQuadkeyShape(t *testing.T) {
	for _, qk := range allCoveringQuadkeys(quadkeyZoom) {
		if len(qk) != quadkeyZoom {
			t.Errorf("quadkey %q has length %d, want %d", qk, len(qk), quadkeyZoom)
		}
		if strings.Trim(qk, "0123") != "" {
			t.Errorf("quadkey %q contains a non-base-4 digit", qk)
		}
	}
}

func TestPreviousQuarterWrapsTheYear(t *testing.T) {
	for _, c := range []struct{ y, q, wantY, wantQ int }{
		{2026, 1, 2025, 4},
		{2026, 2, 2026, 1},
		{2026, 4, 2026, 3},
	} {
		y, q := previousQuarter(c.y, c.q)
		if y != c.wantY || q != c.wantQ {
			t.Errorf("previousQuarter(%d,%d) = %d,%d want %d,%d",
				c.y, c.q, y, q, c.wantY, c.wantQ)
		}
	}
}

// The quarter's file is published well after the quarter starts, so asking for
// the current one is a guaranteed 404.
func TestOoklaURLTargetsAPublishedQuarter(t *testing.T) {
	at := time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC) // Q3
	y, q := previousQuarter(quarterOf(at))
	url := ooklaQuarterURL(y, q)
	if !strings.Contains(url, "year=2026") || !strings.Contains(url, "quarter=2") {
		t.Errorf("August 2026 should target 2026 Q2, got %s", url)
	}
	if !strings.Contains(url, "2026-04-01_performance_fixed_tiles.parquet") {
		t.Errorf("filename should carry the quarter start date, got %s", url)
	}
}

// Credentials belong in headers, never in a URL that gets logged on failure.
func TestFCCCredentialsAreNotInTheURL(t *testing.T) {
	for _, f := range feeds {
		if f.agency != "fcc" {
			continue
		}
		if strings.Contains(f.url, "{key}") || f.secret != "" {
			t.Errorf("%s/%s puts a credential in its URL", f.agency, f.name)
		}
		if len(f.auth) == 0 {
			t.Errorf("%s/%s has no auth headers configured", f.agency, f.name)
		}
	}
}

// A feed whose credential is missing must be SKIPPED, not attempted. Sending
// an unauthenticated request would store a 401 body as though it were data.
func TestResolveAuthFailsClosedWithoutSecrets(t *testing.T) {
	t.Setenv("SECRETS_DIR", t.TempDir())

	f := feed{auth: []authHeader{{header: "username", secret: "fcc_username"}}}
	if _, err := resolveAuth(f); err == nil {
		t.Fatal("resolveAuth should fail when the secret file is absent")
	}
	if _, err := resolveAuth(feed{}); err != nil {
		t.Errorf("a feed with no auth should resolve cleanly, got %v", err)
	}
}

func TestResolveAuthReadsEveryHeader(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SECRETS_DIR", dir)
	for name, body := range map[string]string{
		"fcc_username":   "someone@example.com\n",
		"fcc_hash_value": "abc123\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := resolveAuth(feed{auth: []authHeader{
		{header: "username", secret: "fcc_username"},
		{header: "hash_value", secret: "fcc_hash_value"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// Trailing newlines from `echo >` must not reach the header value.
	if got["username"] != "someone@example.com" || got["hash_value"] != "abc123" {
		t.Errorf("credentials not trimmed: %#v", got)
	}
}

// lastSnapshotAt drives the `every` cadence. Reading the timestamp out of the
// FILENAME rather than the mtime is deliberate: a restore, an rsync or a docker
// volume copy rewrites mtimes and would otherwise make every archived snapshot
// look like it arrived just now, suppressing all future fetches.
func TestLastSnapshotAtReadsTheFilename(t *testing.T) {
	lake := t.TempDir()
	f := feed{agency: "ookla", name: "fixed_performance", ext: "parquet"}

	if _, ok := lastSnapshotAt(lake, f); ok {
		t.Fatal("an empty lake should report no snapshot")
	}

	for _, stamp := range []string{"20260101T000000Z", "20260715T120000Z", "20260301T000000Z"} {
		dir := filepath.Join(lake, "raw", "ookla", "fixed_performance",
			"dt="+stamp[:4]+"-"+stamp[4:6]+"-"+stamp[6:8])
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, stamp+".parquet"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	got, ok := lastSnapshotAt(lake, f)
	if !ok {
		t.Fatal("expected a snapshot")
	}
	want := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("newest = %s, want %s", got, want)
	}
}

// Dedupe is what keeps a weekly poll of a twice-yearly dataset from storing
// fifty identical copies.
func TestSameAsNewestDetectsIdenticalBytes(t *testing.T) {
	lake := t.TempDir()
	f := feed{agency: "census", name: "acs5_boston", ext: "json"}

	dir := filepath.Join(lake, "raw", "census", "acs5_boston", "dt=2026-01-01")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	body := []byte(`[["NAME","B01003_001E"],["Tract 1","4210"]]`)
	if err := os.WriteFile(filepath.Join(dir, "20260101T000000Z.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}

	same := filepath.Join(t.TempDir(), "same.json")
	if err := os.WriteFile(same, body, 0o644); err != nil {
		t.Fatal(err)
	}
	if !sameAsNewest(lake, f, same) {
		t.Error("identical bytes should be detected as unchanged")
	}

	// A one-character change must NOT be deduplicated away. Same length, so
	// this also proves the size shortcut is not the whole comparison.
	diff := filepath.Join(t.TempDir(), "diff.json")
	changed := append([]byte{}, body...)
	changed[len(changed)-3] = '9'
	if err := os.WriteFile(diff, changed, 0o644); err != nil {
		t.Fatal(err)
	}
	if sameAsNewest(lake, f, diff) {
		t.Error("changed bytes of equal length were wrongly deduplicated")
	}
}

// Every broadband source polls far more often than its publisher releases, and
// relies on dedupe rather than guesswork to avoid storing repeats. A source
// with neither `every` nor `daily` would be fetched every 30 minutes.
func TestBroadbandFeedsArePacedAndDeduplicated(t *testing.T) {
	for _, f := range broadbandFeeds() {
		if f.every == 0 && !f.daily {
			t.Errorf("%s/%s would be fetched every cycle", f.agency, f.name)
		}
		if !f.dedupe {
			t.Errorf("%s/%s does not deduplicate; it will store repeats",
				f.agency, f.name)
		}
	}
}

// The transit feeds must keep their original behaviour: they are ephemeral, so
// pacing or deduplicating them would discard real observations.
func TestTransitFeedsAreNotPacedOrDeduplicated(t *testing.T) {
	for _, f := range feeds {
		switch f.agency {
		case "ookla", "census", "fcc":
			continue
		}
		if f.dedupe {
			t.Errorf("%s/%s deduplicates; two identical snapshots are two real observations",
				f.agency, f.name)
		}
		if f.every != 0 {
			t.Errorf("%s/%s is paced; realtime feeds must run every cycle",
				f.agency, f.name)
		}
	}
}
