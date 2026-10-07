package main

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeZip(t *testing.T, path string, members map[string]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	zw := zip.NewWriter(f)
	for name, body := range members {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
}

func fullGTFS() map[string]string {
	return map[string]string{
		"routes.txt": "route_id,route_long_name\n714,Pemberton Point\n",
		"stops.txt":  "stop_id,stop_name\n71420,Hull\n",
		"trips.txt":  "trip_id,trip_headsign\n789,\"Boston, via Hull\"\n",
		"shapes.txt": "shape_id,shape_pt_lat\n1,42.0\n", // present but not wanted
	}
}

func TestExtractStaticPullsOnlyWantedTables(t *testing.T) {
	lake := t.TempDir()
	raw := filepath.Join(lake, "raw", "mbta", "gtfs_static",
		"dt=2026-10-07", "20261007T050000Z.zip")
	makeZip(t, raw, fullGTFS())

	if err := extractStatic(lake, raw); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(lake, "bronze", "mbta", "gtfs_static", "dt=2026-10-07")
	for _, want := range staticTables {
		if _, err := os.Stat(filepath.Join(out, want)); err != nil {
			t.Errorf("%s not extracted: %v", want, err)
		}
	}
	// shapes.txt is 16MB in the real feed and nothing joins against it yet.
	if _, err := os.Stat(filepath.Join(out, "shapes.txt")); err == nil {
		t.Error("shapes.txt should not be extracted")
	}
}

// A zip member named ../../etc/passwd must not escape the output directory.
func TestExtractStaticResistsZipSlip(t *testing.T) {
	lake := t.TempDir()
	raw := filepath.Join(lake, "raw", "mbta", "gtfs_static",
		"dt=2026-10-07", "evil.zip")

	members := fullGTFS()
	members["../../../../tmp/pwned-routes.txt"] = "route_id\nEVIL\n"
	makeZip(t, raw, members)

	if err := extractStatic(lake, raw); err != nil {
		t.Fatal(err)
	}

	// Everything written must sit under the bronze partition.
	root := filepath.Join(lake, "bronze", "mbta", "gtfs_static", "dt=2026-10-07")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		switch e.Name() {
		case "routes.txt", "stops.txt", "trips.txt":
		default:
			t.Errorf("unexpected file written: %s", e.Name())
		}
	}
	if _, err := os.Stat("/tmp/pwned-routes.txt"); err == nil {
		_ = os.Remove("/tmp/pwned-routes.txt")
		t.Fatal("zip member escaped the output directory")
	}
}

func TestExtractStaticFailsOnMissingTable(t *testing.T) {
	lake := t.TempDir()
	raw := filepath.Join(lake, "raw", "mbta", "gtfs_static",
		"dt=2026-10-07", "partial.zip")
	makeZip(t, raw, map[string]string{"routes.txt": "route_id\n1\n"})

	if err := extractStatic(lake, raw); err == nil {
		t.Error("a zip missing stops.txt/trips.txt should fail loudly")
	}
}

// Gold must never join against a half-extracted snapshot.
func TestLatestStaticDirSkipsIncomplete(t *testing.T) {
	lake := t.TempDir()
	root := filepath.Join(lake, "bronze", "mbta", "gtfs_static")

	complete := filepath.Join(root, "dt=2026-10-05")
	if err := os.MkdirAll(complete, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range staticTables {
		if err := os.WriteFile(filepath.Join(complete, tbl), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Newer, but missing trips.txt.
	partial := filepath.Join(root, "dt=2026-10-07")
	if err := os.MkdirAll(partial, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partial, "routes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := latestStaticDir(lake, "mbta")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(got) != "dt=2026-10-05" {
		t.Errorf("picked %s, want the complete dt=2026-10-05", filepath.Base(got))
	}
}

func TestLatestStaticDirErrorsWhenNoneUsable(t *testing.T) {
	if _, err := latestStaticDir(t.TempDir(), "mbta"); err == nil {
		t.Error("expected an error when no static snapshot exists")
	}
}

// Anything reaching a SQL string must look exactly like a date partition.
func TestDtPartitionRegexRejectsInjection(t *testing.T) {
	good := []string{"dt=2026-10-07", "dt=1999-01-31"}
	bad := []string{
		"dt=2026-10-07'", "dt=2026-10-07; DROP TABLE x", "dt=../../etc",
		"dt=2026-1-7", "2026-10-07", "dt=", "dt=2026-10-07 ",
	}
	for _, s := range good {
		if !dtPartition.MatchString(s) {
			t.Errorf("%q should be accepted", s)
		}
	}
	for _, s := range bad {
		if dtPartition.MatchString(s) {
			t.Errorf("%q must be rejected", s)
		}
	}
}

func TestDailyFeedSkipsWhenTodayAlreadyCaptured(t *testing.T) {
	lake := t.TempDir()
	f := feed{agency: "mbta", name: "gtfs_static", ext: "zip", daily: true}
	at := time.Date(2026, 10, 7, 5, 0, 0, 0, time.UTC)

	if alreadyHaveToday(lake, f, at) {
		t.Error("empty lake should not report a capture")
	}

	dir, name := snapshotPath(lake, f, at)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}

	if !alreadyHaveToday(lake, f, at) {
		t.Error("should detect today's existing snapshot")
	}
	// A different day must still be fetched.
	if alreadyHaveToday(lake, f, at.AddDate(0, 0, 1)) {
		t.Error("tomorrow should not be considered captured")
	}
}

func TestFeedExtensionDefaultsToPb(t *testing.T) {
	if got := (feed{}).extension(); got != "pb" {
		t.Errorf("default extension = %q, want pb", got)
	}
	if got := (feed{ext: "zip"}).extension(); got != "zip" {
		t.Errorf("extension = %q, want zip", got)
	}
}
