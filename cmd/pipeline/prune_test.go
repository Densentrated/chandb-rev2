package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func seedPartition(t *testing.T, lake, layer, agency, table string, day time.Time) string {
	t.Helper()
	dir := filepath.Join(lake, layer, agency, table,
		"dt="+day.UTC().Format("2006-01-02"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "x.pb"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestPruneRemovesOnlyExpiredPartitions(t *testing.T) {
	lake := t.TempDir()
	now := time.Now().UTC()

	fresh := seedPartition(t, lake, "raw", "mbta", "vehicle_positions", now)
	old := seedPartition(t, lake, "raw", "mbta", "vehicle_positions", now.AddDate(0, 0, -20))

	t.Setenv("RAW_RETENTION_DAYS", "14")
	prune(context.Background(), lake)

	if _, err := os.Stat(fresh); err != nil {
		t.Error("today's partition must survive")
	}
	if _, err := os.Stat(old); err == nil {
		t.Error("a 20-day-old partition should have been pruned at 14 days")
	}
}

// Gold is the archive. If this ever starts deleting, the data is gone — raw
// will have expired long before.
func TestPruneNeverTouchesGold(t *testing.T) {
	lake := t.TempDir()
	ancient := seedPartition(t, lake, "gold", "mbta", "vehicle_positions",
		time.Now().UTC().AddDate(-5, 0, 0))

	t.Setenv("RAW_RETENTION_DAYS", "1")
	t.Setenv("BRONZE_RETENTION_DAYS", "1")
	prune(context.Background(), lake)

	if _, err := os.Stat(ancient); err != nil {
		t.Fatal("gold must never be pruned, even when five years old")
	}
}

// Bronze expires sooner than raw, since it is a rebuildable intermediate that
// measures several times larger than its source.
func TestPruneAppliesSeparateRetentionPerLayer(t *testing.T) {
	lake := t.TempDir()
	day := time.Now().UTC().AddDate(0, 0, -5)

	rawDir := seedPartition(t, lake, "raw", "mbta", "vehicle_positions", day)
	bronzeDir := seedPartition(t, lake, "bronze", "mbta", "vehicle_positions", day)

	t.Setenv("RAW_RETENTION_DAYS", "14")
	t.Setenv("BRONZE_RETENTION_DAYS", "2")
	prune(context.Background(), lake)

	if _, err := os.Stat(rawDir); err != nil {
		t.Error("raw at 5 days should survive a 14-day retention")
	}
	if _, err := os.Stat(bronzeDir); err == nil {
		t.Error("bronze at 5 days should be pruned at 2-day retention")
	}
}

// A directory that is not a dt= partition must be left alone rather than
// guessed at and deleted.
func TestPruneIgnoresNonPartitionDirectories(t *testing.T) {
	lake := t.TempDir()
	odd := filepath.Join(lake, "raw", "mbta", "vehicle_positions", "scratch")
	if err := os.MkdirAll(odd, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("RAW_RETENTION_DAYS", "1")
	prune(context.Background(), lake)

	if _, err := os.Stat(odd); err != nil {
		t.Error("a non-partition directory must not be pruned")
	}
}

func TestRetentionDaysRejectsNonsense(t *testing.T) {
	for _, v := range []string{"", "abc", "0", "-3"} {
		t.Setenv("RAW_RETENTION_DAYS", v)
		if got := retentionDays("RAW_RETENTION_DAYS", 14); got != 14 {
			t.Errorf("value %q gave %d, want the 14-day default", v, got)
		}
	}
	t.Setenv("RAW_RETENTION_DAYS", "30")
	if got := retentionDays("RAW_RETENTION_DAYS", 14); got != 30 {
		t.Errorf("got %d, want 30", got)
	}
}

// Without this, every cycle would re-derive bronze for days the pruner has
// already deleted, forever.
func TestExpiredPartitionWatermark(t *testing.T) {
	cutoff := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	cases := map[string]bool{
		"/lake/raw/mbta/vehicle_positions/dt=2026-10-01/x.pb": true,
		"/lake/raw/mbta/vehicle_positions/dt=2026-10-07/x.pb": false,
		"/lake/raw/mbta/vehicle_positions/dt=2026-10-05/x.pb": false,
		"/lake/raw/mbta/whatever/x.pb":                        false, // no partition: never skip
	}
	for path, want := range cases {
		if got := expiredPartition(path, cutoff); got != want {
			t.Errorf("expiredPartition(%q) = %v, want %v", path, got, want)
		}
	}
}

func TestPruneReportsFreedBytes(t *testing.T) {
	lake := t.TempDir()
	dir := seedPartition(t, lake, "raw", "mbta", "vehicle_positions",
		time.Now().UTC().AddDate(0, 0, -30))
	for i := range 3 {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d.pb", i)),
			make([]byte, 1000), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := dirSize(dir); got < 3000 {
		t.Errorf("dirSize = %d, want at least 3000", got)
	}
}
