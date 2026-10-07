package main

// Retention: raw and bronze are buffers, gold is the archive.
//
// Measured on this lake, one day of 30-minute snapshots costs ~37MB of raw and
// ~190MB of bronze, while the gold tables derived from it cost well under a
// megabyte. Keeping every layer forever means ~7GB/month to preserve ~25MB of
// answers.
//
// So the layers expire on different clocks:
//
//	gold    never      the archive; everything else can be rebuilt from raw
//	raw     14 days    replay buffer: re-derive gold after a schema change
//	bronze   2 days    pure intermediate, 5x larger than the raw it came from
//
// Gold is NEVER pruned here. If that ever changes, the data is gone — raw will
// have expired long before.

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	defaultRawRetentionDays    = 14
	defaultBronzeRetentionDays = 2
)

func retentionDays(env string, fallback int) int {
	v := os.Getenv(env)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		slog.Warn("bad retention setting, using default",
			"env", env, "value", v, "default", fallback)
		return fallback
	}
	return n
}

// prune deletes dt= partitions older than each layer's retention.
//
// It runs after the transforms, never before: pruning raw that has not yet
// been promoted to gold would discard data permanently.
func prune(ctx context.Context, lake string) {
	layers := []struct {
		name string
		days int
	}{
		{"raw", retentionDays("RAW_RETENTION_DAYS", defaultRawRetentionDays)},
		{"bronze", retentionDays("BRONZE_RETENTION_DAYS", defaultBronzeRetentionDays)},
	}

	for _, l := range layers {
		cutoff := time.Now().UTC().AddDate(0, 0, -l.days)
		removed, freed := pruneLayer(ctx, filepath.Join(lake, l.name), cutoff)
		if removed > 0 {
			slog.Info("pruned",
				"layer", l.name, "partitions", removed,
				"freed_mb", freed/(1<<20),
				"older_than", cutoff.Format("2006-01-02"))
		}
	}
}

// pruneLayer walks <layer>/<agency>/<table>/dt=... and removes expired
// partitions. Only directories matching the dt= pattern exactly are considered,
// so an unexpected directory is left alone rather than deleted.
func pruneLayer(ctx context.Context, root string, cutoff time.Time) (removed int, freed int64) {
	agencies, err := os.ReadDir(root)
	if err != nil {
		return 0, 0
	}

	for _, agency := range agencies {
		if !agency.IsDir() {
			continue
		}
		tables, err := os.ReadDir(filepath.Join(root, agency.Name()))
		if err != nil {
			continue
		}

		for _, table := range tables {
			if !table.IsDir() {
				continue
			}
			tableDir := filepath.Join(root, agency.Name(), table.Name())
			parts, err := os.ReadDir(tableDir)
			if err != nil {
				continue
			}

			for _, p := range parts {
				if ctx.Err() != nil {
					return removed, freed
				}
				if !p.IsDir() || !dtPartition.MatchString(p.Name()) {
					continue
				}
				day, err := time.Parse("2006-01-02",
					p.Name()[len("dt="):])
				if err != nil || !day.Before(cutoff) {
					continue
				}

				dir := filepath.Join(tableDir, p.Name())
				size := dirSize(dir)
				if err := os.RemoveAll(dir); err != nil {
					slog.Warn("prune failed", "dir", dir, "err", err)
					continue
				}
				removed++
				freed += size
			}
		}
	}
	return removed, freed
}

func dirSize(dir string) int64 {
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}
