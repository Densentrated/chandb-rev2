package main

// Gold: join realtime vehicle positions against the static GTFS lookup tables
// and write one compacted Parquet file per day.
//
// Two things make this the layer worth querying:
//
//   - It is legible. Bronze says route_id "714" near stop_id "71420"; gold
//     says "Green Line B toward Boston College, near Packard's Corner".
//   - It is compacted. Bronze is 48 files per feed per day. A year of that is
//     ~17,500 files, and any reader — DuckDB-WASM in a browser especially —
//     dies on the round trips long before it struggles with the row count.
//     One file per day makes a year 365 files.
//
// DuckDB does the work because it reads NDJSON and CSV natively and writes
// Parquet, so the transform is one SQL statement rather than a hand-rolled
// join plus an encoder.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// dtPartition guards every directory name before it reaches a SQL string.
// Paths here are ours, but a partition name carrying a quote would break the
// statement, and "it can't happen" is how injection bugs start.
var dtPartition = regexp.MustCompile(`^dt=\d{4}-\d{2}-\d{2}$`)

// goldQuery is the transform. LEFT JOINs throughout: MBTA inserts realtime
// "ADDED-" trips that exist in no static schedule, and dropping them would
// silently lose exactly the unusual service worth looking at.
//
// Every GTFS id is forced to VARCHAR. They look numeric and mostly are, so
// type sniffing guesses INT64 and then fails on the first ADDED- trip id.
// quote/escape must be given explicitly too: supplying `types` disables the
// dialect sniffer, and headsigns like "Burlington via Medford Square, West
// Cummings" contain commas.
const goldQuery = `
COPY (
  SELECT
    v.feed_timestamp,
    to_timestamp(v.feed_timestamp)        AS feed_time,
    v.agency,
    v.vehicle_id,
    v.label,
    v.trip_id,
    v.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    r.route_color,
    t.trip_headsign,
    v.direction_id,
    v.latitude,
    v.longitude,
    v.bearing,
    v.speed_mps,
    v.current_status,
    v.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon
  FROM read_json_auto(?) v
  LEFT JOIN read_csv(?, types={'route_id':'VARCHAR'}, quote='"', escape='"') r
    ON r.route_id = v.route_id
  LEFT JOIN read_csv(?, types={'trip_id':'VARCHAR','route_id':'VARCHAR'}, quote='"', escape='"') t
    ON t.trip_id = v.trip_id
  LEFT JOIN read_csv(?, types={'stop_id':'VARCHAR'}, quote='"', escape='"') s
    ON s.stop_id = v.stop_id
  WHERE v.latitude IS NOT NULL
  ORDER BY v.feed_timestamp, v.vehicle_id
) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)`

func transformGold(ctx context.Context, lake string) {
	staticDir, err := latestStaticDir(lake)
	if err != nil {
		slog.Debug("gold skipped, no static GTFS yet", "err", err)
		return
	}

	srcRoot := filepath.Join(lake, "bronze", "mbta", "vehicle_positions")
	parts, err := os.ReadDir(srcRoot)
	if err != nil {
		return
	}

	db, err := openDuckDB()
	if err != nil {
		slog.Warn("gold skipped, duckdb unavailable", "err", err)
		return
	}
	defer func() { _ = db.Close() }()

	today := "dt=" + time.Now().UTC().Format("2006-01-02")
	var built int
	start := time.Now()

	for _, p := range parts {
		if ctx.Err() != nil {
			return
		}
		name := p.Name()
		if !p.IsDir() || !dtPartition.MatchString(name) {
			continue
		}

		outDir := filepath.Join(lake, "gold", "mbta", "vehicle_positions", name)
		outFile := filepath.Join(outDir, "part-0.parquet")

		// Past days are complete, so a existing file is final. Today's is
		// still accumulating snapshots, so rebuild it every cycle.
		if _, err := os.Stat(outFile); err == nil && name != today {
			continue
		}
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			slog.Warn("gold mkdir failed", "dir", outDir, "err", err)
			continue
		}

		if err := buildGoldPartition(ctx, db, srcRoot, name, staticDir, outFile); err != nil {
			slog.Warn("gold partition failed", "partition", name, "err", err)
			continue
		}
		built++
	}

	if built > 0 {
		slog.Info("gold",
			"partitions", built,
			"static", filepath.Base(staticDir),
			"elapsed", time.Since(start).Round(time.Millisecond))
	}
}

func buildGoldPartition(ctx context.Context, db *sql.DB,
	srcRoot, partition, staticDir, outFile string) error {
	// COPY ... TO cannot be parameterised, so it is built by hand. Safe here
	// because the partition name is regex-validated above and every other
	// component is a constant: this code never sees user input. The portal,
	// which does, must never allow COPY at all.
	tmp := outFile + ".partial"
	q := strings.Replace(goldQuery, ") TO ?", ") TO '"+tmp+"'", 1)

	if _, err := db.ExecContext(ctx, q,
		filepath.Join(srcRoot, partition, "*.ndjson"),
		filepath.Join(staticDir, "routes.txt"),
		filepath.Join(staticDir, "trips.txt"),
		filepath.Join(staticDir, "stops.txt"),
	); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, outFile)
}

func openDuckDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	// The pipeline container is capped at 512MB and its /tmp tmpfs at 64MB,
	// so DuckDB must be told to stay well inside both rather than discovering
	// the limit via the OOM killer.
	for _, pragma := range []string{
		"SET memory_limit='256MB'",
		"SET threads=2",
		"SET temp_directory='/tmp'",
		"SET max_temp_directory_size='48MB'",
	} {
		if _, err := db.Exec(pragma); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", pragma, err)
		}
	}
	return db, nil
}

// latestStaticDir returns the newest bronze GTFS snapshot that actually has
// every table extracted. Partial snapshots are skipped so a gold build never
// half-joins against an interrupted download.
func latestStaticDir(lake string) (string, error) {
	root := filepath.Join(lake, "bronze", "mbta", "gtfs_static")
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}

	var dts []string
	for _, e := range entries {
		if e.IsDir() && dtPartition.MatchString(e.Name()) {
			dts = append(dts, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dts))) // dt= sorts chronologically

	for _, dt := range dts {
		dir := filepath.Join(root, dt)
		complete := true
		for _, tbl := range staticTables {
			if _, err := os.Stat(filepath.Join(dir, tbl)); err != nil {
				complete = false
				break
			}
		}
		if complete {
			return dir, nil
		}
	}
	return "", fmt.Errorf("no complete static GTFS snapshot under %s", root)
}
