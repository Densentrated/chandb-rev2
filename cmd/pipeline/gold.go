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

// staticLoad narrows each GTFS table to the columns gold actually uses and
// materialises it once per run.
//
// Re-reading the CSVs inside every partition's query was what ran the
// container out of memory on the first full day: trips.txt is 9.8MB across 12
// columns, re-parsed per partition, when only two columns are ever read.
//
// One statement per Exec, deliberately. The driver will happily accept several
// statements in one string, but parameter binding does not survive the split —
// it fails with "incorrect argument count for command: have 0 want 1".
var staticLoads = []struct{ file, stmt string }{
	{"routes.txt", `CREATE OR REPLACE TEMP TABLE gtfs_routes AS
  SELECT route_id, route_short_name, route_long_name, route_type, route_color
  FROM read_csv(?, types={'route_id':'VARCHAR'}, quote='"', escape='"')`},
	{"trips.txt", `CREATE OR REPLACE TEMP TABLE gtfs_trips AS
  SELECT trip_id, route_id AS trip_route_id, trip_headsign
  FROM read_csv(?, types={'trip_id':'VARCHAR','route_id':'VARCHAR'}, quote='"', escape='"')`},
	{"stops.txt", `CREATE OR REPLACE TEMP TABLE gtfs_stops AS
  SELECT stop_id, stop_name, stop_lat, stop_lon
  FROM read_csv(?, types={'stop_id':'VARCHAR'}, quote='"', escape='"')`},
}

// goldQuery is the transform. LEFT JOINs throughout: MBTA inserts realtime
// "ADDED-" trips that exist in no static schedule, and dropping them would
// silently lose exactly the unusual service worth looking at.
//
// Every GTFS id above is forced to VARCHAR. They look numeric and mostly are,
// so type sniffing guesses INT64 and then fails on the first ADDED- trip id.
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
    coalesce(v.route_id, t.trip_route_id) AS route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    CASE r.route_type
      WHEN 0 THEN 'Light Rail' WHEN 1 THEN 'Subway'   WHEN 2 THEN 'Commuter Rail'
      WHEN 3 THEN 'Bus'        WHEN 4 THEN 'Ferry'    WHEN 5 THEN 'Cable Tram'
      WHEN 6 THEN 'Aerial Lift' WHEN 7 THEN 'Funicular'
      WHEN 11 THEN 'Trolleybus' WHEN 12 THEN 'Monorail'
    END                                   AS vehicle_type,
    '{{SOURCE}}'                          AS source,
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
  FROM read_json(?, format='newline_delimited', columns={
        'agency':'VARCHAR','feed_timestamp':'BIGINT','vehicle_id':'VARCHAR',
        'label':'VARCHAR','trip_id':'VARCHAR','route_id':'VARCHAR',
        'direction_id':'BIGINT','latitude':'DOUBLE','longitude':'DOUBLE',
        'bearing':'DOUBLE','speed_mps':'DOUBLE','stop_id':'VARCHAR',
        'current_status':'VARCHAR','vehicle_timestamp':'BIGINT'}) v
  LEFT JOIN gtfs_trips  t ON t.trip_id  = v.trip_id
  LEFT JOIN gtfs_routes r ON r.route_id = coalesce(v.route_id, t.trip_route_id)
  LEFT JOIN gtfs_stops  s ON s.stop_id  = v.stop_id
  WHERE v.latitude IS NOT NULL
  ORDER BY v.feed_timestamp, v.vehicle_id
) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)`

// stopEventsQuery answers "was it on time, and can I trust that number?".
//
// GTFS-RT TripUpdates are forecasts, not outcomes: a row says "we currently
// expect this trip at this stop N seconds late". The forecast made closest to
// the actual arrival is the best available proxy for what happened, and
// agencies drop a stop from the feed once it has been served. So the LAST
// prediction we observed for each (trip, stop) is the one worth keeping.
//
// That dedupe is also what bounds storage. The row count here is set by how
// many stop events the timetable actually contains, not by how often we poll,
// so sampling faster buys accuracy without growing this table.
//
// The JSON schema is pinned rather than sniffed. Bronze rows use omitempty, so
// an absent field is simply missing from the line — and read_json_auto then
// never creates the column. BART emits no stop_sequence at all, so sniffing
// produced a table without it and the query failed to bind. Declaring the
// columns makes the shape independent of what any one day happened to contain.
//
// lead_time_s is the honesty column: seconds between the final prediction and
// the predicted arrival. Small means the forecast was made near the event and
// the delay figure is trustworthy; large means we sampled too slowly to know.
// Filter on it rather than assuming every row is equally good.
const stopEventsQuery = `
COPY (
  WITH ranked AS (
    SELECT *,
      row_number() OVER (PARTITION BY trip_id, stop_id, stop_sequence
                         ORDER BY feed_timestamp DESC)   AS rn,
      count(*)     OVER (PARTITION BY trip_id, stop_id, stop_sequence) AS observations
    FROM read_json(?, format='newline_delimited', columns={
          'agency':'VARCHAR','feed_timestamp':'BIGINT','trip_id':'VARCHAR',
          'route_id':'VARCHAR','direction_id':'BIGINT','vehicle_id':'VARCHAR',
          'stop_id':'VARCHAR','stop_sequence':'BIGINT',
          'arrival_time':'BIGINT','arrival_delay':'BIGINT',
          'departure_time':'BIGINT','departure_delay':'BIGINT',
          'schedule_relationship':'VARCHAR'})
  )
  SELECT
    u.agency,
    u.trip_id,
    coalesce(u.route_id, t.trip_route_id) AS route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    CASE r.route_type
      WHEN 0 THEN 'Light Rail' WHEN 1 THEN 'Subway'   WHEN 2 THEN 'Commuter Rail'
      WHEN 3 THEN 'Bus'        WHEN 4 THEN 'Ferry'    WHEN 5 THEN 'Cable Tram'
      WHEN 6 THEN 'Aerial Lift' WHEN 7 THEN 'Funicular'
      WHEN 11 THEN 'Trolleybus' WHEN 12 THEN 'Monorail'
    END                                   AS vehicle_type,
    '{{SOURCE}}'                          AS source,
    t.trip_headsign,
    u.direction_id,
    u.vehicle_id,
    u.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    u.stop_sequence,
    u.arrival_time,
    u.arrival_delay,
    u.departure_time,
    u.departure_delay,
    u.schedule_relationship,
    u.feed_timestamp                   AS last_seen,
    to_timestamp(u.feed_timestamp)     AS last_seen_time,
    u.observations,
    u.arrival_time - u.feed_timestamp  AS lead_time_s
  FROM ranked u
  LEFT JOIN gtfs_trips  t ON t.trip_id  = u.trip_id
  -- BART's TripUpdates carry no route_id at all, so fall back to the route
  -- the static schedule assigns the trip. Without this every BART route name
  -- is NULL.
  LEFT JOIN gtfs_routes r ON r.route_id = coalesce(u.route_id, t.trip_route_id)
  LEFT JOIN gtfs_stops  s ON s.stop_id  = u.stop_id
  WHERE u.rn = 1
  ORDER BY u.route_id, u.trip_id, u.stop_sequence
) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)`

// goldTable is one derived dataset: a bronze source and the SQL that shapes it.
type goldTable struct {
	agency string
	source string // bronze table name
	out    string // gold table name
	query  string
}

var goldTables = []goldTable{
	{agency: "mbta", source: "vehicle_positions", out: "vehicle_positions", query: goldQuery},
	{agency: "mbta", source: "trip_updates", out: "stop_events", query: stopEventsQuery},
	// BART publishes no vehicle positions, so its TripUpdates are the only
	// signal about where its trains are and whether they are late.
	{agency: "bart", source: "trip_updates", out: "stop_events", query: stopEventsQuery},
}

func transformGold(ctx context.Context, lake string) {
	db, err := openDuckDB(lake)
	if err != nil {
		slog.Warn("gold skipped, duckdb unavailable", "err", err)
		return
	}
	defer func() { _ = db.Close() }()

	today := "dt=" + time.Now().UTC().Format("2006-01-02")
	var built int
	start := time.Now()

	// Static lookups are per-agency and MUST stay that way. Joining one
	// agency's realtime feed against another's routes/stops silently yields
	// NULL names at best, and at worst invents matches where ids collide.
	for _, agency := range goldAgencies() {
		staticDir, err := latestStaticDir(lake, agency)
		if err != nil {
			slog.Debug("gold skipped, no static GTFS yet", "agency", agency)
			continue
		}
		if err := loadStatic(ctx, db, staticDir); err != nil {
			slog.Warn("gold skipped, static tables would not load",
				"agency", agency, "err", err)
			continue
		}

		for _, tbl := range goldTables {
			if tbl.agency != agency {
				continue
			}
			srcRoot := filepath.Join(lake, "bronze", tbl.agency, tbl.source)
			parts, err := os.ReadDir(srcRoot)
			if err != nil {
				continue // that bronze table has not been produced yet
			}

			for _, p := range parts {
				if ctx.Err() != nil {
					return
				}
				name := p.Name()
				if !p.IsDir() || !dtPartition.MatchString(name) {
					continue
				}

				outDir := filepath.Join(lake, "gold", tbl.agency, tbl.out, name)
				outFile := filepath.Join(outDir, "part-0.parquet")

				// Past days are complete, so an existing file is final.
				// Today's is still accumulating, so rebuild it every cycle.
				if _, err := os.Stat(outFile); err == nil && name != today {
					continue
				}
				if err := os.MkdirAll(outDir, 0o755); err != nil {
					slog.Warn("gold mkdir failed", "dir", outDir, "err", err)
					continue
				}

				if err := buildGoldPartition(ctx, db, tbl.query,
					tbl.agency+"/"+tbl.source,
					filepath.Join(srcRoot, name, "*.ndjson"), outFile); err != nil {
					slog.Warn("gold partition failed",
						"table", tbl.agency+"/"+tbl.out,
						"partition", name, "err", err)
					continue
				}
				built++
			}
		}
	}

	if built > 0 {
		slog.Info("gold", "partitions", built,
			"elapsed", time.Since(start).Round(time.Millisecond))
	}
}

// goldAgencies lists the agencies with gold tables, in a stable order so logs
// read the same way run to run.
func goldAgencies() []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range goldTables {
		if !seen[t.agency] {
			seen[t.agency] = true
			out = append(out, t.agency)
		}
	}
	sort.Strings(out)
	return out
}

func buildGoldPartition(ctx context.Context, db *sql.DB,
	query, source, srcGlob, outFile string) error {
	// COPY ... TO cannot be parameterised, so it is built by hand. Safe here
	// because the partition name is regex-validated above and every other
	// component is a constant: this code never sees user input. The portal,
	// which does, must never allow COPY at all.
	tmp := outFile + ".partial"
	q := strings.Replace(query, ") TO ?", ") TO '"+tmp+"'", 1)
	q = strings.ReplaceAll(q, "{{SOURCE}}", source)

	if _, err := db.ExecContext(ctx, q, srcGlob); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, outFile)
}

// loadStatic materialises the narrowed GTFS lookup tables once per run, so
// each partition joins against in-memory tables instead of re-parsing CSV.
func loadStatic(ctx context.Context, db *sql.DB, staticDir string) error {
	for _, l := range staticLoads {
		if _, err := db.ExecContext(ctx, l.stmt,
			filepath.Join(staticDir, l.file)); err != nil {
			return fmt.Errorf("load %s: %w", l.file, err)
		}
	}
	return nil
}

func openDuckDB(lake string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	// Spill to DISK, not to /tmp.
	//
	// /tmp in this container is a tmpfs, so DuckDB offloading there consumes
	// the very memory it is trying to free — and a full day of MBTA trip
	// updates exhausted both the memory limit and the 48MB tmpfs cap. The lake
	// volume is real disk with hundreds of GB spare. The leading dot keeps the
	// directory out of the dt= partition scans in bronze and prune.
	tmpDir := filepath.Join(lake, ".duckdb-tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("duckdb temp dir: %w", err)
	}
	// The pipeline container is capped at 512MB and its /tmp tmpfs at 64MB,
	// so DuckDB must be told to stay well inside both rather than discovering
	// the limit via the OOM killer.
	for _, pragma := range []string{
		"SET memory_limit='512MB'",
		"SET threads=2",
		"SET preserve_insertion_order=false",
		"SET temp_directory='" + tmpDir + "'",
		"SET max_temp_directory_size='8GB'",
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
func latestStaticDir(lake, agency string) (string, error) {
	root := filepath.Join(lake, "bronze", agency, "gtfs_static")
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
