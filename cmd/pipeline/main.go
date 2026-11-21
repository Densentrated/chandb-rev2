// Command pipeline ingests GTFS-Realtime feeds into the raw layer of the lake.
//
// It runs as a long-lived service on the VPS, on its own schedule. Nothing
// external triggers it: GitHub's cron is best-effort and drifts 5-20 minutes
// under load, and scheduled workflows get disabled after 60 days of repo
// inactivity. Owning the clock here makes the cadence real.
//
//	pipeline           # serve: fetch every INGEST_INTERVAL, forever
//	pipeline -once     # single cycle, exit non-zero if any feed failed
//	pipeline -rebuild  # re-derive bronze and gold from disk, no network
//
// Raw means raw: each feed's response body is written to disk byte-for-byte,
// unparsed. GTFS-RT is protobuf, and the bytes carry their own header
// timestamp, so parsing belongs in the bronze transform where it can be
// re-run against history. That also keeps this binary dependency-free.
//
// Layout, Hive-partitioned so DuckDB can prune by date later:
//
//	/lake/raw/<agency>/<feed>/dt=2026-10-04/20261004T183000Z.pb
//
// Each file is written to a temp name and renamed into place, so a crashed or
// half-finished fetch can never leave a truncated file that looks complete.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultLakeDir    = "/lake"
	defaultSecretsDir = "/run/secrets"

	fetchTimeout = 30 * time.Second
	maxFeedBytes = 32 << 20 // GTFS-RT feeds run ~100KB; this is a sanity bound.
	userAgent    = "chandb-rev2/0.1 (+https://github.com/Densentrated/chandb-rev2)"
)

// fetcher writes one snapshot into the lake and reports where it landed.
//
// The default (fetchFeed) is a plain GET of the whole body, which is the only
// correct choice for an ephemeral feed: a GTFS-RT response exists for about
// thirty seconds and is then gone forever, so raw has to be the replay buffer.
// A source whose publisher archives its files permanently can override this —
// see extractFetcher in broadband.go.
type fetcher func(ctx context.Context, client *http.Client, url, lake string,
	f feed, at time.Time) (string, int64, error)

// authHeader names a request header whose value is read from a secret file.
// Distinct from feed.secret, which substitutes into the URL: the FCC's API
// wants credentials in headers, and a URL-embedded token would end up in logs.
type authHeader struct {
	header string
	secret string
}

// feed is one endpoint to snapshot.
type feed struct {
	agency string
	name   string
	url    string
	// secret names a file in the secrets dir whose contents replace "{key}"
	// in url. Empty means the feed needs no auth. If the file is missing the
	// feed is skipped rather than failed, so this runs fine before you have
	// a key.
	secret string
	// auth carries credentials in headers instead of the URL. Same
	// skip-if-missing behaviour as secret.
	auth []authHeader
	// ext is the snapshot file extension, without the dot. Defaults to "pb".
	ext string
	// daily caps the feed at one snapshot per dt= partition. Static GTFS is
	// a 24MB zip that changes about weekly; pulling it every 30 minutes would
	// be 1.1GB/day of identical bytes.
	daily bool
	// every bounds how often a slow-changing source is re-fetched, measured
	// from the newest snapshot already on disk. The broadband sources publish
	// twice a year (FCC) or quarterly (Ookla); polling them every 30 minutes
	// would be pure waste. Zero means every cycle, subject to daily.
	every time.Duration
	// dedupe drops a snapshot that is byte-identical to the newest one already
	// stored. This is what keeps a weekly poll of a twice-yearly dataset from
	// accumulating fifty identical copies, WITHOUT giving up the ability to
	// re-derive everything from raw — unlike storing diffs, which cannot be
	// replayed through changed parsing logic.
	dedupe bool
	// fetch overrides the default whole-body GET. nil means fetchFeed.
	fetch fetcher
}

func (f feed) extension() string {
	if f.ext == "" {
		return "pb"
	}
	return f.ext
}

var feeds = []feed{
	// MBTA publishes GTFS-RT straight from its CDN with no API key. This is
	// the real thing: actual vehicle lat/lon.
	{
		agency: "mbta",
		name:   "vehicle_positions",
		url:    "https://cdn.mbta.com/realtime/VehiclePositions.pb",
	},
	{
		agency: "mbta",
		name:   "trip_updates",
		url:    "https://cdn.mbta.com/realtime/TripUpdates.pb",
	},

	// BART's own GTFS-RT has NO VehiclePositions feed — only these two.
	// Train locations have to be inferred from stop_time_updates.
	{
		agency: "bart",
		name:   "trip_updates",
		url:    "https://api.bart.gov/gtfsrt/tripupdate.aspx",
	},
	{
		agency: "bart",
		name:   "alerts",
		url:    "https://api.bart.gov/gtfsrt/alerts.aspx",
	},

	// Actual BART vehicle positions exist only through 511's regional API,
	// which needs a free key. Skipped automatically until you add one.
	{
		agency: "bart",
		name:   "vehicle_positions",
		url:    "https://api.511.org/transit/vehiclepositions?agency=BA&api_key={key}",
		secret: "511_api_key",
	},

	// SEPTA runs two independent GTFS feeds with separate id spaces: one for
	// bus, metro and trolley, one for Regional Rail. They are modelled as two
	// agencies so each realtime feed joins only against its own static tables.
	// No API key.
	{
		agency: "septa",
		name:   "vehicle_positions",
		url:    "https://www3.septa.org/gtfsrt/septa-pa-us/Vehicle/rtVehiclePosition.pb",
	},
	{
		agency: "septa",
		name:   "trip_updates",
		url:    "https://www3.septa.org/gtfsrt/septa-pa-us/Trip/rtTripUpdates.pb",
	},
	{
		agency: "septarail",
		name:   "vehicle_positions",
		url:    "https://www3.septa.org/gtfsrt/septarail-pa-us/Vehicle/rtVehiclePosition.pb",
	},
	// Regional Rail trip updates carry one stop_time_update per trip with
	// only a stop_sequence and a delay — no stop_id and no absolute time — so
	// they cannot reach the departure board without joining stop_times.txt.
	{
		agency: "septarail",
		name:   "trip_updates",
		url:    "https://www3.septa.org/gtfsrt/septarail-pa-us/Trip/rtTripUpdates.pb",
	},

	// Static GTFS: the lookup tables that turn realtime IDs into meaning.
	// Without this, a vehicle is "route_id 714 near stop_id 71420"; with it,
	// that's the Pemberton Point bus near a named stop with coordinates.
	{
		agency: "mbta",
		name:   "gtfs_static",
		url:    "https://cdn.mbta.com/MBTA_GTFS.zip",
		ext:    "zip",
		daily:  true,
	},
	{
		agency: "bart",
		name:   "gtfs_static",
		url:    "https://www.bart.gov/dev/schedules/google_transit.zip",
		ext:    "zip",
		daily:  true,
	},
	// SEPTA publishes both feeds in ONE zip that wraps google_bus.zip and
	// google_rail.zip (see nestedStatic). It is fetched once per agency so
	// each raw snapshot stays self-contained under its own agency; that costs
	// an extra ~22MB a day, which raw retention bounds.
	{
		agency: "septa",
		name:   "gtfs_static",
		url:    "https://www3.septa.org/developer/gtfs_public.zip",
		ext:    "zip",
		daily:  true,
	},
	{
		agency: "septarail",
		name:   "gtfs_static",
		url:    "https://www3.septa.org/developer/gtfs_public.zip",
		ext:    "zip",
		daily:  true,
	},
}

const defaultInterval = 30 * time.Minute

// Feeds are independent network requests; a small fixed pool reduces cycle
// wall time without creating one goroutine per configured endpoint.
const ingestWorkers = 4

func main() {
	once := flag.Bool("once", false,
		"run a single ingest cycle and exit instead of serving")
	rebuild := flag.Bool("rebuild", false,
		"re-derive bronze and gold from what is already in LAKE_DIR, then exit")
	installExt := flag.Bool("install-extensions", false,
		"download DuckDB extensions into DUCKDB_EXTENSION_DIR and exit (build step)")
	flag.Parse()

	// Build-time only. The runtime container is read-only and has no business
	// fetching executable code, so extensions are baked into the image here
	// and merely LOADed later.
	if *installExt {
		if err := installExtensions(); err != nil {
			slog.Error("installing duckdb extensions failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if *rebuild {
		if err := runRebuild(); err != nil {
			slog.Error("rebuild failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if *once {
		if err := runOnce(); err != nil {
			slog.Error("ingest failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if err := serve(); err != nil {
		slog.Error("ingest server failed", "err", err)
		os.Exit(1)
	}
}

func interval() time.Duration {
	v := os.Getenv("INGEST_INTERVAL")
	if v == "" {
		return defaultInterval
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		slog.Warn("bad INGEST_INTERVAL, using default",
			"value", v, "default", defaultInterval)
		return defaultInterval
	}
	return d
}

// untilNext returns the wait until the next wall-clock boundary that is a
// multiple of d. With a 30m interval that means :00 and :30 every hour, so
// snapshots land on predictable times no matter when the container happened
// to start or last restart.
func untilNext(now time.Time, d time.Duration) time.Duration {
	if d <= 0 {
		return defaultInterval
	}
	return now.UTC().Truncate(d).Add(d).Sub(now.UTC())
}

func serve() error {
	every := interval()

	lake := lakeDir()
	if _, err := os.Stat(lake); err != nil {
		return fmt.Errorf("stat lake dir %s: %w", lake, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("ingest server starting",
		"interval", every, "lake", lake, "feeds", len(feeds))

	// Fetch immediately so a fresh deploy doesn't sit idle for a whole
	// interval before producing anything.
	cycle(ctx, lake)

	for {
		wait := untilNext(time.Now(), every)
		slog.Info("next cycle",
			"in", wait.Round(time.Second),
			"at", time.Now().Add(wait).UTC().Format(time.RFC3339))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			slog.Info("shutting down")
			return nil
		case <-timer.C:
			cycle(ctx, lake)
		}
	}
}

// cycle runs one ingest pass. It never returns an error: a long-lived service
// must not die because one agency's CDN had a blip. Failures are logged and
// the next tick tries again.
func cycle(ctx context.Context, lake string) {
	start := time.Now()
	failed := ingestAll(ctx, lake)

	// Parse whatever raw snapshots don't have a bronze counterpart yet. Runs
	// even when a feed failed: the other feeds' data is still worth parsing,
	// and it backfills anything written before this stage existed.
	transformBronze(ctx, lake)
	transformGold(ctx, lake)
	// Strictly after the transforms: pruning raw that has not reached gold
	// would discard it permanently.
	prune(ctx, lake)

	switch {
	case ctx.Err() != nil:
		slog.Info("cycle interrupted", "elapsed", time.Since(start).Round(time.Millisecond))
	case len(failed) > 0:
		slog.Error("cycle finished with failures",
			"failed", strings.Join(failed, ", "),
			"elapsed", time.Since(start).Round(time.Millisecond))
	default:
		slog.Info("cycle ok",
			"elapsed", time.Since(start).Round(time.Millisecond))
	}
}

func runOnce() error {
	lake := lakeDir()
	if _, err := os.Stat(lake); err != nil {
		return fmt.Errorf("stat lake dir %s: %w", lake, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(len(feeds))*fetchTimeout)
	defer cancel()

	failed := ingestAll(ctx, lake)

	// Must run here too, not just in cycle(): `-once` is the backfill path,
	// and skipping the transform would make a manual run silently produce
	// raw-only output.
	//
	// Deliberately NOT the fetch context: that one is sized for HTTP timeouts,
	// and a first run has thousands of snapshots to catch up on. Reusing it
	// would abort the backfill partway and look like success.
	tctx, tcancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer tcancel()
	transformBronze(tctx, lake)
	transformGold(tctx, lake)
	prune(tctx, lake)

	if len(failed) > 0 {
		return fmt.Errorf("%d feed(s) failed: %s",
			len(failed), strings.Join(failed, ", "))
	}
	return nil
}

// runRebuild re-derives the lake from what is already on disk: transforms
// only, no fetch and no prune.
//
// It exists so a hand-assembled lake can be turned into gold by the real
// transform. The alternative — writing Parquet by hand to test against — means
// a second copy of the gold schema that drifts the moment gold.go changes, and
// then the thing under test and the thing doing the testing disagree without
// anyone noticing. Here gold.go stays the only definition of a gold row.
//
// ingestAll is skipped because it needs the network; prune is skipped because
// a rebuild runs against inputs someone placed deliberately, and deleting them
// afterwards would make the second run behave differently from the first.
func runRebuild() error {
	lake := lakeDir()
	if _, err := os.Stat(lake); err != nil {
		return fmt.Errorf("stat lake dir %s: %w", lake, err)
	}

	// Same budget as the -once backfill path: a full re-derive of retained raw
	// is minutes of DuckDB work, and the fetch timeout is the wrong scale.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	transformBronze(ctx, lake)
	transformGold(ctx, lake)

	// transformGold logs every failure and carries on, by design — one bad
	// agency must not stop the rest. That makes "it ran" worthless as evidence,
	// so count the output. Empty gold after a rebuild means the bronze inputs
	// or the gold schema moved, and failing loudly here is what makes that
	// visible instead of producing an empty portal.
	n, err := countGoldParquet(lake)
	if err != nil {
		return fmt.Errorf("count gold: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("rebuild produced no parquet under %s",
			filepath.Join(lake, "gold"))
	}
	slog.Info("rebuild ok", "gold_files", n)
	return nil
}

func countGoldParquet(lake string) (int, error) {
	var n int
	err := filepath.WalkDir(filepath.Join(lake, "gold"),
		func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
				n++
			}
			return nil
		})
	// No gold tree at all is a legitimate state to report, not an error to
	// propagate: the caller's "produced nothing" check is the one that matters.
	if errors.Is(err, fs.ErrNotExist) {
		err = nil
	}
	return n, err
}

func lakeDir() string {
	if d := os.Getenv("LAKE_DIR"); d != "" {
		return d
	}
	return defaultLakeDir
}

func secretsDir() string {
	if d := os.Getenv("SECRETS_DIR"); d != "" {
		return d
	}
	return defaultSecretsDir
}

// readSecret returns the trimmed contents of a secret file. Secrets are files,
// never environment variables: DuckDB can read /proc/self/environ, so anything
// in the portal's environment is one query away from being public.
func readSecret(name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(secretsDir(), name))
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return "", fmt.Errorf("secret %s is empty", name)
	}
	return v, nil
}

// ingestAll fetches every feed once and returns the names that failed.
func ingestAll(ctx context.Context, lake string) []string {
	client := &http.Client{Timeout: fetchTimeout}
	now := time.Now().UTC()

	workers := min(ingestWorkers, len(feeds))
	jobs := make(chan feed)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed []string

	worker := func() {
		defer wg.Done()
		for f := range jobs {
			if ctx.Err() != nil {
				return
			}
			if f.daily && alreadyHaveToday(lake, f, now) {
				continue
			}
			if f.every > 0 {
				if last, ok := lastSnapshotAt(lake, f); ok &&
					now.Sub(last) < f.every {
					continue
				}
			}
			url := f.url
			if f.secret != "" {
				key, err := readSecret(f.secret)
				if err != nil {
					slog.Warn("skipping feed, secret unavailable",
						"agency", f.agency, "feed", f.name,
						"secret", f.secret, "err", err)
					continue
				}
				url = strings.ReplaceAll(url, "{key}", key)
			}
			// Resolved up front so a missing credential skips the feed
			// before any request goes out.
			if _, err := resolveAuth(f); err != nil {
				slog.Warn("skipping feed, secret unavailable",
					"agency", f.agency, "feed", f.name, "err", err)
				continue
			}

			get := f.fetch
			if get == nil {
				get = fetchFeed
			}
			path, n, err := get(ctx, client, url, lake, f, now)
			if err != nil {
				slog.Error("feed failed",
					"agency", f.agency, "feed", f.name, "err", err)
				mu.Lock()
				failed = append(failed, f.agency+"/"+f.name)
				mu.Unlock()
				continue
			}
			// An empty path means the fetcher deliberately stored nothing —
			// dedupe found the bytes unchanged. That is a success, not a
			// no-op worth alarming about.
			if path == "" {
				slog.Info("unchanged since last snapshot",
					"agency", f.agency, "feed", f.name, "bytes", n)
				continue
			}
			slog.Info("ingested",
				"agency", f.agency, "feed", f.name, "bytes", n, "path", path)
		}
	}

	wg.Add(workers)
	for range workers {
		go worker()
	}
send:
	for _, f := range feeds {
		select {
		case jobs <- f:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	sort.Strings(failed)
	return failed
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// snapshotPath is the destination for one fetch. Exported shape:
//
//	<lake>/raw/<agency>/<feed>/dt=YYYY-MM-DD/YYYYMMDDTHHMMSSZ.pb
func snapshotPath(lake string, f feed, at time.Time) (dir, file string) {
	at = at.UTC()
	dir = filepath.Join(lake, "raw", f.agency, f.name,
		"dt="+at.Format("2006-01-02"))
	file = at.Format("20060102T150405Z") + "." + f.extension()
	return dir, file
}

// lastSnapshotAt returns the timestamp of the newest snapshot on disk for this
// feed. Snapshot filenames ARE the timestamp (20060102T150405Z), so the newest
// name is the newest capture — no stat calls, and immune to the mtime churn a
// restore or an rsync would cause.
func lastSnapshotAt(lake string, f feed) (time.Time, bool) {
	root := filepath.Join(lake, "raw", f.agency, f.name)
	parts, err := os.ReadDir(root)
	if err != nil {
		return time.Time{}, false
	}
	suffix := "." + f.extension()
	var newest time.Time
	for _, p := range parts {
		if !p.IsDir() || !dtPartition.MatchString(p.Name()) {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(root, p.Name()))
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, suffix) {
				continue
			}
			at, err := time.Parse("20060102T150405Z",
				strings.TrimSuffix(name, suffix))
			if err == nil && at.After(newest) {
				newest = at
			}
		}
	}
	return newest, !newest.IsZero()
}

// newestSnapshotPath returns the path of the newest snapshot, for dedupe.
func newestSnapshotPath(lake string, f feed) (string, bool) {
	at, ok := lastSnapshotAt(lake, f)
	if !ok {
		return "", false
	}
	dir, name := snapshotPath(lake, f, at)
	return filepath.Join(dir, name), true
}

// resolveAuth reads every header credential this feed needs. It returns an
// error if any is missing so the caller can skip the feed rather than send an
// unauthenticated request and record a 401 as real data.
func resolveAuth(f feed) (map[string]string, error) {
	if len(f.auth) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(f.auth))
	for _, a := range f.auth {
		v, err := readSecret(a.secret)
		if err != nil {
			return nil, fmt.Errorf("header %s: %w", a.header, err)
		}
		out[a.header] = v
	}
	return out, nil
}

// sameAsNewest reports whether path holds bytes identical to this feed's most
// recent snapshot. Compares size first: these files are megabytes, and the
// overwhelmingly common case for a slow-changing source is "identical", which
// a size check cannot prove but a size MISMATCH can disprove for free.
func sameAsNewest(lake string, f feed, path string) bool {
	prev, ok := newestSnapshotPath(lake, f)
	if !ok {
		return false
	}
	a, err := os.Stat(prev)
	if err != nil {
		return false
	}
	b, err := os.Stat(path)
	if err != nil || a.Size() != b.Size() {
		return false
	}
	sum := func(p string) ([]byte, error) {
		fh, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		defer func() { _ = fh.Close() }()
		h := sha256.New()
		if _, err := io.Copy(h, fh); err != nil {
			return nil, err
		}
		return h.Sum(nil), nil
	}
	x, err := sum(prev)
	if err != nil {
		return false
	}
	y, err := sum(path)
	return err == nil && bytes.Equal(x, y)
}

// alreadyHaveToday reports whether a daily feed has already been captured for
// this dt= partition.
func alreadyHaveToday(lake string, f feed, at time.Time) bool {
	dir, _ := snapshotPath(lake, f, at)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	suffix := "." + f.extension()
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			return true
		}
	}
	return false
}

func fetchFeed(ctx context.Context, client *http.Client, url, lake string,
	f feed, at time.Time) (string, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", userAgent)
	headers, err := resolveAuth(f)
	if err != nil {
		return "", 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("http %d", resp.StatusCode)
	}

	dir, name := snapshotPath(lake, f, at)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, fmt.Errorf("mkdir %s (is the lake writable by uid %d?): %w",
			dir, os.Getuid(), err)
	}

	// Temp file in the SAME directory, so the rename is atomic rather than a
	// cross-filesystem copy.
	tmp, err := os.CreateTemp(dir, ".partial-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once renamed

	n, err := io.Copy(tmp, io.LimitReader(resp.Body, maxFeedBytes))
	if err != nil {
		_ = tmp.Close()
		return "", 0, err
	}
	if err := tmp.Close(); err != nil {
		return "", 0, err
	}
	if n == 0 {
		return "", 0, errors.New("empty response body")
	}
	// CreateTemp makes 0600. This data is public and operators need to read
	// it; 0600 only blocks debugging without protecting anything.
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return "", 0, err
	}

	// Dedupe before the rename, while the bytes are still in a temp file the
	// deferred Remove will clean up. Doing it after would publish the
	// duplicate and then have to delete a file readers may already have seen.
	if f.dedupe && sameAsNewest(lake, f, tmpName) {
		return "", n, nil
	}

	final := filepath.Join(dir, name)
	if err := os.Rename(tmpName, final); err != nil {
		return "", 0, err
	}
	return final, n, nil
}
