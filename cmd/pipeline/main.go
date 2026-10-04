// Command pipeline ingests GTFS-Realtime feeds into the raw layer of the lake.
//
// It runs as a long-lived service on the VPS, on its own schedule. Nothing
// external triggers it: GitHub's cron is best-effort and drifts 5-20 minutes
// under load, and scheduled workflows get disabled after 60 days of repo
// inactivity. Owning the clock here makes the cadence real.
//
//	pipeline           # serve: fetch every INGEST_INTERVAL, forever
//	pipeline -once     # single cycle, exit non-zero if any feed failed
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
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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

// feed is one GTFS-Realtime endpoint to snapshot.
type feed struct {
	agency string
	name   string
	url    string
	// secret names a file in the secrets dir whose contents replace "{key}"
	// in url. Empty means the feed needs no auth. If the file is missing the
	// feed is skipped rather than failed, so this runs fine before you have
	// a key.
	secret string
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
}

const defaultInterval = 30 * time.Minute

func main() {
	once := flag.Bool("once", false,
		"run a single ingest cycle and exit instead of serving")
	flag.Parse()

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

	if failed := ingestAll(ctx, lake); len(failed) > 0 {
		return fmt.Errorf("%d feed(s) failed: %s",
			len(failed), strings.Join(failed, ", "))
	}
	return nil
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

	var failed []string
	for _, f := range feeds {
		if ctx.Err() != nil {
			return failed
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

		path, n, err := fetchFeed(ctx, client, url, lake, f, now)
		if err != nil {
			// One bad feed must not discard the others. Record it and keep
			// going; the exit code at the end surfaces it.
			slog.Error("feed failed",
				"agency", f.agency, "feed", f.name, "err", err)
			failed = append(failed, f.agency+"/"+f.name)
			continue
		}
		slog.Info("ingested",
			"agency", f.agency, "feed", f.name, "bytes", n, "path", path)
	}
	return failed
}

// snapshotPath is the destination for one fetch. Exported shape:
//
//	<lake>/raw/<agency>/<feed>/dt=YYYY-MM-DD/YYYYMMDDTHHMMSSZ.pb
func snapshotPath(lake string, f feed, at time.Time) (dir, file string) {
	at = at.UTC()
	dir = filepath.Join(lake, "raw", f.agency, f.name,
		"dt="+at.Format("2006-01-02"))
	file = at.Format("20060102T150405Z") + ".pb"
	return dir, file
}

func fetchFeed(ctx context.Context, client *http.Client, url, lake string,
	f feed, at time.Time) (string, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", userAgent)

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

	final := filepath.Join(dir, name)
	if err := os.Rename(tmpName, final); err != nil {
		return "", 0, err
	}
	return final, n, nil
}
