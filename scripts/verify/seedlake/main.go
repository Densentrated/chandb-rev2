// Command seedlake turns the committed fixtures in testdata/lake into a bronze
// layer the real pipeline can transform.
//
// It writes bronze, never gold. Hand-writing Parquet here would mean a second
// copy of the gold schema living outside cmd/pipeline/gold.go, and the two
// would drift the first time a column is added — at which point the harness
// would be testing its own fixture rather than the app. So this stops at
// bronze and `pipeline -rebuild` produces gold by the only code that knows how.
//
// Times are stamped relative to NOW, not frozen. Both pages are windowed:
// /board treats max(last_seen) as "now" and shows -10min..+60min around it,
// and /maps shows the latest 24 hours of feed_timestamp. A fixture with fixed
// dates renders two empty pages and fails checks that are actually fine.
//
// The fixture files carry @<signed seconds>@ tokens, substituted here with
// base + offset. The base is the run's wall clock truncated to the second, so
// one run is internally consistent even if it straddles a second boundary.
//
//	seedlake -testdata testdata/lake -lake .verify/lake
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// staticTables mirrors cmd/pipeline/static.go: the three GTFS lookup tables
// gold joins against. latestStaticDir skips a snapshot that is missing any of
// them, so all three must be copied or gold silently produces nothing.
var staticTables = []string{"routes.txt", "stops.txt", "trips.txt"}

// timeToken matches @+300@ / @-86400@ / @+0@.
var timeToken = regexp.MustCompile(`@([+-]\d+)@`)

func main() {
	testdata := flag.String("testdata", "testdata/lake",
		"directory of committed fixtures, one subdirectory per agency")
	lake := flag.String("lake", ".verify/lake",
		"lake root to write bronze into")
	at := flag.String("now", "",
		"RFC3339 base time for token substitution (default: now, UTC)")
	flag.Parse()

	base := time.Now().UTC().Truncate(time.Second)
	if *at != "" {
		parsed, err := time.Parse(time.RFC3339, *at)
		if err != nil {
			log.Fatalf("seedlake: -now: %v", err)
		}
		base = parsed.UTC()
	}

	n, err := seed(*testdata, *lake, base)
	if err != nil {
		log.Fatalf("seedlake: %v", err)
	}
	fmt.Printf("seedlake: wrote %d bronze file(s) under %s (base %s)\n",
		n, *lake, base.Format(time.RFC3339))
}

func seed(testdata, lake string, base time.Time) (int, error) {
	agencies, err := os.ReadDir(testdata)
	if err != nil {
		return 0, fmt.Errorf("read fixtures: %w", err)
	}

	// One partition for everything. The feed timestamps inside reach back 24
	// hours and may cross UTC midnight, which is fine: dt= only tells DuckDB
	// which directories to scan, and every query here filters on the column.
	dt := "dt=" + base.Format("2006-01-02")
	// Snapshot filename in the pipeline's own format, so a seeded lake is
	// indistinguishable in shape from one the ingest wrote.
	snapshot := base.Format("20060102T150405Z") + ".ndjson"

	var written int
	for _, a := range agencies {
		if !a.IsDir() {
			continue
		}
		agency := a.Name()
		srcDir := filepath.Join(testdata, agency)

		entries, err := os.ReadDir(srcDir)
		if err != nil {
			return written, err
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if !e.IsDir() {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names) // deterministic order, so logs compare run to run

		staticDir := filepath.Join(lake, "bronze", agency, "gtfs_static", dt)
		for _, name := range names {
			src := filepath.Join(srcDir, name)
			switch {
			case contains(staticTables, name):
				if err := copyFile(src, filepath.Join(staticDir, name)); err != nil {
					return written, err
				}
				written++
			case strings.HasSuffix(name, ".ndjson"):
				table := strings.TrimSuffix(name, ".ndjson")
				out := filepath.Join(lake, "bronze", agency, table, dt, snapshot)
				if err := stampNDJSON(src, out, base); err != nil {
					return written, err
				}
				written++
			}
		}
	}
	if written == 0 {
		return 0, fmt.Errorf("no fixtures found under %s", testdata)
	}
	return written, nil
}

// stampNDJSON rewrites the @offset@ tokens and publishes the result.
func stampNDJSON(src, out string, base time.Time) error {
	body, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	var bad error
	stamped := timeToken.ReplaceAllFunc(body, func(tok []byte) []byte {
		offset, err := strconv.ParseInt(string(tok[1:len(tok)-1]), 10, 64)
		if err != nil {
			bad = fmt.Errorf("%s: bad time token %q", src, tok)
			return tok
		}
		return []byte(strconv.FormatInt(base.Unix()+offset, 10))
	})
	if bad != nil {
		return bad
	}
	// An unsubstituted token would reach DuckDB as invalid JSON and surface as
	// an unreadable gold failure three stages later. Catch it here.
	if i := strings.Index(string(stamped), "@"); i >= 0 {
		return fmt.Errorf("%s: unsubstituted token near offset %d", src, i)
	}
	return write(out, stamped)
}

func copyFile(src, out string) error {
	body, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return write(out, body)
}

func write(out string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}
	return os.WriteFile(out, body, 0o644)
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
