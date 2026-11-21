package main

// Broadband: what ISPs CLAIM they serve, against what anyone can measure.
//
// The FCC's Broadband Data Collection is self-reported by providers. It is the
// map that federal funding is allocated from, and it is persistently more
// optimistic than reality — a provider may report an entire census block as
// served if it could serve one location there within ten business days. There
// is no published ground truth to check it against, so this domain assembles
// the next best thing from three independent angles:
//
//   - CLAIMED      FCC BDC area summaries: % of locations by technology.
//                  Fiber-to-the-premises is technology code 50.
//   - MEASURED     Ookla speed tests, aggregated to ~600m tiles. Performance
//                  where people actually ran a test.
//   - SUBSCRIBED   Census ACS table B28002: households reporting a broadband
//                  subscription. What people actually buy and use.
//
// None of these is ground truth on its own, and the gold layer must not pretend
// otherwise. Measured speed is not availability: a tract with no fiber can post
// fast numbers on good cable, and a tract with no tests is unmeasured rather
// than unserved. The analysis worth drawing is the DISAGREEMENT between the
// three — tracts where claims are high, measurements are low, and subscription
// is lower still are where the claimed map is least believable.
//
// Everything is keyed on the 11-digit census tract GEOID (state+county+tract),
// which is also what ACS demographics join on. That is what turns "this tract
// claims 94% fiber" into "claimed coverage is lowest in the poorest tracts".

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// region is one metro area: the counties it covers and a bounding box.
//
// The bbox is deliberately generous. It only pre-filters Ookla tiles down from
// a global file; the authoritative cut is the spatial join against real tract
// polygons in gold, so a few extra tiles cost a little bandwidth and change no
// result. A bbox that is too TIGHT, by contrast, silently drops real data.
type region struct {
	id                             string
	state                          string   // 2-digit state FIPS
	counties                       []string // 3-digit county FIPS within that state
	minLon, minLat, maxLon, maxLat float64
}

var regions = []region{
	{
		id: "boston", state: "25",
		counties: []string{"025"}, // Suffolk County, MA
		minLon:   -71.25, minLat: 42.19, maxLon: -70.88, maxLat: 42.48,
	},
	{
		id: "bayarea", state: "06",
		// Alameda, Contra Costa, Marin, Napa, San Francisco, San Mateo,
		// Santa Clara, Solano, Sonoma.
		counties: []string{"001", "013", "041", "055", "075", "081", "085", "095", "097"},
		minLon:   -123.60, minLat: 36.85, maxLon: -121.20, maxLat: 38.95,
	},
}

// ---------------------------------------------------------------- quadkeys
//
// Ookla publishes one global Parquet per quarter, ~350MB, with a `quadkey`
// column and a `tile` WKT polygon. The files are ordered by quadkey, so Parquet
// row-group statistics on that column let a prefix filter prune almost every
// row group — DuckDB then fetches only the surviving byte ranges over HTTP.
// That is how a 350MB remote file becomes a few MB of transfer.
//
// A quadkey is the Bing Maps tile id: a base-4 string where each character
// subdivides the previous tile into quadrants, so a prefix IS a bounding
// region. Covering each metro at a coarse zoom yields a handful of prefixes.
const quadkeyZoom = 9

func lonLatToTile(lon, lat float64, zoom int) (int, int) {
	// Clamp to the Web Mercator limits; the projection is undefined past them.
	lat = math.Max(-85.05112878, math.Min(85.05112878, lat))
	lon = math.Max(-180, math.Min(180, lon))

	sinLat := math.Sin(lat * math.Pi / 180)
	x := (lon + 180) / 360
	y := 0.5 - math.Log((1+sinLat)/(1-sinLat))/(4*math.Pi)

	n := 1 << zoom
	clamp := func(v float64) int {
		i := int(v * float64(n))
		return max(0, min(n-1, i))
	}
	return clamp(x), clamp(y)
}

func quadkeyOf(x, y, zoom int) string {
	var sb strings.Builder
	for i := zoom; i > 0; i-- {
		digit := byte('0')
		mask := 1 << (i - 1)
		if x&mask != 0 {
			digit++
		}
		if y&mask != 0 {
			digit += 2
		}
		sb.WriteByte(digit)
	}
	return sb.String()
}

// coveringQuadkeys returns the zoom-level tiles that cover a region's bbox.
func (r region) coveringQuadkeys(zoom int) []string {
	x0, y0 := lonLatToTile(r.minLon, r.maxLat, zoom) // NW corner
	x1, y1 := lonLatToTile(r.maxLon, r.minLat, zoom) // SE corner

	var out []string
	for x := min(x0, x1); x <= max(x0, x1); x++ {
		for y := min(y0, y1); y <= max(y0, y1); y++ {
			out = append(out, quadkeyOf(x, y, zoom))
		}
	}
	return out
}

// allCoveringQuadkeys is every metro's covering set, deduplicated.
func allCoveringQuadkeys(zoom int) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range regions {
		for _, qk := range r.coveringQuadkeys(zoom) {
			if !seen[qk] {
				seen[qk] = true
				out = append(out, qk)
			}
		}
	}
	return out
}

// ------------------------------------------------------------ ookla extract

// ooklaQuarterURL is the published location of one quarter's fixed-broadband
// tile file. These are immutable once published and retained indefinitely,
// which is what makes storing an extract instead of the whole body safe.
func ooklaQuarterURL(year int, quarter int) string {
	// The filename carries the first day of the quarter.
	month := (quarter-1)*3 + 1
	return fmt.Sprintf(
		"https://ookla-open-data.s3.amazonaws.com/parquet/performance/"+
			"type=fixed/year=%d/quarter=%d/%d-%02d-01_performance_fixed_tiles.parquet",
		year, quarter, year, month)
}

func quarterOf(t time.Time) (int, int) {
	return t.Year(), (int(t.Month())-1)/3 + 1
}

// previousQuarter steps back one quarter, because the current quarter's file
// does not exist until well after the quarter begins.
func previousQuarter(year, quarter int) (int, int) {
	if quarter == 1 {
		return year - 1, 4
	}
	return year, quarter - 1
}

// ooklaFetch pulls only the tiles covering our metros out of the global file.
//
// This deliberately does NOT follow the "raw means raw" rule the GTFS feeds
// follow, and the distinction is about replayability rather than purity. A
// GTFS-RT response is gone thirty seconds after it is served, so raw has to be
// the replay buffer or the data is lost. Ookla's quarterly files are immutable
// and archived by the publisher, so upstream IS the replay buffer: a changed
// extract can always be re-derived by re-reading the same URL. Storing 350MB a
// quarter to preserve a guarantee we already have from the publisher would cost
// ~1.4GB a year for no benefit.
//
// The provenance of each extract is recorded alongside it so that re-derivation
// is mechanical rather than archaeological.
func ooklaFetch(ctx context.Context, _ *http.Client, _, lake string,
	f feed, at time.Time) (string, int64, error) {
	year, quarter := previousQuarter(quarterOf(at))
	src := ooklaQuarterURL(year, quarter)

	db, err := openDuckDB(lake)
	if err != nil {
		return "", 0, fmt.Errorf("duckdb unavailable: %w", err)
	}
	defer func() { _ = db.Close() }()

	// httpfs is what turns read_parquet(<url>) into range requests. It is baked
	// into the image at build time: the production container is read-only, so
	// INSTALL at runtime cannot write the extension directory.
	if _, err := db.ExecContext(ctx, `LOAD httpfs`); err != nil {
		return "", 0, fmt.Errorf("load httpfs (is it baked into the image?): %w", err)
	}

	var prefixes []string
	for _, qk := range allCoveringQuadkeys(quadkeyZoom) {
		prefixes = append(prefixes, fmt.Sprintf("quadkey LIKE '%s%%'", qk))
	}
	if len(prefixes) == 0 {
		return "", 0, fmt.Errorf("no regions configured")
	}

	dir, name := snapshotPath(lake, f, at)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", 0, err
	}
	tmp, err := os.CreateTemp(dir, ".partial-*")
	if err != nil {
		return "", 0, err
	}
	tmpName := tmp.Name()
	_ = tmp.Close()
	defer func() { _ = os.Remove(tmpName) }() // no-op once renamed

	// The quarter is stamped into every row so that a reader never has to
	// recover it from the filename.
	stmt := fmt.Sprintf(`
COPY (
  SELECT
    quadkey, tile,
    avg_d_kbps, avg_u_kbps, avg_lat_ms,
    tests, devices,
    %d AS src_year, %d AS src_quarter, DATE '%d-%02d-01' AS src_quarter_start
  FROM read_parquet('%s')
  WHERE %s
) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		year, quarter, year, (quarter-1)*3+1,
		src, strings.Join(prefixes, " OR "), tmpName)

	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return "", 0, fmt.Errorf("ookla extract: %w", err)
	}

	info, err := os.Stat(tmpName)
	if err != nil {
		return "", 0, err
	}
	if info.Size() == 0 {
		return "", 0, fmt.Errorf("ookla extract produced no bytes")
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return "", 0, err
	}
	if f.dedupe && sameAsNewest(lake, f, tmpName) {
		return "", info.Size(), nil
	}

	final := filepath.Join(dir, name)
	if err := os.Rename(tmpName, final); err != nil {
		return "", 0, err
	}
	// Provenance: the exact upstream object this extract came from. Without
	// this, "re-derive from source" means guessing which quarter a file was.
	_ = os.WriteFile(final+".source", []byte(src+"\n"), 0o644)
	return final, info.Size(), nil
}

// ------------------------------------------------------------ tract geometry

// extractTracts turns a TIGER shapefile zip into bronze: one row per census
// tract, with its boundary as WKT.
//
// Parquet rather than the NDJSON the GTFS tables use, because a tract polygon
// is tens of kilobytes of coordinates and there are thousands of them. The
// geometry is stored as WKT text rather than a native geometry type so the
// file stays readable by a plain Parquet reader — including DuckDB-WASM in the
// browser, which is not guaranteed to have the spatial extension loaded.
//
// The shapefile is read in place out of the zip through GDAL's /vsizip/ layer.
// Unpacking first would mean writing ~100MB of .shp/.dbf/.shx to the lake for
// a file we read exactly once.
func extractTracts(ctx context.Context, lake, rawZip string) error {
	_, feedName := agencyAndFeed(lake, rawZip)
	stateFIPS := strings.TrimPrefix(feedName, "tiger_tracts_")
	if len(stateFIPS) != 2 {
		return fmt.Errorf("cannot infer state FIPS from feed %q", feedName)
	}

	// Only the counties we actually cover. CA ships ~9,100 tracts statewide and
	// we want nine counties' worth; carrying the rest would bloat every
	// downstream join for no analytical gain.
	var counties []string
	for _, r := range regions {
		if r.state == stateFIPS {
			counties = append(counties, r.counties...)
		}
	}
	if len(counties) == 0 {
		return fmt.Errorf("no configured region uses state %s", stateFIPS)
	}
	quoted := make([]string, len(counties))
	for i, c := range counties {
		quoted[i] = "'" + c + "'"
	}

	outDir := strings.Replace(filepath.Dir(rawZip),
		string(filepath.Separator)+"raw"+string(filepath.Separator),
		string(filepath.Separator)+"bronze"+string(filepath.Separator), 1)
	out := filepath.Join(outDir,
		strings.TrimSuffix(filepath.Base(rawZip), ".zip")+".parquet")
	if _, err := os.Stat(out); err == nil {
		return nil // already extracted
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	db, err := openDuckDB(lake)
	if err != nil {
		return fmt.Errorf("duckdb unavailable: %w", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `LOAD spatial`); err != nil {
		return fmt.Errorf("load spatial (is it baked into the image?): %w", err)
	}

	shp := fmt.Sprintf("/vsizip/%s/tl_2024_%s_tract.shp", rawZip, stateFIPS)
	tmp := filepath.Join(outDir, ".partial-tracts.parquet")
	defer func() { _ = os.Remove(tmp) }()

	stmt := fmt.Sprintf(`
COPY (
  SELECT
    GEOID        AS geoid,
    STATEFP      AS state_fips,
    COUNTYFP     AS county_fips,
    TRACTCE      AS tract_code,
    NAMELSAD     AS tract_name,
    CAST(ALAND  AS BIGINT) AS land_m2,
    CAST(AWATER AS BIGINT) AS water_m2,
    CAST(INTPTLAT AS DOUBLE) AS centroid_lat,
    CAST(INTPTLON AS DOUBLE) AS centroid_lon,
    ST_AsText(geom) AS geometry_wkt
  FROM ST_Read('%s')
  WHERE COUNTYFP IN (%s)
) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		shp, strings.Join(quoted, ","), tmp)

	if _, err := db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("read %s: %w", shp, err)
	}
	if err := os.Chmod(tmp, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, out)
}

// ----------------------------------------------------------- source catalog

// acsVariables is what we pull per tract.
//
// B28002 is the quietly important one: it is the only public measure of what
// households actually SUBSCRIBE to, as opposed to what a provider says is
// available to them. A tract can be 100% "served" on paper, post decent speed
// tests from the few who are connected, and still have a third of households
// with no broadband at all.
var acsVariables = []string{
	"B01003_001E", // total population
	"B11001_001E", // total households
	"B19013_001E", // median household income
	"B03002_003E", // white alone, not hispanic or latino
	"B03002_004E", // black or african american alone
	"B03002_006E", // asian alone
	"B03002_012E", // hispanic or latino
	"B28002_001E", // internet subscription: total households
	"B28002_004E", // ... with a broadband subscription
	"B28002_013E", // ... with no internet access at all
}

const acsYear = "2023"

func acsURL(r region) string {
	return fmt.Sprintf(
		"https://api.census.gov/data/%s/acs/acs5?get=NAME,%s&for=tract:*&in=state:%s&in=county:%s&key={key}",
		acsYear, strings.Join(acsVariables, ","), r.state, strings.Join(r.counties, ","))
}

// tigerURL is the census tract boundary file for a state. Public, no key.
func tigerURL(stateFIPS string) string {
	return fmt.Sprintf(
		"https://www2.census.gov/geo/tiger/TIGER2024/TRACT/tl_2024_%s_tract.zip",
		stateFIPS)
}

// broadbandFeeds is appended to feeds in init().
//
// Cadence reflects how often each publisher actually moves:
//   - FCC BDC       twice a year
//   - Ookla         quarterly
//   - ACS 5-year    annually
//   - TIGER tracts  annually, and boundaries change only after a census
//
// Each is polled far more often than it changes and deduplicated on content,
// so a new release is picked up within days without storing repeats.
func broadbandFeeds() []feed {
	out := []feed{
		{
			agency: "ookla",
			name:   "fixed_performance",
			// url is unused: ooklaFetch derives the quarter's object itself.
			url:    "https://ookla-open-data.s3.amazonaws.com/",
			ext:    "parquet",
			every:  7 * 24 * time.Hour,
			dedupe: true,
			fetch:  ooklaFetch,
		},
	}

	// Census tract geometry, one file per state we cover.
	for _, state := range []string{"25", "06"} {
		out = append(out, feed{
			agency: "census",
			name:   "tiger_tracts_" + state,
			url:    tigerURL(state),
			ext:    "zip",
			every:  30 * 24 * time.Hour,
			dedupe: true,
		})
	}

	// ACS demographics. Needs a free API key; skipped until one exists.
	for _, r := range regions {
		out = append(out, feed{
			agency: "census",
			name:   "acs5_" + r.id,
			url:    acsURL(r),
			secret: "census_api_key",
			ext:    "json",
			every:  7 * 24 * time.Hour,
			dedupe: true,
		})
	}

	// FCC BDC. Credentials are headers, not a URL parameter, so the token
	// never lands in a log line or a stack trace.
	//
	// UNVERIFIED: the FCC API could not be reached while this was written —
	// it returns 401 without credentials and the host runs bot management, so
	// the two-step list-then-download flow has not been exercised against the
	// real service. If it fails once credentials exist, the fallback is to
	// download the periodic files by hand into raw/fcc/<name>/dt=.../ and let
	// bronze pick them up; everything downstream reads the lake, not the API.
	for _, r := range regions {
		out = append(out, feed{
			agency: "fcc",
			name:   "bdc_availability_" + r.id,
			url:    fccAvailabilityURL(r),
			auth: []authHeader{
				{header: "username", secret: "fcc_username"},
				{header: "hash_value", secret: "fcc_hash_value"},
			},
			ext:    "csv",
			every:  7 * 24 * time.Hour,
			dedupe: true,
		})
	}
	return out
}

// fccAvailabilityURL targets the public area-summary endpoint rather than
// location-level data. That is a licensing decision as much as a volume one:
// the location-level Broadband Serviceable Location Fabric requires a signed
// agreement and may not be redistributed, while area summaries are public and
// already aggregate to the geography we key on.
func fccAvailabilityURL(r region) string {
	return fmt.Sprintf(
		"https://broadbandmap.fcc.gov/api/public/map/downloads/listAvailabilityData/"+
			"%s?category=Summary&subcategory=Summary%%20by%%20Geography%%20Type%%20-%%20Other%%20Geographies&stateFips=%s",
		fccAsOfDate, r.state)
}

// fccAsOfDate is the BDC release to pull. The FCC publishes as-of June 30 and
// December 31 each year, roughly six months in arrears.
const fccAsOfDate = "2025-06-30"

func init() {
	feeds = append(feeds, broadbandFeeds()...)
}
