package main

// Bronze: parse the raw GTFS-Realtime protobuf snapshots into one JSON object
// per line, which DuckDB reads natively with read_json_auto().
//
// Raw stays untouched and authoritative. Bronze is derived, so it can always
// be deleted and rebuilt — which is the whole reason ingest writes the bytes
// verbatim instead of parsing at fetch time. Changing a field mapping here is
// a re-run, not a lost day of data.
//
// Layout mirrors raw exactly:
//
//	/lake/raw/mbta/vehicle_positions/dt=2026-10-07/20261007T050000Z.pb
//	/lake/bronze/mbta/vehicle_positions/dt=2026-10-07/20261007T050000Z.ndjson

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

const bronzeWorkers = 4

// vehicleRow is one vehicle at one instant: where a train actually is.
// Pointers where absent must be distinguishable from zero — (0,0) is a real
// coordinate in the Gulf of Guinea, and a 0s delay is not the same as no
// prediction.
type vehicleRow struct {
	Agency        string   `json:"agency"`
	FeedTimestamp int64    `json:"feed_timestamp"`
	VehicleID     string   `json:"vehicle_id,omitempty"`
	Label         string   `json:"label,omitempty"`
	TripID        string   `json:"trip_id,omitempty"`
	RouteID       string   `json:"route_id,omitempty"`
	DirectionID   *uint32  `json:"direction_id,omitempty"`
	Latitude      *float32 `json:"latitude,omitempty"`
	Longitude     *float32 `json:"longitude,omitempty"`
	Bearing       *float32 `json:"bearing,omitempty"`
	SpeedMPS      *float32 `json:"speed_mps,omitempty"`
	StopID        string   `json:"stop_id,omitempty"`
	CurrentStatus string   `json:"current_status,omitempty"`
	VehicleTime   *int64   `json:"vehicle_timestamp,omitempty"`
}

// stopTimeRow is one predicted arrival/departure. BART publishes no vehicle
// positions at all, so for BART these predictions are the only signal about
// where trains are.
type stopTimeRow struct {
	Agency         string  `json:"agency"`
	FeedTimestamp  int64   `json:"feed_timestamp"`
	TripID         string  `json:"trip_id,omitempty"`
	RouteID        string  `json:"route_id,omitempty"`
	DirectionID    *uint32 `json:"direction_id,omitempty"`
	VehicleID      string  `json:"vehicle_id,omitempty"`
	StopID         string  `json:"stop_id,omitempty"`
	StopSequence   *uint32 `json:"stop_sequence,omitempty"`
	ArrivalTime    *int64  `json:"arrival_time,omitempty"`
	ArrivalDelay   *int32  `json:"arrival_delay,omitempty"`
	DepartureTime  *int64  `json:"departure_time,omitempty"`
	DepartureDelay *int32  `json:"departure_delay,omitempty"`
	Relationship   string  `json:"schedule_relationship,omitempty"`
}

// transformBronze walks every raw snapshot and writes any that don't yet have
// a bronze counterpart. That makes it both incremental and a backfill: point
// it at two days of accumulated .pb files and it catches up on its own.
func transformBronze(ctx context.Context, lake string) {
	rawRoot := filepath.Join(lake, "raw")
	if _, err := os.Stat(rawRoot); err != nil {
		return
	}

	var done, skipped, failed int
	start := time.Now()

	// Bronze expires sooner than raw. Without this, every cycle would re-derive
	// bronze for days the pruner just deleted, forever.
	bronzeCutoff := time.Now().UTC().AddDate(0, 0,
		-retentionDays("BRONZE_RETENTION_DAYS", defaultBronzeRetentionDays))

	var files []string
	err := filepath.WalkDir(rawRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			return nil
		}

		// Static GTFS arrives as a zip of CSVs rather than protobuf, so it
		// takes a different path out of raw.
		if strings.HasSuffix(path, ".zip") {
			if err := extractStatic(lake, path); err != nil {
				slog.Warn("static extract failed", "file", path, "err", err)
				failed++
			}
			return nil
		}
		if !strings.HasSuffix(path, ".pb") {
			return nil
		}
		if expiredPartition(path, bronzeCutoff) {
			return nil
		}

		files = append(files, path)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("bronze walk ended early", "err", err)
	}

	// Decoding is CPU-bound and each snapshot is independent. A small fixed
	// pool keeps a backfill from creating one goroutine per file while making
	// catch-up substantially faster on multi-core hosts.
	workers := min(bronzeWorkers, len(files))
	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	worker := func() {
		defer wg.Done()
		for path := range jobs {
			if ctx.Err() != nil {
				return
			}
			out, err := bronzePath(lake, path)
			if err != nil {
				continue
			}
			if _, err := os.Stat(out); err == nil {
				mu.Lock()
				skipped++
				mu.Unlock()
				continue
			}
			_, err = transformFile(lake, path, out)
			mu.Lock()
			if err != nil {
				failed++
			} else {
				done++
			}
			mu.Unlock()
			if err != nil {
				slog.Warn("bronze transform failed", "file", path, "err", err)
			}
		}
	}
	wg.Add(workers)
	for range workers {
		go worker()
	}
send:
	for _, path := range files {
		select {
		case jobs <- path:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()

	if done > 0 || failed > 0 {
		slog.Info("bronze",
			"written", done, "already_present", skipped, "failed", failed,
			"elapsed", time.Since(start).Round(time.Millisecond))
	}
}

// expiredPartition reports whether a file sits in a dt= partition older than
// the cutoff. Paths without a recognisable partition are never skipped.
func expiredPartition(path string, cutoff time.Time) bool {
	for _, seg := range strings.Split(filepath.ToSlash(path), "/") {
		if !dtPartition.MatchString(seg) {
			continue
		}
		day, err := time.Parse("2006-01-02", seg[len("dt="):])
		return err == nil && day.Before(cutoff)
	}
	return false
}

// bronzePath maps a raw snapshot to its bronze output, preserving the
// agency/feed/dt= structure so both layers partition identically.
func bronzePath(lake, rawFile string) (string, error) {
	rel, err := filepath.Rel(filepath.Join(lake, "raw"), rawFile)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("%s is outside the raw tree", rawFile)
	}
	rel = strings.TrimSuffix(rel, ".pb") + ".ndjson"
	return filepath.Join(lake, "bronze", rel), nil
}

// agencyAndFeed pulls the partition keys back out of a raw path:
// <lake>/raw/<agency>/<feed>/dt=.../<ts>.pb
func agencyAndFeed(lake, rawFile string) (agency, feedName string) {
	rel, err := filepath.Rel(filepath.Join(lake, "raw"), rawFile)
	if err != nil {
		return "", ""
	}
	parts := strings.Split(filepath.ToSlash(rel), "/")
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func transformFile(lake, rawFile, outFile string) (int, error) {
	data, err := os.ReadFile(rawFile)
	if err != nil {
		return 0, err
	}

	agency, feedName := agencyAndFeed(lake, rawFile)
	rowCount, write, err := decodeSnapshot(data, agency, feedName)
	if err != nil {
		return 0, err
	}
	if write == nil {
		return 0, nil
	}
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		return 0, err
	}
	return rowCount, write(outFile)
}

// decodeSnapshot contains no filesystem work. Keeping protobuf decoding and
// row mapping separate from publication makes the transform easy to test and
// ensures a failed decode can never create a bronze artifact.
func decodeSnapshot(data []byte, agency, feedName string) (int, func(string) error, error) {
	var msg gtfs.FeedMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		return 0, nil, fmt.Errorf("unmarshal: %w", err)
	}

	feedTS := int64(msg.GetHeader().GetTimestamp())

	var write func(string) error
	var rowCount int
	switch feedName {
	case "vehicle_positions":
		rows := vehicleRows(&msg, agency, feedTS)
		rowCount = len(rows)
		write = func(path string) error { return writeNDJSON(path, rows) }
	case "trip_updates":
		rows := stopTimeRows(&msg, agency, feedTS)
		rowCount = len(rows)
		write = func(path string) error { return writeNDJSON(path, rows) }
	default:
		// alerts and anything else: not modelled yet. Raw is still kept, so
		// this becomes a re-run whenever we decide to parse them.
		return 0, nil, nil
	}
	return rowCount, write, nil
}

func vehicleRows(msg *gtfs.FeedMessage, agency string, feedTS int64) []vehicleRow {
	rows := make([]vehicleRow, 0, len(msg.GetEntity()))
	for _, ent := range msg.GetEntity() {
		vp := ent.GetVehicle()
		if vp == nil {
			continue
		}
		r := vehicleRow{
			Agency:        agency,
			FeedTimestamp: feedTS,
			VehicleID:     vp.GetVehicle().GetId(),
			Label:         vp.GetVehicle().GetLabel(),
			TripID:        vp.GetTrip().GetTripId(),
			RouteID:       vp.GetTrip().GetRouteId(),
			StopID:        vp.GetStopId(),
		}
		if vp.Trip != nil && vp.Trip.DirectionId != nil {
			d := vp.Trip.GetDirectionId()
			r.DirectionID = &d
		}
		if p := vp.GetPosition(); p != nil {
			lat, lon := p.GetLatitude(), p.GetLongitude()
			r.Latitude, r.Longitude = &lat, &lon
			if p.Bearing != nil {
				b := p.GetBearing()
				r.Bearing = &b
			}
			if p.Speed != nil {
				s := p.GetSpeed()
				r.SpeedMPS = &s
			}
		}
		if vp.CurrentStatus != nil {
			r.CurrentStatus = vp.GetCurrentStatus().String()
		}
		if vp.Timestamp != nil {
			t := int64(vp.GetTimestamp())
			r.VehicleTime = &t
		}
		rows = append(rows, r)
	}
	return rows
}

func stopTimeRows(msg *gtfs.FeedMessage, agency string, feedTS int64) []stopTimeRow {
	rows := make([]stopTimeRow, 0)
	for _, ent := range msg.GetEntity() {
		tu := ent.GetTripUpdate()
		if tu == nil {
			continue
		}
		base := stopTimeRow{
			Agency:        agency,
			FeedTimestamp: feedTS,
			TripID:        tu.GetTrip().GetTripId(),
			RouteID:       tu.GetTrip().GetRouteId(),
			VehicleID:     tu.GetVehicle().GetId(),
		}
		if tu.Trip != nil && tu.Trip.DirectionId != nil {
			d := tu.Trip.GetDirectionId()
			base.DirectionID = &d
		}

		for _, stu := range tu.GetStopTimeUpdate() {
			r := base
			r.StopID = stu.GetStopId()
			if stu.StopSequence != nil {
				s := stu.GetStopSequence()
				r.StopSequence = &s
			}
			if a := stu.GetArrival(); a != nil {
				if a.Time != nil {
					t := a.GetTime()
					r.ArrivalTime = &t
				}
				if a.Delay != nil {
					d := a.GetDelay()
					r.ArrivalDelay = &d
				}
			}
			if dp := stu.GetDeparture(); dp != nil {
				if dp.Time != nil {
					t := dp.GetTime()
					r.DepartureTime = &t
				}
				if dp.Delay != nil {
					d := dp.GetDelay()
					r.DepartureDelay = &d
				}
			}
			if stu.ScheduleRelationship != nil {
				r.Relationship = stu.GetScheduleRelationship().String()
			}
			rows = append(rows, r)
		}
	}
	return rows
}

// writeNDJSON publishes atomically, same as ingest: a half-written bronze file
// would otherwise be indistinguishable from a complete one to a query.
func writeNDJSON[T any](outFile string, rows []T) error {
	dir := filepath.Dir(outFile)
	tmp, err := os.CreateTemp(dir, ".partial-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	enc := json.NewEncoder(tmp)
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, outFile)
}
