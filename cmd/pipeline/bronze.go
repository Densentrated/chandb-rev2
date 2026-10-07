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
	"time"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

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

		out, err := bronzePath(lake, path)
		if err != nil {
			return nil // not a path we recognise; leave it alone
		}
		if _, err := os.Stat(out); err == nil {
			skipped++
			return nil
		}

		n, err := transformFile(lake, path, out)
		switch {
		case err != nil:
			slog.Warn("bronze transform failed", "file", path, "err", err)
			failed++
		default:
			done++
			_ = n
		}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("bronze walk ended early", "err", err)
	}

	if done > 0 || failed > 0 {
		slog.Info("bronze",
			"written", done, "already_present", skipped, "failed", failed,
			"elapsed", time.Since(start).Round(time.Millisecond))
	}
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

	var msg gtfs.FeedMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		return 0, fmt.Errorf("unmarshal: %w", err)
	}

	agency, feedName := agencyAndFeed(lake, rawFile)
	feedTS := int64(msg.GetHeader().GetTimestamp())

	var rows []any
	switch feedName {
	case "vehicle_positions":
		rows = vehicleRows(&msg, agency, feedTS)
	case "trip_updates":
		rows = stopTimeRows(&msg, agency, feedTS)
	default:
		// alerts and anything else: not modelled yet. Raw is still kept, so
		// this becomes a re-run whenever we decide to parse them.
		return 0, nil
	}

	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		return 0, err
	}
	return len(rows), writeNDJSON(outFile, rows)
}

func vehicleRows(msg *gtfs.FeedMessage, agency string, feedTS int64) []any {
	rows := make([]any, 0, len(msg.GetEntity()))
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

func stopTimeRows(msg *gtfs.FeedMessage, agency string, feedTS int64) []any {
	var rows []any
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
func writeNDJSON(outFile string, rows []any) error {
	dir := filepath.Dir(outFile)
	tmp, err := os.CreateTemp(dir, ".partial-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	enc := json.NewEncoder(tmp)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, outFile)
}
