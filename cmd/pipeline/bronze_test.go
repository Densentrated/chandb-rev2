package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

func TestBronzePathMirrorsRaw(t *testing.T) {
	got, err := bronzePath("/lake",
		"/lake/raw/mbta/vehicle_positions/dt=2026-10-07/20261007T050000Z.pb")
	if err != nil {
		t.Fatal(err)
	}
	want := "/lake/bronze/mbta/vehicle_positions/dt=2026-10-07/20261007T050000Z.ndjson"
	if got != want {
		t.Errorf("bronzePath = %q, want %q", got, want)
	}
}

func TestBronzePathRejectsOutsideRaw(t *testing.T) {
	if _, err := bronzePath("/lake", "/etc/passwd"); err == nil {
		t.Error("expected an error for a path outside the raw tree")
	}
}

func TestAgencyAndFeed(t *testing.T) {
	a, f := agencyAndFeed("/lake",
		"/lake/raw/bart/trip_updates/dt=2026-10-07/x.pb")
	if a != "bart" || f != "trip_updates" {
		t.Errorf("got (%q, %q), want (bart, trip_updates)", a, f)
	}
}

// writeFeed marshals a FeedMessage to a raw-layout path, as ingest would.
//
// gtfs_realtime_version is `required` in the proto2 schema, so Marshal refuses
// without it. Filling it in here keeps every fixture below focused on the
// field under test.
func writeFeed(t *testing.T, lake, agency, feedName string, msg *gtfs.FeedMessage) string {
	t.Helper()
	if msg.Header == nil {
		msg.Header = &gtfs.FeedHeader{}
	}
	if msg.Header.GtfsRealtimeVersion == nil {
		msg.Header.GtfsRealtimeVersion = proto.String("2.0")
	}
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(lake, "raw", agency, feedName, "dt=2026-10-07")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "20261007T050000Z.pb")
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestTransformVehiclePositions(t *testing.T) {
	lake := t.TempDir()
	raw := writeFeed(t, lake, "mbta", "vehicle_positions", &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           proto.Uint64(1791139958),
		},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("y1"),
			Vehicle: &gtfs.VehiclePosition{
				Trip: &gtfs.TripDescriptor{
					TripId:      proto.String("78953782"),
					RouteId:     proto.String("714"),
					DirectionId: proto.Uint32(1),
				},
				Vehicle: &gtfs.VehicleDescriptor{
					Id:    proto.String("dLF 957"),
					Label: proto.String("LF 957"),
				},
				Position: &gtfs.Position{
					Latitude:  proto.Float32(42.27868),
					Longitude: proto.Float32(-70.86715),
					Bearing:   proto.Float32(119),
				},
				StopId:        proto.String("71420"),
				CurrentStatus: gtfs.VehiclePosition_IN_TRANSIT_TO.Enum(),
				Timestamp:     proto.Uint64(1791139956),
			},
		}},
	})

	out, err := bronzePath(lake, raw)
	if err != nil {
		t.Fatal(err)
	}
	n, err := transformFile(lake, raw, out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("got %d rows, want 1", n)
	}

	var row vehicleRow
	line, _, _ := strings.Cut(readFile(t, out), "\n")
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		t.Fatal(err)
	}

	if row.Agency != "mbta" {
		t.Errorf("agency = %q, want mbta", row.Agency)
	}
	if row.Latitude == nil || *row.Latitude != 42.27868 {
		t.Errorf("latitude = %v, want 42.27868", row.Latitude)
	}
	if row.CurrentStatus != "IN_TRANSIT_TO" {
		t.Errorf("current_status = %q, want IN_TRANSIT_TO", row.CurrentStatus)
	}
	if row.FeedTimestamp != 1791139958 {
		t.Errorf("feed_timestamp = %d, want 1791139958", row.FeedTimestamp)
	}
}

// A vehicle with no Position must emit no coordinates at all. (0,0) is a real
// place in the Gulf of Guinea, so defaulting would silently invent a location.
func TestTransformOmitsAbsentPosition(t *testing.T) {
	lake := t.TempDir()
	raw := writeFeed(t, lake, "mbta", "vehicle_positions", &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{Timestamp: proto.Uint64(1)},
		Entity: []*gtfs.FeedEntity{{
			Id:      proto.String("y1"),
			Vehicle: &gtfs.VehiclePosition{Vehicle: &gtfs.VehicleDescriptor{Id: proto.String("v")}},
		}},
	})
	out, _ := bronzePath(lake, raw)
	if _, err := transformFile(lake, raw, out); err != nil {
		t.Fatal(err)
	}

	body := readFile(t, out)
	if strings.Contains(body, "latitude") || strings.Contains(body, "longitude") {
		t.Errorf("absent position must be omitted, got: %s", body)
	}
}

// One row per stop_time_update, not per trip.
func TestTransformTripUpdatesFansOutPerStop(t *testing.T) {
	lake := t.TempDir()
	raw := writeFeed(t, lake, "bart", "trip_updates", &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{Timestamp: proto.Uint64(1791139963)},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("t1"),
			TripUpdate: &gtfs.TripUpdate{
				Trip: &gtfs.TripDescriptor{TripId: proto.String("1973503")},
				StopTimeUpdate: []*gtfs.TripUpdate_StopTimeUpdate{
					{
						StopId:    proto.String("A10-1"),
						Arrival:   &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(1791139909), Delay: proto.Int32(36)},
						Departure: &gtfs.TripUpdate_StopTimeEvent{Time: proto.Int64(1791139945), Delay: proto.Int32(36)},
					},
					{StopId: proto.String("A20-1")},
				},
			},
		}},
	})

	out, _ := bronzePath(lake, raw)
	n, err := transformFile(lake, raw, out)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("got %d rows, want 2 (one per stop_time_update)", n)
	}

	var row stopTimeRow
	line, _, _ := strings.Cut(readFile(t, out), "\n")
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		t.Fatal(err)
	}
	if row.StopID != "A10-1" || row.ArrivalDelay == nil || *row.ArrivalDelay != 36 {
		t.Errorf("unexpected first row: %+v", row)
	}
}

// Unmodelled feeds (alerts) must not fail the run — raw is still kept, so
// parsing them later is a re-run.
func TestTransformIgnoresUnmodelledFeeds(t *testing.T) {
	lake := t.TempDir()
	raw := writeFeed(t, lake, "bart", "alerts", &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{Timestamp: proto.Uint64(1)},
	})
	out, _ := bronzePath(lake, raw)
	n, err := transformFile(lake, raw, out)
	if err != nil {
		t.Errorf("unmodelled feed should not error, got %v", err)
	}
	if n != 0 {
		t.Errorf("got %d rows, want 0", n)
	}
}

func TestTransformBronzeIsIdempotent(t *testing.T) {
	lake := t.TempDir()
	writeFeed(t, lake, "mbta", "vehicle_positions", &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{Timestamp: proto.Uint64(1)},
		Entity: []*gtfs.FeedEntity{{
			Id: proto.String("y1"),
			Vehicle: &gtfs.VehiclePosition{
				Position: &gtfs.Position{Latitude: proto.Float32(42), Longitude: proto.Float32(-71)},
			},
		}},
	})

	transformBronze(context.Background(), lake)
	out := filepath.Join(lake, "bronze", "mbta", "vehicle_positions",
		"dt=2026-10-07", "20261007T050000Z.ndjson")
	first, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}

	// Second pass must skip, not rewrite.
	transformBronze(context.Background(), lake)
	second, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if !first.ModTime().Equal(second.ModTime()) {
		t.Error("second pass rewrote an existing bronze file")
	}

	matches, _ := filepath.Glob(filepath.Join(lake, "bronze", "**", "*.partial-*"))
	if len(matches) != 0 {
		t.Errorf("left partial files behind: %v", matches)
	}
}

func TestTransformRejectsGarbageProtobuf(t *testing.T) {
	lake := t.TempDir()
	dir := filepath.Join(lake, "raw", "mbta", "vehicle_positions", "dt=2026-10-07")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw := filepath.Join(dir, "20261007T050000Z.pb")
	// Bytes that are not a valid FeedMessage at all.
	if err := os.WriteFile(raw, []byte{0xff, 0xff, 0xff, 0xff, 0xff}, 0o644); err != nil {
		t.Fatal(err)
	}

	out, _ := bronzePath(lake, raw)
	if _, err := transformFile(lake, raw, out); err == nil {
		t.Error("expected an error on undecodable protobuf")
	}
	if _, err := os.Stat(out); err == nil {
		t.Error("a failed transform must not leave an output file")
	}
}

func readFile(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
