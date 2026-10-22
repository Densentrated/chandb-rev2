package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/MobilityData/gtfs-realtime-bindings/golang/gtfs"
	"google.golang.org/protobuf/proto"
)

// BenchmarkBronzeVehicleRows measures the CPU-bound part of protobuf-to-row
// conversion without filesystem noise. The production path retains the raw
// protobuf and writes the resulting rows as NDJSON.
func BenchmarkBronzeVehicleRows(b *testing.B) {
	msg := benchmarkFeed(256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := vehicleRows(msg, "mbta", 1791139958)
		if len(rows) != 256 {
			b.Fatalf("got %d rows, want 256", len(rows))
		}
	}
}

// BenchmarkBronzeNDJSON measures encoding the typed bronze rows. Keeping this
// separate from BenchmarkBronzeVehicleRows makes regressions in allocation or
// JSON shape easier to attribute.
func BenchmarkBronzeNDJSON(b *testing.B) {
	rows := vehicleRows(benchmarkFeed(256), "mbta", 1791139958)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		enc := json.NewEncoder(&out)
		for _, row := range rows {
			if err := enc.Encode(row); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkFeed(n int) *gtfs.FeedMessage {
	msg := &gtfs.FeedMessage{
		Header: &gtfs.FeedHeader{
			GtfsRealtimeVersion: proto.String("2.0"),
			Timestamp:           proto.Uint64(1791139958),
		},
		Entity: make([]*gtfs.FeedEntity, n),
	}
	for i := range msg.Entity {
		msg.Entity[i] = &gtfs.FeedEntity{
			Id: proto.String("vehicle"),
			Vehicle: &gtfs.VehiclePosition{
				Trip: &gtfs.TripDescriptor{
					TripId:      proto.String("trip"),
					RouteId:     proto.String("route"),
					DirectionId: proto.Uint32(1),
				},
				Vehicle: &gtfs.VehicleDescriptor{
					Id:    proto.String("vehicle"),
					Label: proto.String("label"),
				},
				Position: &gtfs.Position{
					Latitude:  proto.Float32(42.3),
					Longitude: proto.Float32(-71.1),
					Bearing:   proto.Float32(90),
					Speed:     proto.Float32(12),
				},
				StopId:        proto.String("stop"),
				CurrentStatus: gtfs.VehiclePosition_IN_TRANSIT_TO.Enum(),
				Timestamp:     proto.Uint64(1791139957),
			},
		}
	}
	return msg
}
