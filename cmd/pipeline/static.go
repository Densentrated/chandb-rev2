package main

// Static GTFS: unpack the lookup tables the realtime feeds refer to by ID.
//
// The raw zip is 24MB, most of it shapes.txt (route geometry, 16MB). Only the
// three tables below are needed to make a vehicle position legible, so only
// those are extracted. The full zip stays in raw, so pulling out shapes.txt
// later — to draw routes on a map — is a re-run, not a re-fetch.

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// staticTables are the GTFS files extracted to bronze.
var staticTables = []string{"routes.txt", "stops.txt", "trips.txt"}

// nestedStatic names, per agency, a zip INSIDE the raw zip that holds that
// agency's GTFS. SEPTA's gtfs_public.zip contains no tables at its top level,
// only google_bus.zip and google_rail.zip, which are complete, independent
// feeds. Agencies not listed here keep their tables at the top level.
var nestedStatic = map[string]string{
	"septa":     "google_bus.zip",
	"septarail": "google_rail.zip",
}

// maxStaticMemberBytes bounds decompression. A zip member can claim a modest
// compressed size and expand without limit; without a cap, a hostile or
// corrupt feed fills the disk.
const maxStaticMemberBytes = 256 << 20

// extractStatic writes the wanted members of a raw GTFS zip into bronze,
// preserving the dt= partition so a gold query can pick the newest snapshot.
//
//	raw/mbta/gtfs_static/dt=2026-10-07/20261007T050000Z.zip
//	bronze/mbta/gtfs_static/dt=2026-10-07/{routes,stops,trips}.txt
func extractStatic(lake, rawZip string) error {
	rel, err := filepath.Rel(filepath.Join(lake, "raw"), rawZip)
	if err != nil || strings.HasPrefix(rel, "..") {
		return fmt.Errorf("%s is outside the raw tree", rawZip)
	}
	outDir := filepath.Join(lake, "bronze", filepath.Dir(rel))

	// Every wanted table already present means this zip is done.
	done := true
	for _, name := range staticTables {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			done = false
			break
		}
	}
	if done {
		return nil
	}

	zr, err := zip.OpenReader(rawZip)
	if err != nil {
		return err
	}
	defer func() { _ = zr.Close() }()

	src := &zr.Reader
	agency, _ := agencyAndFeed(lake, rawZip)
	if inner, ok := nestedStatic[agency]; ok {
		if src, err = openNested(src, inner); err != nil {
			return fmt.Errorf("%s: %w", rawZip, err)
		}
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	for _, want := range staticTables {
		member := findMember(src, want)
		if member == nil {
			return fmt.Errorf("%s not present in %s", want, rawZip)
		}
		if err := extractMember(member, filepath.Join(outDir, want)); err != nil {
			return fmt.Errorf("extract %s: %w", want, err)
		}
	}
	return nil
}

// openNested opens a zip stored as a member of another zip. archive/zip needs
// random access, so the member is read into memory — under the same
// decompression cap as any other member.
func openNested(zr *zip.Reader, name string) (*zip.Reader, error) {
	member := findMember(zr, name)
	if member == nil {
		return nil, fmt.Errorf("nested %s not present", name)
	}
	rc, err := member.Open()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	b, err := io.ReadAll(io.LimitReader(rc, maxStaticMemberBytes))
	if err != nil {
		return nil, err
	}
	if len(b) == maxStaticMemberBytes {
		return nil, fmt.Errorf("nested %s hit the %d byte decompression cap",
			name, maxStaticMemberBytes)
	}
	return zip.NewReader(bytes.NewReader(b), int64(len(b)))
}

func findMember(zr *zip.Reader, name string) *zip.File {
	for _, f := range zr.File {
		// Compare the base name only: never trust a path from inside an
		// archive, or a member called ../../etc/passwd writes outside the
		// directory we chose.
		if filepath.Base(f.Name) == name {
			return f
		}
	}
	return nil
}

func extractMember(f *zip.File, outFile string) error {
	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	dir := filepath.Dir(outFile)
	tmp, err := os.CreateTemp(dir, ".partial-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	n, err := io.Copy(tmp, io.LimitReader(rc, maxStaticMemberBytes))
	if err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if n == maxStaticMemberBytes {
		return fmt.Errorf("%s hit the %d byte decompression cap",
			f.Name, maxStaticMemberBytes)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, outFile)
}
