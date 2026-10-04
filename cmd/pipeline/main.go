// Command pipeline runs the ingest and transform DAG.
//
// Stub. It writes a marker file into the lake root and exits, which is enough
// to prove the one thing most likely to be misconfigured: that this container
// can *write* /lake while the portal mounts the same directory :ro.
//
// Both containers run as uid 10001 (Dockerfile USER), and the host owns /lake
// as uid 10001 via the `portal` account created in infra/ansible/playbook.yml.
// If those numbers ever drift apart, this binary fails with EACCES and tells
// you immediately — which is the point of running it before writing the real
// raw -> bronze -> silver -> gold transforms.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultLakeDir = "/lake"
	markerName     = "_pipeline_last_run"
)

func main() {
	if err := run(); err != nil {
		slog.Error("pipeline failed", "err", err)
		os.Exit(1)
	}
}

func lakeDir() string {
	if d := os.Getenv("LAKE_DIR"); d != "" {
		return d
	}
	return defaultLakeDir
}

func run() error {
	dir := lakeDir()

	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("stat lake dir %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("lake dir %s is not a directory", dir)
	}

	marker := filepath.Join(dir, markerName)
	stamp := time.Now().UTC().Format(time.RFC3339)

	if err := os.WriteFile(marker, []byte(stamp+"\n"), 0o644); err != nil {
		return fmt.Errorf("write marker (is /lake writable by uid %d?): %w",
			os.Getuid(), err)
	}

	slog.Info("pipeline ok", "marker", marker, "stamp", stamp, "uid", os.Getuid())
	return nil
}
