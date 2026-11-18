package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func cleanerCrashOptions() Options {
	return Options{MaxRecordBytes: 4096, TargetSegmentBytes: 16 << 10}
}

func seedCleanerCrashImage(t *testing.T, dir string) {
	t.Helper()
	db, err := OpenWithIO(dir, cleanerCrashOptions(), &storage.IO{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"keep", "hot", "gone", "hot", "again", "hot"} {
		if err := db.Put([]byte(key), bytes.Repeat([]byte{byte(i)}, 3000)); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"gone", "again"} {
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	mustPut(t, db, "again", "new")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanerPublicationCrashMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess crash sweep")
	}
	for _, phase := range []string{"clean.output.sync", "clean.catalog.sync", "clean.catalog.dirsync",
		"clean.current.sync", "clean.current.rename", "clean.current.dirsync", "clean.retire.unlink", "clean.retire.dirsync", "clean.remap"} {
		for _, edge := range []string{":before", ":after"} {
			if phase == "clean.remap" && edge == ":before" {
				continue
			}
			point := phase + edge
			t.Run(point, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "db")
				seedCleanerCrashImage(t, dir)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCleanerCrashChild$")
				cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0", "TARP_CLEANER_CRASH_DB="+dir, "TARP_CLEANER_CRASH_POINT="+point)
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 91 {
					t.Fatalf("interruption at %s: %v\n%s", point, err, output)
				}
				// Stop further physical cleaning during inspection so either permissible
				// on-disk selection must stand on its own.
				db, err := OpenWithIO(dir, cleanerCrashOptions(), &storage.IO{CleanerStart: make(chan struct{})})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if db.store.Current.Sequence != 9 || len(db.index) != 3 {
					t.Fatal("lost the acknowledged frontier or key set")
				}
				for k, v := range map[string][]byte{"hot": bytes.Repeat([]byte{5}, 3000), "keep": make([]byte, 3000), "again": []byte("new")} {
					got, err := db.Get([]byte(k))
					if err != nil || !bytes.Equal(got, v) {
						t.Fatalf("recovery changed key %q: %v", k, err)
					}
				}
				if _, err := db.Get([]byte("gone")); !errors.Is(err, ErrNotFound) {
					t.Fatal("deleted key reappeared", err)
				}
				mustPut(t, db, "next", "")
				if db.store.Current.Sequence != 10 {
					t.Fatal("incorrect next sequence")
				}
			})
		}
	}
}

func TestCleanerCrashChild(t *testing.T) {
	dir := os.Getenv("TARP_CLEANER_CRASH_DB")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	point := os.Getenv("TARP_CLEANER_CRASH_POINT")
	fs := &storage.IO{Hook: func(p string) error {
		if p == point {
			os.Exit(91)
		}
		return nil
	}}
	db, err := OpenWithIO(dir, cleanerCrashOptions(), fs)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-db.cleaner.done:
		t.Fatalf("worker stopped before cut %s: %+v", point, db.MaintenanceStatus())
	case <-time.After(4 * time.Second):
		t.Fatalf("cut %s not reached: %+v", point, db.MaintenanceStatus())
	}
}
