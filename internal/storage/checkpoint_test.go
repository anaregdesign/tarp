package storage

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestCheckpointDiscardsOnlyItsOwnSnapshotFiles(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 1024, RecordBytes: 64, TargetSegmentBytes: format.SegmentHeaderBytes + 64, Handles: 1}
	store, _, err := Open(t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	live := memory.New()
	tx := live.Begin()
	for _, key := range []string{"a", "b"} {
		loc, err := store.Append(format.Put, []byte(key), nil, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Put(key, loc.Address())
		if err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	if err := store.Rotate(); err != nil {
		t.Fatal(err)
	}
	capture, err := store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	view := live.View()
	defer view.Close()
	moves := store.NewRelocations()
	defer moves.Close()
	checkpoint := store.NewCheckpoint(capture)
	var reserved []*File
	reserve := func() (*File, error) {
		file, err := store.ReserveSnapshotFile()
		if err != nil {
			return nil, err
		}
		reserved = append(reserved, file)
		if len(reserved) == 2 {
			// The exclusive create must leave this other file untouched.
			err = os.WriteFile(file.Path, []byte("existing"), 0600)
		}
		return file, err
	}
	if err := checkpoint.Copy(t.Context(), memory.New(), view, moves, reserve); !errors.Is(err, os.ErrExist) || len(reserved) != 2 {
		t.Fatalf("copy with %d reserved files: %v", len(reserved), err)
	}
	if err := checkpoint.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reserved[0].Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpublished owned file after discard: %v", err)
	}
	if data, err := os.ReadFile(reserved[1].Path); err != nil || string(data) != "existing" {
		t.Fatalf("preexisting file after discard: %q, %v", data, err)
	}
	for _, file := range reserved {
		if store.Files[file.ID] != nil {
			t.Fatalf("unpublished snapshot %d entered the file registry", file.ID)
		}
	}
}

func TestCheckpointRejectsAStaleLocationWithTheRightLiveCount(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 1024, RecordBytes: 128, TargetSegmentBytes: 512, Handles: 1}
	db, _, err := Open(t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	viewIndex := memory.New()
	for _, value := range []string{"old", "current"} {
		loc, err := db.Append(format.Put, []byte("key"), []byte(value), func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if value == "old" {
			tx := viewIndex.Begin()
			if err := tx.Put("key", loc.Address()); err != nil {
				t.Fatal(err)
			}
			tx.Commit()
		}
	}
	if err := db.Rotate(); err != nil {
		t.Fatal(err)
	}
	before := db.Current
	capture, err := db.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	view := viewIndex.View()
	defer view.Close()
	checkpoint := db.NewCheckpoint(capture)
	moves := db.NewRelocations()
	defer moves.Close()
	if err := checkpoint.Copy(t.Context(), memory.New(), view, moves, db.ReserveSnapshotFile); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("stale derived index accepted: %v", err)
	}
	if err := checkpoint.Discard(); err != nil {
		t.Fatal(err)
	}
	if db.Current != before {
		t.Fatal("replaced authoritative DATA after stale lookup")
	}
}

// Ten times as many keys must move through the same fixed page caches. Test
// charged residency directly so allocator and GC timing cannot mask growth.
func TestCheckpointSpoolsTenfoldGrowthWithinFixedPageCaches(t *testing.T) {
	var smallBytes int64
	for _, n := range []int{256, 2560} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			const liveBudget, historyBudget = 512 << 10, 256 << 10
			live, err := btree.NewIn(t.TempDir(), liveBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Close()
			history, err := btree.NewIn(t.TempDir(), historyBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer history.Close()
			liveMeter, historyMeter := live.(index.Meter), history.(index.Meter)
			var livePeak, historyPeak uint64
			io := &IO{Hook: func(point string) error {
				if point == "compact.record:after" {
					livePeak = max(livePeak, liveMeter.Metrics().CacheBytes)
					historyPeak = max(historyPeak, historyMeter.Metrics().CacheBytes)
				}
				return nil
			}}
			cfg := Config{ValueCacheBudgetBytes: 2 << 20, RecordBytes: 1024, TargetSegmentBytes: 64 << 10, Handles: 4}
			store, _, err := OpenIndexed(t.Context(), t.TempDir(), cfg, false, io, func(*Store) (index.Backend, error) { return live, nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			const batch = 64
			suffix := strings.Repeat("k", 190)
			for start := 0; start < n; start += batch {
				entries := make([]Entry, batch)
				for i := range entries {
					entries[i].Key = []byte(fmt.Sprintf("%08d-%s", start+i, suffix))
				}
				locs, err := store.AppendBatch(format.Put, entries, func() error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				tx := live.Begin()
				for i, entry := range entries {
					if err := tx.Put(string(entry.Key), locs[i].Address()); err != nil {
						tx.Abort()
						t.Fatal(err)
					}
				}
				tx.Commit()
			}
			if err := store.Rotate(); err != nil {
				t.Fatal(err)
			}
			capture, err := store.Capture()
			if err != nil {
				t.Fatal(err)
			}
			defer capture.Release()
			view := live.View()
			defer view.Close()
			checkpoint := store.NewCheckpoint(capture)
			moves := store.NewRelocations()
			defer moves.Close()
			if err := checkpoint.Copy(t.Context(), history, view, moves, store.ReserveSnapshotFile); err != nil {
				t.Fatal(err)
			}
			// A sorted builder publishes its first cache pages after the DATA scan.
			historyPeak = max(historyPeak, historyMeter.Metrics().CacheBytes)
			if moves.count != n || moves.fd == nil || history.Count() != n {
				t.Fatalf("spool count %d, replay count %d, want %d", moves.count, history.Count(), n)
			}
			if livePeak == 0 || historyPeak == 0 || livePeak > liveBudget || historyPeak > historyBudget {
				t.Fatalf("foreground cache = %d, replay cache = %d", livePeak, historyPeak)
			}
			if len(moves.files) > len(capture.Files)+len(checkpoint.Entries()) {
				t.Fatalf("spool retained %d DATA files", len(moves.files))
			}
			if err := checkpoint.Protect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := checkpoint.Install(); err != nil {
				t.Fatal(err)
			}
			work, err := store.BeginClean(checkpoint.Entries())
			if err != nil || work == nil {
				t.Fatalf("select installed snapshots: %v", err)
			}
			defer work.Release()
			cleanHistory, err := btree.NewIn(t.TempDir(), historyBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanHistory.Close()
			cleanMeter := cleanHistory.(index.Meter)
			var cleanPeak uint64
			cleanMoves := store.NewRelocations()
			defer cleanMoves.Close()
			keep := func(string, Location) (bool, error) {
				cleanPeak = max(cleanPeak, cleanMeter.Metrics().CacheBytes)
				return true, nil
			}
			if err := work.Copy(t.Context(), io, cleanMoves, cleanHistory, func() error { return nil }, keep); err != nil {
				t.Fatal(err)
			}
			if cleanPeak == 0 || cleanPeak > historyBudget || cleanMoves.count != n || cleanMoves.bytes != moves.bytes {
				t.Fatalf("clean cache %d, relocations %d/%d bytes, checkpoint %d/%d", cleanPeak, cleanMoves.count, cleanMoves.bytes, moves.count, moves.bytes)
			}
			if smallBytes == 0 {
				smallBytes = moves.bytes
			} else if moves.bytes != 10*smallBytes || historyMeter.Metrics().PageReads == 0 {
				t.Fatalf("spool grew from %d to %d bytes, replay page reads %d", smallBytes, moves.bytes, historyMeter.Metrics().PageReads)
			}
		})
	}
}
