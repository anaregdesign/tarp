package benchmark

import (
	"bytes"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

// Seed using unique keys, with no obsolete records. The 64 KiB target from
// measurementInput produces multiple selected DATA files in the larger case.
func statsBenchmarkDB(b *testing.B, count int) (*tarp.DB, [][]byte, []byte) {
	b.Helper()
	keys, value, opts := measurementInput(64, count)
	db, err := tarp.Open(filepath.Join(b.TempDir(), "db"), tarp.Options(opts))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	for start := 0; start < count; start += 256 {
		group := keys[start:min(start+256, count)]
		entries := make([]tarp.Entry, len(group))
		for i, key := range group {
			entries[i] = tarp.Entry{Key: key, Value: value}
		}
		if err := db.BatchPut(entries); err != nil {
			b.Fatal(err)
		}
	}
	if got := db.Stats(); got.LiveKeys != uint64(count) || got.WrittenRecords != uint64(count) {
		b.Fatalf("incomplete fixture: %+v", got)
	}
	return db, keys, value
}

func BenchmarkStatsSnapshot(b *testing.B) {
	for _, count := range []int{1, 4096} {
		b.Run(fmt.Sprintf("%d-keys", count), func(b *testing.B) {
			db, _, _ := statsBenchmarkDB(b, count)
			before := db.Stats()
			var snapshot tarp.Stats
			b.ReportAllocs()
			for b.Loop() {
				snapshot = db.Stats()
			}
			if snapshot != before {
				b.Fatalf("snapshot changed during idle collection: %+v -> %+v", before, snapshot)
			}
			b.ReportMetric(float64(snapshot.SelectedSegments), "segments")
		})
	}
}

// Each row measures the same serial foreground Get on an LRU-resident value.
// Only the background collection policy changes. Run for at least two seconds
// per row to observe multiple 1 Hz samples.
func BenchmarkStatsDuringGet(b *testing.B) {
	for _, mode := range []string{"baseline", "1Hz", "continuous"} {
		b.Run(mode, func(b *testing.B) {
			db, keys, want := statsBenchmarkDB(b, 1)
			before := db.Stats()
			var got []byte
			var err error
			var scrapes uint64
			b.ReportAllocs()
			func() {
				if mode != "baseline" {
					stop := make(chan struct{})
					ready := make(chan struct{})
					result := make(chan uint64, 1)
					go sampleStats(db, mode == "1Hz", ready, stop, result)
					<-ready
					defer func() {
						close(stop)
						scrapes = <-result
					}()
				}
				for b.Loop() {
					got, err = db.Get(keys[0])
					if err != nil {
						b.Fatal(err)
					}
				}
			}()
			after := db.Stats()
			if !bytes.Equal(got, want) || after.Cache.Hits-before.Cache.Hits != uint64(b.N) ||
				after.Cache.Misses != before.Cache.Misses {
				b.Fatalf("unexpected Get workload: N=%d before=%+v after=%+v", b.N, before, after)
			}
			b.ReportMetric(float64(scrapes), "scrapes")
			b.ReportMetric(float64(scrapes)/b.Elapsed().Seconds(), "scrapes/s")
		})
	}
}

func sampleStats(db *tarp.DB, paced bool, ready chan<- struct{}, stop <-chan struct{}, result chan<- uint64) {
	var ticks <-chan time.Time
	if paced {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		ticks = ticker.C
	}
	var calls uint64
	defer func() { result <- calls }()
	close(ready)
	for {
		if paced {
			select {
			case <-stop:
				return
			case <-ticks:
			}
		} else {
			select {
			case <-stop:
				return
			default:
			}
		}
		snapshot := db.Stats()
		runtime.KeepAlive(snapshot)
		calls++
	}
}
