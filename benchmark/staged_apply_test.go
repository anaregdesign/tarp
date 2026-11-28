package benchmark_test

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func BenchmarkStagedApply(b *testing.B) {
	for _, profile := range []struct {
		name         string
		keys, values int
		target       int64
	}{
		{"multiple-segments", 4096, 1024, 1 << 20},
		{"100k-with-foreground-put", 100000, 64, 4 << 20},
	} {
		b.Run(profile.name, func(b *testing.B) {
			keys := make([][]byte, profile.keys)
			for i := range keys {
				keys[i] = fmt.Appendf(nil, "graph/%08x", i)
			}
			value := bytes.Repeat([]byte("v"), profile.values)
			encoded := int64(len(keys)) * int64(format.RecordHeaderBytes+len(keys[0])+len(value))
			park := make(chan struct{})
			var arm atomic.Bool
			var started, done chan struct{}
			var foreground time.Duration
			var foregroundErr error
			var db *tarp.DB
			hooks := &engine.Hooks{CleanerStart: park, ParityStart: park, WriterWait: func(acquired bool) {
				if !acquired && arm.Load() {
					close(started)
				}
			}}
			var staged atomic.Bool
			io := &storage.IO{Hook: func(point string) error {
				if point != "staged.data.write:before" || !staged.CompareAndSwap(true, false) {
					return nil
				}
				arm.Store(true)
				go func() {
					start := time.Now()
					foregroundErr = db.Put([]byte("progress"), []byte("foreground"))
					foreground += time.Since(start)
					close(done)
				}()
				<-started
				arm.Store(false)
				return nil
			}}
			native, err := engine.OpenWithHooks(b.TempDir(), engine.Options{TargetSegmentBytes: profile.target}, io, hooks)
			if err != nil {
				b.Fatal(err)
			}
			db = (*tarp.DB)(native)
			b.Cleanup(func() {
				if err := db.Close(); err != nil {
					b.Error(err)
				}
			})
			b.SetBytes(encoded)
			b.ReportAllocs()
			for b.Loop() {
				if profile.keys == 100000 {
					started, done = make(chan struct{}), make(chan struct{})
					staged.Store(true)
				}
				err := db.ApplyStaged(b.Context(), func(yield func(tarp.StagedMutation, error) bool) {
					for _, key := range keys {
						if !yield(tarp.StagedMutation{Key: key, Value: value}, nil) {
							return
						}
					}
				})
				if profile.keys == 100000 && !staged.Load() {
					<-done
					if foregroundErr != nil {
						b.Fatal(foregroundErr)
					}
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			if got, err := db.Get(keys[len(keys)-1]); err != nil || !bytes.Equal(got, value) {
				b.Fatalf("last staged value: %v", err)
			}
			b.ReportMetric(float64(encoded), "spool-B/op")
			b.ReportMetric(float64(engine.Observe(native).IndexMetrics().ScratchPeakBytes), "peak-index-scratch-B")
			if profile.keys == 100000 {
				b.ReportMetric(float64(foreground.Nanoseconds())/float64(b.N), "ns/overlapping-Put")
			}
		})
	}
}
