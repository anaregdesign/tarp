package benchmark

import (
	"runtime"
	"testing"
)

// Compare the same 128-key, cache-resident workload with and without a Stats
// collector. Run each arm for at least two seconds to observe multiple ticks.
func BenchmarkParallelGetDuringStats(b *testing.B) {
	for _, mode := range []string{"baseline", "1Hz"} {
		b.Run(mode, func(b *testing.B) {
			db, keys, _ := statsBenchmarkDB(b, 128)
			before := db.Stats()
			var stop chan struct{}
			var samples chan uint64
			if mode == "1Hz" {
				stop = make(chan struct{})
				samples = make(chan uint64, 1)
				ready := make(chan struct{})
				go sampleStats(db, true, ready, stop, samples)
				<-ready
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					value, err := db.Get(keys[i%len(keys)])
					if err != nil {
						b.Error(err)
						return
					}
					runtime.KeepAlive(value)
					i++
				}
			})
			b.StopTimer()
			var calls uint64
			if stop != nil {
				close(stop)
				calls = <-samples
			}
			after := db.Stats()
			if after.Cache.Hits-before.Cache.Hits != uint64(b.N) || after.Cache.Misses != before.Cache.Misses {
				b.Fatalf("unexpected Get counts: before=%+v after=%+v N=%d", before, after, b.N)
			}
			b.ReportMetric(float64(calls), "scrapes")
		})
	}
}
