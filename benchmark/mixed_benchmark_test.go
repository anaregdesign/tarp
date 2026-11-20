package benchmark

import (
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/qualification"
	"github.com/anaregdesign/tarp/internal/engine"
)

const (
	mixedWorkers = 4
	mixedOps     = qualification.MeasurementKeys
	mixedStride  = 20
)

type mixedSchedule struct {
	GetCounts       [mixedWorkers]int `json:"gets_per_worker"`
	PutCounts       [mixedWorkers]int `json:"puts_per_worker"`
	OverlappingPuts int               `json:"puts_overlapping_another_put"`
}

// Rotate the worker assigned the first operation in each group of twenty.
// Swap just two assignments to preserve the exact keys and operation mix.
func mixedOperation(slot int, multiWriter bool) int {
	if multiWriter {
		group, offset := slot/mixedStride, slot%mixedStride
		writer := group % mixedWorkers
		switch offset {
		case 0:
			return slot + writer
		case writer:
			return slot - writer
		}
	}
	return slot
}

type mixedWriteSpan struct{ start, end time.Time }

func overlappingWrites(spans []mixedWriteSpan) int {
	slices.SortFunc(spans, func(a, b mixedWriteSpan) int { return a.start.Compare(b.start) })
	var latest time.Time
	count := 0
	for i, span := range spans {
		if span.start.Before(latest) || i+1 < len(spans) && spans[i+1].start.Before(span.end) {
			count++
		}
		if latest.Before(span.end) {
			latest = span.end
		}
	}
	return count
}

type mixedMeasurement struct {
	all, reads, puts sampledLatency
	schedule         mixedSchedule
}

// Keep one value per window for each percentile. Workload throughput uses
// total calls divided by total worker wall time, excluding setup and analysis.
type mixedMetrics struct {
	calls, wallNS                  int64
	getP50, getP99, putP50, putP99 []int64
}

func (s *mixedMetrics) add(m mixedMeasurement) {
	s.calls += m.all.Operations
	s.wallNS += m.all.WallNS
	s.getP50 = append(s.getP50, m.reads.P50NS)
	s.getP99 = append(s.getP99, m.reads.P99NS)
	s.putP50 = append(s.putP50, m.puts.P50NS)
	s.putP99 = append(s.putP99, m.puts.P99NS)
}

func (s *mixedMetrics) report(metric func(float64, string)) {
	median := func(values []int64) float64 {
		slices.Sort(values)
		return (float64(values[(len(values)-1)/2]) + float64(values[len(values)/2])) / 2
	}
	metric(float64(s.calls)*float64(time.Second)/float64(s.wallNS), "calls/s")
	metric(median(s.getP50), "ns/get-p50")
	metric(median(s.getP99), "ns/get-p99")
	metric(median(s.putP50), "ns/put-p50")
	metric(median(s.putP99), "ns/put-p99")
}

func TestMixedMetricsReportAcrossWindows(t *testing.T) {
	var stats mixedMetrics
	for _, scale := range []int64{3, 1, 4, 2} {
		stats.add(mixedMeasurement{
			all:   sampledLatency{Operations: 100, WallNS: scale * int64(time.Second)},
			reads: sampledLatency{P50NS: scale, P99NS: 2 * scale},
			puts:  sampledLatency{P50NS: 10 * scale, P99NS: 20 * scale},
		})
	}
	got := make(map[string]float64)
	stats.report(func(value float64, unit string) { got[unit] = value })
	want := map[string]float64{"calls/s": 40, "ns/get-p50": 2.5, "ns/get-p99": 5, "ns/put-p50": 25, "ns/put-p99": 50}
	if !maps.Equal(got, want) {
		t.Fatalf("metrics = %v, want %v", got, want)
	}
}

func measureMixed(t testing.TB, db *engine.DB, keys [][]byte, value []byte, multiWriter bool) mixedMeasurement {
	t.Helper()
	spans := make([]mixedWriteSpan, (mixedOps+mixedStride-1)/mixedStride)
	all := measureCalls(t, mixedOps, mixedWorkers, func(slot int) error {
		op := mixedOperation(slot, multiWriter)
		key := keys[uint64(op)*2654435761%uint64(len(keys))]
		if op%mixedStride == 0 {
			span := &spans[op/mixedStride]
			span.start = time.Now()
			err := db.Put(key, value)
			span.end = time.Now()
			return err
		}
		_, err := db.Get(key)
		return err
	})
	schedule := mixedSchedule{OverlappingPuts: overlappingWrites(spans)}
	var reads, puts []int64
	for slot, ns := range all.SamplesNS {
		worker := slot % mixedWorkers
		if mixedOperation(slot, multiWriter)%mixedStride == 0 {
			puts = append(puts, ns)
			schedule.PutCounts[worker]++
		} else {
			reads = append(reads, ns)
			schedule.GetCounts[worker]++
		}
	}
	wall := time.Duration(all.WallNS)
	return mixedMeasurement{all, latency(reads, int64(len(reads)), wall), latency(puts, int64(len(puts)), wall), schedule}
}

func TestMixedWriterAssignment(t *testing.T) {
	for _, multi := range []bool{false, true} {
		var seen [mixedOps]bool
		var writes [mixedWorkers]int
		for slot := range mixedOps {
			op := mixedOperation(slot, multi)
			if op < 0 || op >= mixedOps || seen[op] {
				t.Fatalf("slot %d assigned invalid or duplicate operation %d", slot, op)
			}
			seen[op] = true
			if op%mixedStride == 0 {
				writes[slot%mixedWorkers]++
			}
		}
		want := [4]int{205, 0, 0, 0}
		if multi {
			want = [4]int{52, 51, 51, 51}
		}
		if writes != want {
			t.Fatalf("writes per worker %v, want %v", writes, want)
		}
	}
	start := time.Now()
	span := func(a, b time.Duration) mixedWriteSpan { return mixedWriteSpan{start.Add(a), start.Add(b)} }
	if got := overlappingWrites([]mixedWriteSpan{span(9, 12), span(0, 10), span(2, 3), span(12, 13)}); got != 3 {
		t.Fatalf("counted %d overlapping calls, want 3", got)
	}
}

// The first arm reproduces the historical single-writer schedule. Both arms
// use 4,096 keys, identical closed data and the VM profile's effective options.
func BenchmarkMixedWriters(b *testing.B) {
	keys, value, opts := measurementInput(1024, mixedOps)
	opts.ValueCacheBudgetBytes, opts.TargetSegmentBytes, opts.ReadFileHandles = 64<<10, 1<<20, 4
	seed := filepath.Join(b.TempDir(), "seed")
	db, err := engine.Open(seed, opts)
	if err != nil {
		b.Fatal(err)
	}
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"single-writer", "multi-writer"} {
		b.Run(mode, func(b *testing.B) {
			b.StopTimer()
			var metrics mixedMetrics
			for i := 0; i < b.N; i++ {
				dir := filepath.Join(b.TempDir(), "db")
				measurementCloneClosed(b, seed, dir)
				db, err := engine.Open(dir, opts)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				m := measureMixed(b, db, keys, value, mode == "multi-writer")
				b.StopTimer()
				metrics.add(m)
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				b.Logf("gets=%v puts=%v overlap=%d wall=%s Get p50/p99=%s/%s Put p50/p99=%s/%s",
					m.schedule.GetCounts, m.schedule.PutCounts, m.schedule.OverlappingPuts, time.Duration(m.all.WallNS),
					time.Duration(m.reads.P50NS), time.Duration(m.reads.P99NS), time.Duration(m.puts.P50NS), time.Duration(m.puts.P99NS))
			}
			metrics.report(b.ReportMetric)
		})
	}
}
