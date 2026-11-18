package benchmark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	repackParked     = "parked"
	repackConcurrent = "concurrent"
	repackGetsPerPut = 8
	repackPuts       = 128
	repackWriteGap   = time.Millisecond
)

type repackMeasurement struct {
	Mode               string            `json:"mode"`
	WriteGapNS         int64             `json:"write_gap_ns"`
	Options            engine.Options    `json:"options"`
	Get                sampledLatency    `json:"get"`
	Put                sampledLatency    `json:"put"`
	WallNS             int64             `json:"wall_ns"`
	RepackElapsedNS    int64             `json:"repack_elapsed_ns"`
	CoordinationWaitNS int64             `json:"coordination_wait_ns"`
	OverlappingGets    int               `json:"overlapping_gets"`
	OverlappingPuts    int               `json:"overlapping_puts"`
	GetDuringRepack    repackPercentiles `json:"get_during_repack"`
	PutDuringRepack    repackPercentiles `json:"put_during_repack"`
	DataFilesBefore    int               `json:"data_files_before"`
	DataFilesAfter     int               `json:"data_files_after"`
	ReopenedStateHash  string            `json:"reopened_state_sha256"`
}

type repackCall struct{ begin, end time.Time }

type repackPercentiles struct {
	P50NS int64 `json:"p50_ns"`
	P99NS int64 `json:"p99_ns"`
}

func summarizeRepackCalls(calls []repackCall, start, retired time.Time) (sampledLatency, int, repackPercentiles) {
	raw := make([]int64, len(calls))
	overlaps := 0
	for i, call := range calls {
		raw[i] = call.end.Sub(call.begin).Nanoseconds()
		if call.begin.Before(retired) && call.end.After(start) {
			overlaps++
		}
	}
	// Each caller is sequential and starts at or after release, so the
	// overlapping calls are exactly the prefix of its raw distribution.
	during := latency(raw[:overlaps], int64(overlaps), 0)
	return latency(raw, int64(len(calls)), calls[len(calls)-1].end.Sub(start)),
		overlaps, repackPercentiles{P50NS: during.P50NS, P99NS: during.P99NS}
}

func (f repackFixture) measure(t testing.TB, mode string) repackMeasurement {
	t.Helper()
	return f.measureWindow(t, mode, repackPuts)
}

// Both foreground workers and the first copy write share a start signal. The
// copy may finish quickly on a warm filesystem. At output sync the worker waits
// for the first Get and Put to complete so each overlaps its maintenance pass.
// Foreground calls never wait at a measurement hook. Record any controlled wait.
func (f repackFixture) measureWindow(t testing.TB, mode string, puts int) repackMeasurement {
	t.Helper()
	return f.measureTraced(t, mode, puts, nil)
}

func (f repackFixture) measureTraced(t testing.TB, mode string, puts int, trace *repackTrace) repackMeasurement {
	t.Helper()
	repacking := mode == repackConcurrent
	if !repacking && mode != repackParked || puts <= 0 {
		t.Fatalf("invalid repack measurement mode %q or Put count %d", mode, puts)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	workerStart, copyReady, start := make(chan struct{}), make(chan struct{}), make(chan struct{})
	firstCalls := make(chan struct{}, 2)
	retirement := make(chan time.Time, 1)
	var sawCopy, published bool // hooks below run on the single cleaner
	var coordinationWait time.Duration
	fs := &storage.IO{CleanerStart: workerStart, Hook: func(point string) error {
		if trace != nil {
			trace.record(point)
		}
		switch point {
		case "clean.copy:before":
			if !sawCopy {
				sawCopy = true
				close(copyReady)
				select {
				case <-start:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case "clean.output.sync:after":
			before := time.Now()
			for range 2 {
				select {
				case <-firstCalls:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			coordinationWait = time.Since(before)
		case "clean.current.dirsync:after":
			published = true
		case "clean.retire.dirsync:after":
			if published {
				retirement <- time.Now()
				published = false
			}
		}
		return nil
	}}
	if trace != nil {
		fs.WriterWait = trace.writerWait
	}
	db, cleanup := f.openClone(t, fs)
	defer cleanup()

	gets := make([]repackCall, puts*repackGetsPerPut)
	writes := make([]repackCall, puts)
	bursts := make(chan int, puts)
	var ready, workers sync.WaitGroup
	var getErr, putErr error
	ready.Add(2)
	workers.Go(func() {
		ready.Done()
		for index := range bursts {
			for j := range repackGetsPerPut {
				call := &gets[index*repackGetsPerPut+j]
				call.begin = time.Now()
				value, err := db.Get(f.readKey)
				call.end = time.Now()
				if index == 0 && j == 0 {
					firstCalls <- struct{}{}
				}
				if err != nil || !bytes.Equal(value, f.value) {
					getErr = fmt.Errorf("foreground Get returned %d bytes, error %v", len(value), err)
					return
				}
			}
		}
	})
	workers.Go(func() {
		defer close(bursts)
		ready.Done()
		select {
		case <-start:
		case <-ctx.Done():
			return
		}
		for i := range writes {
			// Pace reads throughout the writer's batch without blocking the writer.
			bursts <- i
			call := &writes[i]
			call.begin = time.Now()
			putErr = db.Put(f.writeKey, f.value)
			call.end = time.Now()
			if i == 0 {
				firstCalls <- struct{}{}
			}
			if putErr != nil {
				return
			}
			// The cleaner yields to pending writers. Leave the same gap in
			// each arm so it can copy between bursts of foreground work.
			if i+1 < len(writes) {
				select {
				case <-time.After(repackWriteGap):
				case <-ctx.Done():
					return
				}
			}
		}
	})
	defer func() { cancel(); workers.Wait() }()
	ready.Wait()
	if repacking {
		close(workerStart)
		select {
		case <-copyReady:
		case <-engine.Observe(db).CleanerDone():
			t.Fatalf("worker stopped before first copy: %+v", db.MaintenanceStatus())
		case <-ctx.Done():
			t.Fatalf("worker did not start copying: %v (%+v)", ctx.Err(), db.MaintenanceStatus())
		}
	}
	b, timed := t.(*testing.B)
	if timed {
		b.StartTimer()
	}
	begin := time.Now()
	close(start)
	workers.Wait()
	var retired time.Time
	if repacking && getErr == nil && putErr == nil {
		select {
		case retired = <-retirement:
		case <-ctx.Done():
			t.Fatalf("repack did not retire its sources: %v (%+v)", ctx.Err(), db.MaintenanceStatus())
		}
	}
	wall := time.Since(begin)
	if timed {
		b.StopTimer()
	}
	if err := errors.Join(getErr, putErr, ctx.Err()); err != nil {
		t.Fatal(err)
	}
	result := repackMeasurement{Mode: mode, WriteGapNS: repackWriteGap.Nanoseconds(), Options: f.opts,
		WallNS: wall.Nanoseconds(), DataFilesBefore: len(f.inputs) + 1}
	result.Get, result.OverlappingGets, result.GetDuringRepack = summarizeRepackCalls(gets, begin, retired)
	result.Put, result.OverlappingPuts, result.PutDuringRepack = summarizeRepackCalls(writes, begin, retired)
	if repacking {
		result.RepackElapsedNS = retired.Sub(begin).Nanoseconds()
		result.CoordinationWaitNS = coordinationWait.Nanoseconds()
		if result.OverlappingGets == 0 || result.OverlappingPuts == 0 {
			t.Fatal("repack finished without overlapping both foreground operations")
		}
	}
	if trace != nil {
		trace.addCalls(begin, writes, retired)
	}
	result.DataFilesAfter, result.ReopenedStateHash = f.verify(t, db, repacking, puts)
	return result
}

func BenchmarkRepackForeground(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			fixture := prepareRepackFixture(b, size)
			for _, mode := range []string{repackParked, repackConcurrent} {
				b.Run(mode, func(b *testing.B) {
					b.StopTimer()
					b.ResetTimer()
					var get50, get99, put50, put99 int64
					var getDuring, putDuring repackPercentiles
					var overlapGets, overlapPuts, filesBefore, filesAfter int
					var elapsedNS int64
					for i := 0; i < b.N; i++ {
						result := fixture.measure(b, mode)
						get50 += result.Get.P50NS
						get99 += result.Get.P99NS
						put50 += result.Put.P50NS
						put99 += result.Put.P99NS
						getDuring.P50NS += result.GetDuringRepack.P50NS
						getDuring.P99NS += result.GetDuringRepack.P99NS
						putDuring.P50NS += result.PutDuringRepack.P50NS
						putDuring.P99NS += result.PutDuringRepack.P99NS
						overlapGets += result.OverlappingGets
						overlapPuts += result.OverlappingPuts
						elapsedNS += result.RepackElapsedNS
						filesBefore, filesAfter = result.DataFilesBefore, result.DataFilesAfter
					}
					// Report means of the per-window percentiles over b.N iterations.
					b.ReportMetric(float64(get50)/float64(b.N), "ns/get-p50")
					b.ReportMetric(float64(get99)/float64(b.N), "ns/get-p99")
					b.ReportMetric(float64(put50)/float64(b.N), "ns/put-p50")
					b.ReportMetric(float64(put99)/float64(b.N), "ns/put-p99")
					if mode == repackConcurrent {
						b.ReportMetric(float64(getDuring.P50NS)/float64(b.N), "ns/overlap-get-p50")
						b.ReportMetric(float64(getDuring.P99NS)/float64(b.N), "ns/overlap-get-p99")
						b.ReportMetric(float64(putDuring.P50NS)/float64(b.N), "ns/overlap-put-p50")
						b.ReportMetric(float64(putDuring.P99NS)/float64(b.N), "ns/overlap-put-p99")
					}
					b.ReportMetric(float64(overlapGets)/float64(b.N), "overlapping-Gets/window")
					b.ReportMetric(float64(overlapPuts)/float64(b.N), "overlapping-Puts/window")
					b.ReportMetric(float64(elapsedNS)/1e6/float64(b.N), "repack-ms/window")
					b.ReportMetric(float64(filesBefore), "DATA-files-before")
					b.ReportMetric(float64(filesAfter), "DATA-files-after")
				})
			}
		})
	}
}

func TestRepackForegroundMeasurement(t *testing.T) {
	fixture := prepareRepackFixture(t, 64)
	const puts = 4
	for _, mode := range []string{repackParked, repackConcurrent} {
		t.Run(mode, func(t *testing.T) {
			result := fixture.measureWindow(t, mode, puts)
			if result.Get.Operations != puts*repackGetsPerPut || result.Put.Operations != puts ||
				result.Get.P50NS <= 0 || result.Get.P99NS < result.Get.P50NS || result.Put.P50NS <= 0 || result.Put.P99NS < result.Put.P50NS {
				t.Fatalf("invalid foreground distributions: %+v", result)
			}
			active := mode == repackConcurrent
			if (result.OverlappingGets > 0 && result.OverlappingPuts > 0) != active ||
				(result.GetDuringRepack.P50NS > 0 && result.PutDuringRepack.P50NS > 0) != active ||
				(result.RepackElapsedNS > 0) != active || result.RepackElapsedNS > result.WallNS ||
				result.CoordinationWaitNS > result.RepackElapsedNS || (result.DataFilesAfter < result.DataFilesBefore) != active {
				t.Fatalf("invalid maintenance window: %+v", result)
			}
		})
	}
}
