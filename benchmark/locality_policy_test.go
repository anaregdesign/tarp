package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// Standalone physical layout experiment. Page counts refer to 4 KiB logical
// pages within regular files. No WAL, fsync, durability or recovery protocol.
const (
	localityPolicyHeads       = 32
	localityPolicyTails       = 128
	localityPolicyRangeKeys   = 256
	localityPolicyRangeCount  = localityPolicyHeads * localityPolicyTails / localityPolicyRangeKeys
	localityPolicyRecordBytes = 512
	localityPolicyPageBytes   = 4096
	localityPolicyEpochs      = 16
	localityPolicyWrites      = 128
)

type localityPolicyRecord [localityPolicyRecordBytes]byte

func localityPolicyMakeRecord(key string, version uint64, deleted bool) localityPolicyRecord {
	var record localityPolicyRecord
	copy(record[:8], key)
	binary.BigEndian.PutUint64(record[8:16], version)
	if deleted {
		record[16] = 1
	}
	digest := sha256.Sum256(record[:17])
	for off := 17; off < len(record); off += len(digest) {
		copy(record[off:], digest[:])
	}
	return record
}

func localityPolicyKeys() []string {
	var keys []string
	for tail := range localityPolicyTails {
		for head := range localityPolicyHeads {
			var key [8]byte
			binary.BigEndian.PutUint32(key[:4], uint32(tail))
			binary.BigEndian.PutUint32(key[4:], uint32(head))
			keys = append(keys, string(key[:]))
		}
	}
	slices.Sort(keys) // Range membership is assigned by ordinary encoded key order.
	return keys
}

type localityPolicyMutation struct {
	key     string
	deleted bool
	data    localityPolicyRecord
}

type localityPolicyEpoch struct {
	mutations []localityPolicyMutation
	reads     [localityPolicyTails]int
	samples   [localityPolicyWrites / 2]int
}

func localityPolicyTrace(keys []string, name string) []localityPolicyEpoch {
	writes := rand.New(rand.NewPCG(150, 1))
	reads := rand.New(rand.NewPCG(150, 2))
	trace := make([]localityPolicyEpoch, localityPolicyEpochs)
	for e := range trace {
		epoch := &trace[e]
		for j := range localityPolicyWrites {
			var idx int
			switch name {
			case "first":
				idx = 0
			case "last":
				idx = len(keys) - 1
			case "hot":
				idx = writes.IntN(96)
			case "uniform":
				idx = writes.IntN(len(keys))
			case "churn":
				idx = writes.IntN(len(keys))
				if j%4 != 0 {
					idx = writes.IntN(localityPolicyRangeKeys)
				}
			}
			del := name == "churn" && j%5 == 0
			k := keys[idx]
			epoch.mutations = append(epoch.mutations, localityPolicyMutation{k, del, localityPolicyMakeRecord(k, uint64(e*localityPolicyWrites+j+1), del)})
		}
		for j := range epoch.samples {
			tail := reads.IntN(localityPolicyTails)
			if j%4 != 0 && name != "uniform" {
				switch name {
				case "first":
					tail = 0
				case "last":
					tail = localityPolicyTails - 1
				case "hot":
					tail = reads.IntN(3)
				case "churn":
					tail = reads.IntN(8)
				}
			}
			epoch.samples[j] = tail
			epoch.reads[tail]++
		}
	}
	return trace
}

type localityPolicyAddress struct{ file, slot int }

type localityPolicyStore struct {
	dir      string
	keys     []string
	latest   map[string]localityPolicyAddress
	files    map[int]*os.File
	base     [localityPolicyRangeCount]int
	nextFile int
	log      int
	logSlots int
}

func (s *localityPolicyStore) create(t *testing.T, kind string) (int, *os.File) {
	t.Helper()
	id := s.nextFile
	s.nextFile++
	file, err := os.OpenFile(filepath.Join(s.dir, fmt.Sprintf("%s-%04d", kind, id)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	s.files[id] = file
	return id, file
}

func localityPolicyWrite(t *testing.T, f *os.File, p []byte) int {
	n, err := f.Write(p)
	if err != nil || n != len(p) {
		t.Fatalf("DATA write %d of %d bytes: %v", n, len(p), err)
	}
	return n
}

func localityPolicySeed(t *testing.T, keys []string) (*localityPolicyStore, map[string]localityPolicyRecord, int) {
	t.Helper()
	s := &localityPolicyStore{dir: t.TempDir(), keys: keys, files: make(map[int]*os.File), latest: make(map[string]localityPolicyAddress), log: -1}
	t.Cleanup(func() {
		for _, file := range s.files {
			if err := file.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	written := 0
	want := make(map[string]localityPolicyRecord, len(keys))
	for r := range localityPolicyRangeCount {
		id, file := s.create(t, "sorted")
		s.base[r] = id
		var buf []byte
		for slot, k := range keys[r*localityPolicyRangeKeys : (r+1)*localityPolicyRangeKeys] {
			record := localityPolicyMakeRecord(k, 0, false)
			buf = append(buf, record[:]...)
			s.latest[k] = localityPolicyAddress{id, slot}
			want[k] = record
		}
		written += localityPolicyWrite(t, file, buf)
	}
	return s, want, written
}

// One unbuffered file Write per foreground mutation, without a durability sync.
func (s *localityPolicyStore) append(t *testing.T, op *localityPolicyMutation) int {
	if s.log == -1 || s.logSlots == localityPolicyRangeKeys {
		s.log, _ = s.create(t, "log")
		s.logSlots = 0
	}
	written := localityPolicyWrite(t, s.files[s.log], op.data[:])
	if op.deleted {
		delete(s.latest, op.key)
	} else {
		s.latest[op.key] = localityPolicyAddress{s.log, s.logSlots}
	}
	s.logSlots++
	return written
}

// Rebuild copies live bytes from their latest addresses, then publishes the new
// locations. Nothing reconstructs DATA from the oracle. Old sorted files can
// retire because this single-threaded experiment has no overlapping snapshots.
// Log files are retained and old versions are skipped by the latest-address map.
func (s *localityPolicyStore) rebuild(t *testing.T, r int) int {
	t.Helper()
	old := s.base[r]
	id, f := s.create(t, "sorted")
	keys := s.keys[r*localityPolicyRangeKeys : (r+1)*localityPolicyRangeKeys]
	buf := make([]byte, 0, len(keys)*localityPolicyRecordBytes)
	for _, k := range keys {
		a, ok := s.latest[k]
		if !ok {
			continue
		}
		var data localityPolicyRecord
		n, err := s.files[a.file].ReadAt(data[:], int64(a.slot*localityPolicyRecordBytes))
		if err != nil || n != len(data) {
			t.Fatalf("read live DATA %x: %d, %v", k, n, err)
		}
		buf = append(buf, data[:]...)
	}
	written := localityPolicyWrite(t, f, buf)
	slot := 0
	for _, k := range keys {
		if _, ok := s.latest[k]; ok {
			s.latest[k] = localityPolicyAddress{id, slot}
			slot++
		}
	}
	s.base[r] = id
	file := s.files[old]
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(file.Name()); err != nil {
		t.Fatal(err)
	}
	delete(s.files, old)
	return written
}

// Verify every current address and every live payload against the independent
// input trace. Read each regular file once to avoid thousands of oracle syscalls.
func (s *localityPolicyStore) verify(t *testing.T, want map[string]localityPolicyRecord) {
	t.Helper()
	if len(s.latest) != len(want) {
		t.Fatalf("latest has %d keys, want %d", len(s.latest), len(want))
	}
	files := make(map[int][]byte)
	for k, expected := range want {
		a, ok := s.latest[k]
		if !ok {
			t.Fatalf("missing live key %x", k)
		}
		if _, ok := files[a.file]; !ok {
			data, err := os.ReadFile(s.files[a.file].Name())
			if err != nil {
				t.Fatal(err)
			}
			files[a.file] = data
		}
		data := files[a.file]
		offset := a.slot * localityPolicyRecordBytes
		if offset+len(expected) > len(data) || !bytes.Equal(data[offset:offset+len(expected)], expected[:]) {
			t.Fatalf("wrong live bytes at %+v for key %x", a, k)
		}
	}
}

type localityPolicyFootprint struct {
	Files      int
	Pages      int
	AfterPages int // Hypothetical page count after packing this entire range.
}

type localityPolicyLayout struct {
	prefix    [localityPolicyTails]localityPolicyFootprint
	displaced [localityPolicyRangeCount]int
}

func (s *localityPolicyStore) layout() localityPolicyLayout {
	var layout localityPolicyLayout
	// Each prefix is wholly inside one range. Model the exact address (file,
	// 4 KiB page) and the effect of rebuilding only currently live records.
	packed := 0
	for tail := range localityPolicyTails {
		start := tail * localityPolicyHeads
		r := start / localityPolicyRangeKeys
		if start%localityPolicyRangeKeys == 0 {
			packed = 0
		}
		files := map[int]struct{}{}
		pages := map[localityPolicyAddress]struct{}{}
		begin := packed
		for _, k := range s.keys[start : start+localityPolicyHeads] {
			a, ok := s.latest[k]
			if !ok {
				continue
			}
			if a.file != s.base[r] {
				layout.displaced[r]++
			}
			files[a.file] = struct{}{}
			pages[localityPolicyAddress{a.file, a.slot * localityPolicyRecordBytes / localityPolicyPageBytes}] = struct{}{}
			packed++
		}
		ideal := 0
		if begin != packed {
			ideal = (packed-1)*localityPolicyRecordBytes/localityPolicyPageBytes - begin*localityPolicyRecordBytes/localityPolicyPageBytes + 1
		}
		layout.prefix[tail] = localityPolicyFootprint{len(files), len(pages), ideal}
	}
	return layout
}

type localityPolicy struct {
	name      string
	threshold int
	heat      bool
	interval  int
}

func (p localityPolicy) jobs(epoch int, l *localityPolicyLayout, past *[localityPolicyTails]int) []int {
	if p.name == "append" {
		return nil
	}
	if p.name == "full" {
		var all []int
		for r := range localityPolicyRangeCount {
			all = append(all, r)
		}
		return all
	}
	if epoch%p.interval != 0 {
		return nil
	}
	best, bestScore := -1, 0
	for r, displaced := range l.displaced {
		if displaced < p.threshold {
			continue
		}
		score := displaced
		if p.heat {
			score = 0
			first := r * localityPolicyRangeKeys / localityPolicyHeads
			for i := first; i < first+localityPolicyRangeKeys/localityPolicyHeads; i++ {
				score += past[i] * (l.prefix[i].Pages - l.prefix[i].AfterPages)
			}
		}
		if score > bestScore || (best >= 0 && score == bestScore && displaced > l.displaced[best]) {
			best, bestScore = r, score
		}
	}
	if best == -1 {
		return nil
	}
	return []int{best}
}

type localityPolicySummary struct {
	MeanPages float64 `json:"mean_pages"`
	MeanFiles float64 `json:"mean_files"`
	ReadPages float64 `json:"read_weighted_pages"`
	ReadFiles float64 `json:"read_weighted_files"`
	HotPages  int     `json:"target_pages"`
	HotFiles  int     `json:"target_files"`
	Displaced int     `json:"displaced_live_keys"`
}

func (l *localityPolicyLayout) summary(reads *[localityPolicyTails]int, target int) localityPolicySummary {
	var s localityPolicySummary
	n := 0
	for i, f := range l.prefix {
		s.MeanPages += float64(f.Pages)
		s.MeanFiles += float64(f.Files)
		s.ReadPages += float64(f.Pages * reads[i])
		s.ReadFiles += float64(f.Files * reads[i])
		n += reads[i]
	}
	for _, displaced := range l.displaced {
		s.Displaced += displaced
	}
	s.MeanPages /= localityPolicyTails
	s.MeanFiles /= localityPolicyTails
	s.ReadPages /= float64(n)
	s.ReadFiles /= float64(n)
	s.HotPages, s.HotFiles = l.prefix[target].Pages, l.prefix[target].Files
	return s
}

// Sample one actual query from the fixed read trace after each pair of writes.
// Each foreground interval times only its two updates; modeled prefix lookups,
// independent oracle work and end-of-epoch maintenance are outside that timer.
type localityPolicyForeground struct {
	bytes, pages, files int
	ns, p50, p99        int64
}

func (s *localityPolicyStore) applyEpoch(t *testing.T, e *localityPolicyEpoch, want map[string]localityPolicyRecord) localityPolicyForeground {
	var obs localityPolicyForeground
	var latencies [localityPolicyWrites]int64
	for j, tail := range e.samples {
		pair := time.Now()
		for i := 2 * j; i < 2*j+2; i++ {
			start := time.Now()
			obs.bytes += s.append(t, &e.mutations[i])
			latencies[i] = time.Since(start).Nanoseconds()
		}
		obs.ns += time.Since(pair).Nanoseconds()
		files := map[int]bool{}
		pages := map[localityPolicyAddress]bool{}
		start := tail * localityPolicyHeads
		for _, key := range s.keys[start : start+localityPolicyHeads] {
			if a, ok := s.latest[key]; ok {
				files[a.file] = true
				pages[localityPolicyAddress{a.file, a.slot * localityPolicyRecordBytes / localityPolicyPageBytes}] = true
			}
		}
		obs.files += len(files)
		obs.pages += len(pages)
	}
	slices.Sort(latencies[:])
	obs.p50 = latencies[(len(latencies)-1)/2]
	obs.p99 = latencies[(99*len(latencies)-1)/100]
	for _, op := range e.mutations {
		if op.deleted {
			delete(want, op.key)
		} else {
			want[op.key] = op.data
		}
	}
	s.verify(t, want)
	return obs
}

func (o localityPolicyForeground) report(row map[string]any, n int) {
	row["foreground_bytes"], row["foreground_ns"] = o.bytes, o.ns
	row["foreground_p50_ns"], row["foreground_p99_ns"] = o.p50, o.p99
	row["stream_queries"] = n
	row["stream_files_per_query"] = float64(o.files) / float64(n)
	row["stream_pages_per_query"] = float64(o.pages) / float64(n)
}

func localityPolicyRun(t *testing.T, name string, p localityPolicy, keys []string, trace []localityPolicyEpoch) {
	s, want, seedBytes := localityPolicySeed(t, keys)
	var past [localityPolicyTails]int // First epoch has no prior reads.
	target := 0
	if name == "last" {
		target = localityPolicyTails - 1
	}
	for i, e := range trace {
		fg := s.applyEpoch(t, &e, want)
		before := s.layout()
		jobs := p.jobs(i+1, &before, &past)
		maintBytes := 0
		var maintNS int64
		after := before
		if len(jobs) > 0 {
			start := time.Now()
			for _, r := range jobs {
				maintBytes += s.rebuild(t, r)
			}
			maintNS = time.Since(start).Nanoseconds()
			s.verify(t, want)
			after = s.layout()
			for _, r := range jobs {
				if after.displaced[r] != 0 {
					t.Fatalf("range %d still has displaced keys after rebuild", r)
				}
			}
		}
		past = e.reads
		row := map[string]any{
			"trace": name, "policy": p.name, "epoch": i + 1, "updates": len(e.mutations), "live_keys": len(want),
			"record_bytes": localityPolicyRecordBytes, "modeled_page_bytes": localityPolicyPageBytes, "range_count": localityPolicyRangeCount,
			"range_bytes": localityPolicyRecordBytes * localityPolicyRangeKeys, "threshold_distinct_keys": p.threshold,
			"maintenance_interval_epochs": p.interval, "seed_bytes": seedBytes,
			"rewritten_ranges": jobs, "rewritten_range_count": len(jobs), "maintenance_bytes": maintBytes,
			"maintenance_copy_ns": maintNS, "before": before.summary(&e.reads, target), "after": after.summary(&e.reads, target),
		}
		fg.report(row, len(e.samples))
		output, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("LOCALITY_POLICY %s", output)
	}
}

func TestLocalityPolicy(t *testing.T) {
	if os.Getenv("TARP_LOCALITY_POLICY") != "1" {
		t.Skip("set TARP_LOCALITY_POLICY=1 to run the physical layout experiment")
	}
	policies := []localityPolicy{
		{name: "append"}, {name: "full", interval: 1}, {name: "bounded12p5", threshold: 32, interval: 1},
		{name: "bounded25", threshold: 64, interval: 1}, {name: "bounded50", threshold: 128, interval: 1},
		{name: "heat25", threshold: 64, interval: 1, heat: true}, {name: "heat25budget", threshold: 64, interval: 4, heat: true},
	}
	keys := localityPolicyKeys()
	for _, name := range []string{"first", "last", "hot", "uniform", "churn"} {
		t.Run(name, func(t *testing.T) {
			trace := localityPolicyTrace(keys, name)
			for _, policy := range policies {
				t.Run(policy.name, func(t *testing.T) { localityPolicyRun(t, name, policy, keys, trace) })
			}
		})
	}
}

func TestLocalityPolicyUsesEligibleRangesAndPreviousReads(t *testing.T) {
	var layout localityPolicyLayout
	layout.displaced[0] = 1 // Many overwrites of one key count as one displaced key.
	layout.displaced[1] = 96
	layout.displaced[2] = 64
	p := localityPolicy{name: "bounded25", threshold: 64, interval: 1}
	var history [localityPolicyTails]int
	if got := p.jobs(1, &layout, &history); !slices.Equal(got, []int{1}) {
		t.Fatalf("most displaced eligible range = %v, want [1]", got)
	}
	p.heat, p.interval = true, 4
	tail := 2 * localityPolicyRangeKeys / localityPolicyHeads
	layout.prefix[tail] = localityPolicyFootprint{Pages: 16, AfterPages: 4}
	if got := p.jobs(4, &layout, &history); len(got) != 0 {
		t.Fatalf("selected %v without any past reads", got)
	}
	history[tail] = 10
	if got := p.jobs(3, &layout, &history); len(got) != 0 {
		t.Fatalf("selected %v before the bandwidth-limited epoch", got)
	}
	if got := p.jobs(4, &layout, &history); !slices.Equal(got, []int{2}) {
		t.Fatalf("read-weighted eligible range = %v, want [2]", got)
	}
}
