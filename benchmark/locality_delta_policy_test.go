package benchmark

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

const (
	localityDeltaMinLogKeys = 32
	localityDeltaMaxKeys    = localityPolicyRangeKeys / 2
	localityDeltaInterval   = 4
)

type localityDeltaChoice struct {
	r, count, logKeys int
	savedPages        int // Net projected savings weighted by previous read observations.
}

// A side file contains one current version of each live key displaced from the
// ORIGINAL base, including entries surviving from the old patch. All other
// addresses stay put. Gaps left by deletes remain gaps in the immutable base.
func (s *localityPolicyStore) deltaChoice(r int, patch map[int]int, before *localityPolicyLayout, past *[localityPolicyTails]int) localityDeltaChoice {
	c := localityDeltaChoice{r: r}
	old, hasOld := patch[r]
	first := r * localityPolicyRangeKeys / localityPolicyHeads
	for tail := first; tail < first+localityPolicyRangeKeys/localityPolicyHeads; tail++ {
		basePages := map[int]bool{}
		begin := c.count
		for _, key := range s.keys[tail*localityPolicyHeads : (tail+1)*localityPolicyHeads] {
			a, ok := s.latest[key]
			if !ok {
				continue
			}
			if a.file == s.base[r] {
				basePages[a.slot*localityPolicyRecordBytes/localityPolicyPageBytes] = true
				continue
			}
			c.count++
			if !hasOld || a.file != old {
				c.logKeys++
			}
		}
		patchPages := 0
		if begin < c.count {
			patchPages = (c.count-1)*localityPolicyRecordBytes/localityPolicyPageBytes - begin*localityPolicyRecordBytes/localityPolicyPageBytes + 1
		}
		c.savedPages += past[tail] * (before.prefix[tail].Pages - len(basePages) - patchPages)
	}
	return c
}

// Rank candidates by net prior-demand page savings per copied record, breaking
// ties by earlier range. Oversized patches stay in the log/old patch, with no
// hidden fallback to a full rebuild or budget carry-over.
func (s *localityPolicyStore) chooseDelta(epoch int, patch map[int]int, before *localityPolicyLayout, past *[localityPolicyTails]int) localityDeltaChoice {
	best := localityDeltaChoice{r: -1}
	if epoch%localityDeltaInterval != 0 {
		return best
	}
	for r, displaced := range before.displaced {
		if displaced < localityDeltaMinLogKeys || displaced > localityDeltaMaxKeys {
			continue
		}
		c := s.deltaChoice(r, patch, before, past)
		if c.logKeys < localityDeltaMinLogKeys || c.savedPages <= 0 {
			continue
		}
		if best.r < 0 || c.savedPages*best.count > best.savedPages*c.count {
			best = c
		}
	}
	return best
}

// Deletes have no latest address and are omitted. A reinsert has a new log
// address and is copied. The append log retains the historical delete marker;
// restoring a latest-address map from these files is outside this experiment.
func (s *localityPolicyStore) replaceDelta(t *testing.T, c localityDeltaChoice, patch map[int]int) (read, written int) {
	t.Helper()
	if c.count > localityDeltaMaxKeys || c.count < localityDeltaMinLogKeys {
		t.Fatalf("invalid patch size %d", c.count)
	}
	var keys []string
	buf := make([]byte, 0, c.count*localityPolicyRecordBytes)
	for _, k := range s.keys[c.r*localityPolicyRangeKeys : (c.r+1)*localityPolicyRangeKeys] {
		a, ok := s.latest[k]
		if !ok || a.file == s.base[c.r] {
			continue
		}
		var data localityPolicyRecord
		n, err := s.files[a.file].ReadAt(data[:], int64(a.slot*localityPolicyRecordBytes))
		if err != nil || n != len(data) {
			t.Fatalf("read patch input %x: %d, %v", k, n, err)
		}
		read += n
		buf = append(buf, data[:]...)
		keys = append(keys, k)
	}
	if len(keys) != c.count {
		t.Fatalf("patch size changed from %d to %d", c.count, len(keys))
	}
	id, file := s.create(t, "patch")
	written = localityPolicyWrite(t, file, buf)
	for slot, k := range keys {
		s.latest[k] = localityPolicyAddress{id, slot}
	}
	if old, ok := patch[c.r]; ok {
		f := s.files[old]
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(f.Name()); err != nil {
			t.Fatal(err)
		}
		delete(s.files, old)
	}
	patch[c.r] = id
	return read, written
}

func localityDeltaRun(t *testing.T, name string, keys []string, trace []localityPolicyEpoch) {
	s, want, seed := localityPolicySeed(t, keys)
	patch := make(map[int]int)
	target := 0
	if name == "last" {
		target = localityPolicyTails - 1
	}
	var past [localityPolicyTails]int
	for i, e := range trace {
		fg := s.applyEpoch(t, &e, want)
		before := s.layout()
		c := s.chooseDelta(i+1, patch, &before, &past)
		var read, written int
		var ns int64
		var jobs []int
		after := before
		if c.r >= 0 {
			jobs = []int{c.r}
			start := time.Now()
			read, written = s.replaceDelta(t, c, patch)
			ns = time.Since(start).Nanoseconds()
			s.verify(t, want)
			after = s.layout()
			observed := 0
			for j, freq := range past {
				observed += freq * (before.prefix[j].Pages - after.prefix[j].Pages)
			}
			if observed != c.savedPages || read != written || written != c.count*localityPolicyRecordBytes {
				t.Fatalf("patch saved %d pages (predicted %d); read %d and wrote %d for %d records", observed, c.savedPages, read, written, c.count)
			}
		}
		oversize := 0
		for _, displaced := range before.displaced {
			if displaced > localityDeltaMaxKeys {
				oversize++
			}
		}
		row := map[string]any{
			"trace": name, "policy": "delta50budget", "epoch": i + 1, "updates": len(e.mutations), "live_keys": len(want),
			"record_bytes": localityPolicyRecordBytes, "modeled_page_bytes": localityPolicyPageBytes,
			"range_bytes": localityPolicyRangeKeys * localityPolicyRecordBytes, "range_count": localityPolicyRangeCount,
			"threshold_distinct_log_keys": localityDeltaMinLogKeys, "patch_cap_keys": localityDeltaMaxKeys,
			"maintenance_interval_epochs": localityDeltaInterval, "seed_bytes": seed,
			"rewritten_ranges": jobs, "rewritten_range_count": len(jobs), "maintenance_bytes": written,
			"maintenance_read_bytes": read, "maintenance_copy_ns": ns, "selected_distinct_log_keys": c.logKeys,
			"selected_patch_records": c.count, "oversize_ranges": oversize, "active_patch_files": len(patch),
			"prior_weighted_page_savings": c.savedPages, "before": before.summary(&e.reads, target), "after": after.summary(&e.reads, target),
		}
		fg.report(row, len(e.samples))
		data, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("LOCALITY_DELTA %s", data)
		past = e.reads
	}
}

func TestLocalityDeltaPolicy(t *testing.T) {
	if os.Getenv("TARP_LOCALITY_DELTA") != "1" {
		t.Skip("set TARP_LOCALITY_DELTA=1 to run the sorted patch experiment")
	}
	keys := localityPolicyKeys()
	for _, name := range []string{"first", "last", "hot", "uniform", "churn"} {
		t.Run(name, func(t *testing.T) {
			localityDeltaRun(t, name, keys, localityPolicyTrace(keys, name))
		})
	}
}

func TestLocalityDeltaKeepsBaseAndCoalescesLivePatch(t *testing.T) {
	keys := localityPolicyKeys()
	s, want, _ := localityPolicySeed(t, keys)
	patch := make(map[int]int)
	base := s.base
	var past [localityPolicyTails]int
	past[0] = 64
	version := uint64(0)
	write := func(i int, del bool) {
		version++
		key := keys[i]
		op := localityPolicyMutation{key, del, localityPolicyMakeRecord(key, version, del)}
		s.append(t, &op)
		if del {
			delete(want, key)
		} else {
			want[key] = op.data
		}
	}
	for range 64 {
		write(0, false)
	}
	before := s.layout()
	if got := s.chooseDelta(4, patch, &before, &past); got.r >= 0 {
		t.Fatalf("single overwritten key selected %+v", got)
	}
	for j := range 64 {
		write(j*17%64, false)
	}
	write(3, true)
	before = s.layout()
	first := s.chooseDelta(4, patch, &before, &past)
	if first.r != 0 || first.count != 63 || first.logKeys != 63 {
		t.Fatalf("first patch selected %+v", first)
	}
	s.replaceDelta(t, first, patch)
	oldPatch := patch[0]
	write(3, false) // Resurrection of a previously absent key.
	for j := range 48 {
		write(j*17%64, false)
	}
	write(12, true)
	before = s.layout()
	second := s.chooseDelta(8, patch, &before, &past)
	if second.r != 0 || second.count != 63 || second.logKeys >= second.count {
		t.Fatalf("replacement should include surviving old patch entries: %+v", second)
	}
	read, written := s.replaceDelta(t, second, patch)
	if s.base != base || s.files[oldPatch] != nil || read != 63*localityPolicyRecordBytes || written != read {
		t.Fatalf("unexpected replacement: base %v, old patch %v, read/write %d/%d", s.base, s.files[oldPatch], read, written)
	}
	s.verify(t, want)
	for i := 64; i < 150; i++ {
		write(i, false)
	}
	before = s.layout()
	if c := s.chooseDelta(12, patch, &before, &past); c.r >= 0 {
		t.Fatalf("selecting oversized patch: %+v", c)
	}
}
