//go:build corner145

package benchmark

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
)

const scanReleaseKeys = 512

type releaseCursor struct {
	next func() (tarp.Entry, error, bool)
	stop func()
	left map[string]string
}

func holdReleaseCursor(t *testing.T, db *tarp.DB, model map[string]string) *releaseCursor {
	t.Helper()
	next, stop := iter.Pull2(db.Scan(t.Context()))
	c := &releaseCursor{next, stop, maps.Clone(model)}
	c.consume(t)
	return c
}

func (c *releaseCursor) consume(t *testing.T) {
	t.Helper()
	entry, err, ok := c.next()
	want, found := c.left[string(entry.Key)]
	if !ok || err != nil || !found || want != string(entry.Value) {
		t.Fatalf("snapshot mismatch: key %q found %t, err %v, yielded %t", entry.Key, found, err, ok)
	}
	delete(c.left, string(entry.Key))
}

func (c *releaseCursor) drainHeld(t *testing.T) {
	t.Helper()
	for len(c.left) != 0 {
		c.consume(t)
	}
	// iter.Pull2 leaves the real Scan suspended at the last user yield.
}

func (c *releaseCursor) finish(t *testing.T) {
	t.Helper()
	if _, err, ok := c.next(); err != nil || ok || len(c.left) != 0 {
		t.Fatalf("snapshot does not exactly match capture: %v, %t, %d remaining", err, ok, len(c.left))
	}
	c.stop()
}

type releaseSample struct {
	Round, Pair, Position int
	Arm                   string
	NewerReleasedFirst    bool
	FirstPutNS            int64
	ConditionerPutNS      int64
	OldestHeldNS          int64
	RetiredAtTwoViews     uint64
	RetiredBeforePut      uint64
	RetiredAfterPut       uint64
	RootBefore, RootAfter uint64
	HighWaterBefore       uint64
	HighWaterAfter        uint64
	PinnedDATA            [2]uint64
	CommitSequence        uint64
}

func scanReleaseEpisode(t *testing.T, round, pair, position int, lateRelease, newerFirst bool) releaseSample {
	t.Helper()
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "tarp-scan-release-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	// All API calls use the public handle. Park independent background work so
	// the first foreground Put is also the first B+tree writer after release.
	open := func() (*tarp.DB, engine.Observer) {
		inner, err := engine.OpenWithHooks(dir, engine.Options{}, nil, &engine.Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		o := engine.Observe(inner)
		if opts := o.Options(); opts.IndexMode != engine.IndexBTree || opts.BTreeMemoryBudgetBytes != 16<<20 || opts.OpenValidation != engine.Full {
			t.Fatalf("unexpected defaults: %+v", opts)
		}
		return (*tarp.DB)(inner), o
	}
	db, observe := open()
	defer db.Close()
	rows := make([]tarp.Entry, scanReleaseKeys)
	model := make(map[string]string, scanReleaseKeys)
	values := make([][]byte, 36)
	for i := range values {
		values[i] = []byte(fmt.Sprintf("value-%058d", i))
	}
	for i := range rows {
		rows[i] = tarp.Entry{Key: []byte(fmt.Sprintf("account/%08d", i)), Value: values[0]}
		model[string(rows[i].Key)] = string(rows[i].Value)
	}
	if err := db.BatchPut(rows); err != nil {
		t.Fatal(err)
	}
	startHold := time.Now()
	a := holdReleaseCursor(t, db, model)
	defer a.stop()
	var b *releaseCursor
	row := releaseSample{Round: round, Pair: pair, Position: position, NewerReleasedFirst: newerFirst, Arm: "normal"}
	if lateRelease {
		row.Arm = "first-after-release"
	}
	sequence := uint64(len(rows))
	for step := 1; step <= 16; step++ {
		// 32 overwritten keys and eight different keys deleted/reinserted per
		// step. The offset varies by matched pair; both arms use the same trace.
		var edits, reinserts []tarp.Entry
		var deletes [][]byte
		for j := range 40 {
			n := (step*17 + j*29 + pair*7 + round*23) % len(rows)
			key := rows[n].Key
			if j < 32 {
				edits = append(edits, tarp.Entry{Key: key, Value: values[step*2-1]})
				model[string(key)] = string(values[step*2-1])
			} else {
				deletes = append(deletes, key)
				reinserts = append(reinserts, tarp.Entry{Key: key, Value: values[step*2]})
				model[string(key)] = string(values[step*2])
			}
		}
		if err := db.BatchPut(edits); err != nil {
			t.Fatal(err)
		}
		if err := db.BatchDelete(deletes); err != nil {
			t.Fatal(err)
		}
		if err := db.BatchPut(reinserts); err != nil {
			t.Fatal(err)
		}
		sequence += uint64(len(edits) + len(deletes) + len(reinserts))
		if step == 2 {
			b = holdReleaseCursor(t, db, model)
			defer b.stop()
		}
		if step == 8 || step == 16 {
			r, err := db.Compact(t.Context())
			if err != nil || r.DroppedKeys != 0 || r.PinnedRetiredDataBytes == 0 {
				t.Fatalf("Compact at step %d: %+v, %v", step, r, err)
			}
			row.PinnedDATA[step/8-1] = uint64(r.PinnedRetiredDataBytes)
		}
	}
	a.drainHeld(t)
	b.drainHeld(t)
	m := observe.IndexMetrics()
	if m.ActiveViews != 2 || m.PinnedGenerations != 2 || m.RetiredPages < 128 {
		t.Fatalf("expected two generations and substantial retirement: %+v", m)
	}
	row.RetiredAtTwoViews = m.RetiredPages
	finish := func() {
		if newerFirst {
			b.finish(t)
			a.finish(t)
		} else {
			a.finish(t)
			b.finish(t)
		}
		row.OldestHeldNS = time.Since(startHold).Nanoseconds()
	}
	if !lateRelease {
		finish()
	}
	// Exactly the same two final overwrites in both arms. In the baseline,
	// the conditioner drains the backlog before the timed request.
	conditionKey := rows[(round+pair+51)%len(rows)].Key
	timedKey := rows[(round+pair+91)%len(rows)].Key
	before := time.Now()
	err = db.Put(conditionKey, values[33])
	row.ConditionerPutNS = time.Since(before).Nanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	model[string(conditionKey)] = string(values[33])
	if lateRelease {
		finish()
	}
	m = observe.IndexMetrics()
	row.RetiredBeforePut = m.RetiredPages
	if m.ActiveViews != 0 || m.PinnedGenerations != 0 || (m.RetiredPages != 0) != lateRelease {
		t.Fatalf("unmatched preconditions: %+v", m)
	}
	row.RootBefore, row.HighWaterBefore = observe.Corner145PageIDs()
	before = time.Now()
	err = db.Put(timedKey, values[34])
	row.FirstPutNS = time.Since(before).Nanoseconds()
	if err != nil {
		t.Fatal(err)
	}
	model[string(timedKey)] = string(values[34])
	row.RootAfter, row.HighWaterAfter = observe.Corner145PageIDs()
	row.RetiredAfterPut = observe.IndexMetrics().RetiredPages
	if row.RetiredAfterPut != 0 || row.RootBefore == row.RootAfter || row.RootAfter > row.HighWaterBefore || row.HighWaterAfter != row.HighWaterBefore {
		t.Fatalf("expected reclamation and allocation from reusable pages: %+v", row)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, observer := open()
	defer reopened.Close()
	check := holdReleaseCursor(t, reopened, model)
	defer check.stop()
	check.drainHeld(t)
	check.finish(t)
	row.CommitSequence = observer.Frontier().Sequence
	if row.CommitSequence != sequence+2 {
		t.Fatalf("frontier %d != expected %d", row.CommitSequence, sequence+2)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestCorner145ScanRelease(t *testing.T) {
	if os.Getenv("TARP_SCAN_RELEASE_OUT") == "" {
		t.Skip("dedicated paired measurement")
	}
	open, err := os.OpenFile(os.Getenv("TARP_SCAN_RELEASE_OUT"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	enc := json.NewEncoder(open)
	readCount := func(name string) int {
		n, err := strconv.Atoi(os.Getenv(name))
		if err != nil || n < 1 {
			t.Fatalf("invalid %s", name)
		}
		return n
	}
	rounds, pairs := readCount("TARP_SCAN_RELEASE_ROUNDS"), readCount("TARP_SCAN_RELEASE_PAIRS")
	for round := range rounds {
		for pair := range pairs {
			for pos := range 2 {
				one := scanReleaseEpisode(t, round, pair, pos, (round+pair+pos)%2 == 0, (round+pair/2)%2 == 0)
				if err := enc.Encode(one); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Logf("round %d/%d complete", round+1, rounds)
	}
}
