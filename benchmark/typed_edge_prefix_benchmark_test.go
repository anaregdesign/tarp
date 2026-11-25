package benchmark

import (
	"bytes"
	"encoding/binary"
	"iter"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

type typedEdgeKey struct{ Tail, Head string }

type typedEdgeClock struct {
	WallNs  int64
	Logical uint32
	NodeID  [16]byte
}

type typedEdgeContribution struct {
	Weight           float32
	Expiration       time.Time
	ContribID        [24]byte
	HLC              typedEdgeClock
	DerivedAggregate bool
}

type typedEdgeState struct {
	HLC           typedEdgeClock
	Contributions []typedEdgeContribution
}

type typedEdgeEntry = tarp.TypedEntry[typedEdgeKey, typedEdgeState]
type typedEdgeDB = tarp.TypedDB[typedEdgeKey, typedEdgeState]

func typedEdgeCodec[T any](t testing.TB) tarp.Codec[T] {
	t.Helper()
	c, err := tarp.NewCodec[T]()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// NewCodec frames each struct field in order. Encoding just the leading
// field includes the complete Tail boundary and omits the Head frame.
func typedEdgeTailPrefix(t testing.TB, tail string) []byte {
	t.Helper()
	type tailOnly struct{ Tail string }
	prefix, err := typedEdgeCodec[tailOnly](t).Encode(tailOnly{Tail: tail})
	if err != nil {
		t.Fatal(err)
	}
	return prefix
}

func TestTypedEdgeTailPrefixMatchesCompleteComponent(t *testing.T) {
	c := typedEdgeCodec[typedEdgeKey](t)
	for _, tail := range []string{"", "a", "a/b", "a\x00b", strings.Repeat("x", 128)} {
		prefix := typedEdgeTailPrefix(t, tail)
		for _, candidate := range []string{tail, tail + "x", "x" + tail} {
			key, err := c.Encode(typedEdgeKey{candidate, "h/i\x00j"})
			if err != nil || bytes.HasPrefix(key, prefix) != (candidate == tail) {
				t.Fatalf("prefix for %q on tail %q: encoded key %x, error %v", tail, candidate, key, err)
			}
		}
	}
}

const typedQueryTail = "vertex-00c0"

func newTypedEdgePrefixFixture(b *testing.B, mode tarp.IndexMode) (*typedEdgeDB, map[typedEdgeKey]typedEdgeState) {
	b.Helper()
	db, err := tarp.OpenTyped(b.TempDir(), tarp.Options{IndexMode: mode}, typedEdgeCodec[typedEdgeKey](b), typedEdgeCodec[typedEdgeState](b))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	want := make(map[typedEdgeKey]typedEdgeState)
	// Reuse the raw benchmark's PCG-shuffled endpoints and key-specific digest.
	for chunk := range slices.Chunk(edgePrefixData(true), 256) {
		entries := make([]typedEdgeEntry, 0, len(chunk))
		for _, e := range chunk {
			tail, head, _ := strings.Cut(strings.TrimPrefix(string(e.Key), "edge/"), "/")
			k := typedEdgeKey{tail, head}
			clock := typedEdgeClock{WallNs: 1_700_000_000_000_000_000, NodeID: [16]byte(e.Value[:16])}
			state := typedEdgeState{HLC: clock}
			for i := range 3 {
				clock.Logical = uint32(i)
				c := typedEdgeContribution{
					Weight:     float32(i+1) / 2,
					Expiration: time.Unix(0, clock.WallNs).UTC().Add(time.Duration(i+1) * time.Hour),
					HLC:        clock,
				}
				if i > 0 {
					copy(c.ContribID[:16], clock.NodeID[:])
					binary.BigEndian.PutUint64(c.ContribID[16:], uint64(i))
				}
				state.Contributions = append(state.Contributions, c)
			}
			entries = append(entries, typedEdgeEntry{Key: k, Value: state})
			if tail == typedQueryTail {
				want[k] = state
			}
		}
		if err := db.BatchPut(entries); err != nil {
			b.Fatal(err)
		}
	}
	// Fully visit DATA and index pages before a selected individual arm.
	var count int
	for _, err := range db.Scan(b.Context()) {
		if err != nil {
			b.Fatal(err)
		}
		count++
	}
	if count != 16385 || len(want) != 56 {
		b.Fatalf("typed fixture has %d keys and %d matching edges", count, len(want))
	}
	return db, want
}

func measureTypedEdgeRows[T any](b *testing.B, rows func() iter.Seq2[T, error], keyOf func(T) typedEdgeKey,
	equalValue func(T, typedEdgeState) bool, native bool, want map[typedEdgeKey]typedEdgeState) {
	walk := func(verify bool) int {
		var yielded, matched int
		var seen map[typedEdgeKey]bool
		if verify {
			seen = make(map[typedEdgeKey]bool, len(want))
		}
		for entry, err := range rows() {
			if err != nil {
				b.Fatal(err)
			}
			yielded++
			key := keyOf(entry)
			if !native && key.Tail != typedQueryTail {
				continue
			}
			matched++
			if verify {
				expected, ok := want[key]
				if !ok || seen[key] || (equalValue != nil && !equalValue(entry, expected)) {
					b.Fatalf("unexpected, duplicate or changed edge %+v", key)
				}
				seen[key] = true
			}
		}
		if matched != len(want) {
			b.Fatalf("got %d edges, want %d", matched, len(want))
		}
		return yielded
	}
	walk(true)
	b.ReportAllocs()
	var total int64
	for b.Loop() {
		total += int64(walk(false))
	}
	b.ReportMetric(float64(len(want)), "matches/op")
	b.ReportMetric(float64(total)/float64(b.N), "typed-yields/op")
}

func BenchmarkTypedEdgePrefix(b *testing.B) {
	for _, index := range []struct {
		name string
		mode tarp.IndexMode
	}{{"BTree", tarp.IndexBTree}, {"Memory", tarp.IndexMemory}} {
		b.Run(index.name, func(b *testing.B) {
			db, want := newTypedEdgePrefixFixture(b, index.mode)
			prefix := typedEdgeTailPrefix(b, typedQueryTail)
			for _, op := range []string{"Keys", "Scan"} {
				for _, arm := range []string{"filter", "prefix"} {
					b.Run(op+"/"+arm, func(b *testing.B) {
						ctx := b.Context()
						native := arm == "prefix"
						if op == "Keys" {
							rows := func() iter.Seq2[typedEdgeKey, error] { return db.Keys(ctx) }
							if native {
								rows = func() iter.Seq2[typedEdgeKey, error] { return db.KeysPrefix(ctx, prefix) }
							}
							measureTypedEdgeRows(b, rows, func(k typedEdgeKey) typedEdgeKey { return k }, nil, native, want)
						} else {
							rows := func() iter.Seq2[typedEdgeEntry, error] { return db.Scan(ctx) }
							if native {
								rows = func() iter.Seq2[typedEdgeEntry, error] { return db.ScanPrefix(ctx, prefix) }
							}
							measureTypedEdgeRows(b, rows, func(e typedEdgeEntry) typedEdgeKey { return e.Key }, func(e typedEdgeEntry, want typedEdgeState) bool {
								return e.Value.HLC == want.HLC && slices.Equal(e.Value.Contributions, want.Contributions)
							}, native, want)
						}
					})
				}
			}
		})
	}
}
