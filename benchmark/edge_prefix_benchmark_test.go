package benchmark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp"
)

// The keys represent directed edges with fixed-width endpoint names. A complete
// tail prefix includes its boundary, so vertex-0002 never matches vertex-0020.
func edgeTailPrefix(tail int) []byte {
	return fmt.Appendf(nil, "edge/vertex-%04x/", tail)
}

func edgePrefixData(interleaved bool) []tarp.Entry {
	var edges []tarp.Entry
	for tail := range 258 {
		degree := 56
		switch tail {
		case 128:
			degree = 1
		case 192:
			degree = 2048
		}
		prefix := edgeTailPrefix(tail * 2) // odd tail IDs are absent
		for head := range degree {
			key := fmt.Appendf(bytes.Clone(prefix), "vertex-%04x", head)
			digest := sha256.Sum256(key)
			edges = append(edges, tarp.Entry{Key: key, Value: bytes.Repeat(digest[:], 3)})
		}
	}
	if interleaved {
		rng := rand.New(rand.NewPCG(148, 1))
		rng.Shuffle(len(edges), func(i, j int) { edges[i], edges[j] = edges[j], edges[i] })
	}
	return edges
}

func newEdgePrefixFixture(b testing.TB, opts tarp.Options, edges []tarp.Entry) *tarp.DB {
	b.Helper()
	db, err := tarp.Open(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	for batch := range slices.Chunk(edges, 256) {
		if err := db.BatchPut(batch); err != nil {
			b.Fatal(err)
		}
	}
	// Scan validates the committed fixture and warms all DATA and index pages
	// before either arm, including when running a single selected benchmark.
	all := edgePrefixQuery{}
	all.verify(b, db, edges, true, false)
	return db
}

type edgePrefixQuery struct {
	prefix []byte
	first  bool
}

type edgePrefixCounts struct {
	matches, candidates, valueBytes int
}

// candidates and valueBytes measure what the public iterator yielded before
// the client's predicate. They do not measure index visits or disk I/O.
func (q edgePrefixQuery) run(ctx context.Context, db *tarp.DB, values, usePrefix bool, check func([]byte, []byte)) (edgePrefixCounts, error) {
	var counts edgePrefixCounts
	visit := func(key, value []byte) bool {
		counts.candidates++
		counts.valueBytes += len(value)
		if !usePrefix && !bytes.HasPrefix(key, q.prefix) {
			return true
		}
		counts.matches++
		if check != nil {
			check(key, value)
		}
		return !q.first
	}

	if values {
		var rows iter.Seq2[tarp.Entry, error]
		if usePrefix {
			rows = db.ScanPrefix(ctx, q.prefix)
		} else {
			rows = db.Scan(ctx)
		}
		for entry, err := range rows {
			if err != nil {
				return counts, err
			}
			if !visit(entry.Key, entry.Value) {
				break
			}
		}
	} else {
		var keys iter.Seq2[[]byte, error]
		if usePrefix {
			keys = db.KeysPrefix(ctx, q.prefix)
		} else {
			keys = db.Keys(ctx)
		}
		for key, err := range keys {
			if err != nil {
				return counts, err
			}
			if !visit(key, nil) {
				break
			}
		}
	}
	return counts, nil
}

// The oracle comes from the inserted records and is independent of both
// iterators and their traversal order. The first-match case accepts any match.
func (q edgePrefixQuery) verify(b testing.TB, db *tarp.DB, edges []tarp.Entry, values, usePrefix bool) int {
	b.Helper()
	want := make(map[string][]byte)
	for _, e := range edges {
		if bytes.HasPrefix(e.Key, q.prefix) {
			want[string(e.Key)] = e.Value
		}
	}
	n := len(want)
	if q.first {
		n = min(1, n)
	}
	counts, err := q.run(b.Context(), db, values, usePrefix, func(k, v []byte) {
		expected, ok := want[string(k)]
		if !ok || (values && !bytes.Equal(v, expected)) {
			b.Fatalf("unexpected key, duplicate or wrong value for %q", k)
		}
		delete(want, string(k))
	})
	if err != nil || counts.matches != n {
		b.Fatalf("got %d matches, want %d: %v", counts.matches, n, err)
	}
	return n
}

func (q edgePrefixQuery) measure(b *testing.B, db *tarp.DB, edges []tarp.Entry, values, usePrefix bool) {
	n := q.verify(b, db, edges, values, usePrefix)
	ctx := b.Context()
	var candidates, valueBytes int64
	b.ReportAllocs()
	for b.Loop() {
		counts, err := q.run(ctx, db, values, usePrefix, nil)
		if err != nil || counts.matches != n {
			b.Fatalf("got %d matches, want %d: %v", counts.matches, n, err)
		}
		candidates += int64(counts.candidates)
		valueBytes += int64(counts.valueBytes)
	}
	b.ReportMetric(float64(n), "matches/op")
	b.ReportMetric(float64(candidates)/float64(b.N), "yielded/op")
	b.ReportMetric(float64(valueBytes)/float64(b.N), "yielded-value-B/op")
}

func TestEdgePrefixArmsCountVisitedRows(t *testing.T) {
	edges := []tarp.Entry{
		{Key: []byte("a/1"), Value: []byte("one")},
		{Key: []byte("b/1"), Value: []byte("two")},
	}
	db := newEdgePrefixFixture(t, tarp.Options{}, edges)
	query := edgePrefixQuery{prefix: []byte("a/")}
	for _, usePrefix := range []bool{false, true} {
		for _, values := range []bool{false, true} {
			got, err := query.run(t.Context(), db, values, usePrefix, func(key, value []byte) {
				if !bytes.Equal(key, edges[0].Key) || (values && !bytes.Equal(value, edges[0].Value)) {
					t.Fatalf("unexpected row: %q, %q", key, value)
				}
			})
			want := edgePrefixCounts{matches: 1, candidates: len(edges)}
			if usePrefix {
				want.candidates = 1
			}
			if values {
				want.valueBytes = 3 * want.candidates
			}
			if err != nil || got != want {
				t.Fatalf("usePrefix=%t values=%t: got %+v, %v; want %+v", usePrefix, values, got, err, want)
			}
		}
	}
}

func BenchmarkEdgePrefix(b *testing.B) {
	queries := []struct {
		name string
		q    edgePrefixQuery
	}{
		{"missing", edgePrefixQuery{prefix: edgeTailPrefix(193)}},
		{"sparse", edgePrefixQuery{prefix: edgeTailPrefix(256)}},
		{"medium", edgePrefixQuery{prefix: edgeTailPrefix(192)}},
		{"broad", edgePrefixQuery{prefix: edgeTailPrefix(384)}},
		{"empty", edgePrefixQuery{}},
		{"first", edgePrefixQuery{prefix: edgeTailPrefix(384), first: true}},
	}
	for _, index := range []struct {
		name string
		mode tarp.IndexMode
	}{{"BTree", tarp.IndexBTree}, {"Memory", tarp.IndexMemory}} {
		for _, layout := range []string{"grouped", "interleaved"} {
			b.Run(index.name+"/"+layout, func(b *testing.B) {
				edges := edgePrefixData(layout == "interleaved")
				db := newEdgePrefixFixture(b, tarp.Options{IndexMode: index.mode}, edges)
				for _, op := range []string{"Keys", "Scan"} {
					for _, query := range queries {
						for _, arm := range []string{"filter", "prefix"} {
							b.Run(op+"/"+query.name+"/"+arm, func(b *testing.B) {
								query.q.measure(b, db, edges, op == "Scan", arm == "prefix")
							})
						}
					}
				}
			})
		}
	}
}
