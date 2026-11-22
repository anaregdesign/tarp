package benchmark

import (
	"testing"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
)

// Rotate across ordinary tails rather than revisiting one hot leaf. The
// process-level page meter counts B+tree pager misses (including OS-cache hits).
func BenchmarkEdgePrefixWorkingSet(b *testing.B) {
	var queries []edgePrefixQuery
	for tail := range 258 {
		if tail != 128 && tail != 192 {
			queries = append(queries, edgePrefixQuery{prefix: edgeTailPrefix(tail * 2)})
		}
	}
	for _, budget := range []struct {
		name  string
		bytes int64
	}{{"1MiB", 1 << 20}, {"16MiB", 16 << 20}} {
		b.Run(budget.name, func(b *testing.B) {
			edges := edgePrefixData(true)
			db := newEdgePrefixFixture(b, tarp.Options{IndexMode: tarp.IndexBTree, BTreeMemoryBudgetBytes: budget.bytes}, edges)
			observe := engine.Observe((*engine.DB)(db))
			for _, op := range []string{"Keys", "Scan"} {
				for _, arm := range []string{"filter", "prefix"} {
					b.Run(op+"/"+arm, func(b *testing.B) {
						reader := edgePrefixArm(b, db, arm == "prefix")
						for _, i := range []int{0, 73, 255} {
							if n := queries[i].verify(b, db, edges, op == "Scan", reader); n != 56 {
								b.Fatalf("ordinary tail yielded %d edges", n)
							}
						}
						cursor := 0
						ctx := b.Context()
						b.ReportAllocs()
						before := observe.IndexMetrics()
						for b.Loop() {
							counts, err := queries[cursor].run(ctx, db, op == "Scan", reader, nil)
							if err != nil || counts.matches != 56 {
								b.Fatalf("tail at %d: %d matches, %v", cursor, counts.matches, err)
							}
							cursor = (cursor + 73) % len(queries)
						}
						after := observe.IndexMetrics()
						b.ReportMetric(float64(after.PageReads-before.PageReads)/float64(b.N), "index-page-reads/op")
						b.ReportMetric(float64(after.LeafReads-before.LeafReads)/float64(b.N), "index-leaf-reads/op")
					})
				}
			}
		})
	}
}
