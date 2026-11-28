package benchmark

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/format"
)

// The same four Put records fill the configured 4 MiB target exactly. One prior
// Put for the first key forces rotation and leaves the same final key/value set.
func BenchmarkBatchPutSegmentBoundary(b *testing.B) {
	const targetBytes = 4 << 20
	const batchBytes = targetBytes - format.SegmentHeaderBytes
	const count = 4
	options := tarp.Options{TargetSegmentBytes: targetBytes}
	keys := make([][]byte, count)
	entries := make([]tarp.Entry, count)
	for i := range entries {
		keys[i] = []byte(fmt.Sprintf("%016x", i))
		valueBytes := batchBytes/count - format.RecordHeaderBytes - len(keys[i])
		entries[i] = tarp.Entry{Key: keys[i], Value: bytes.Repeat([]byte{byte(i + 1)}, valueBytes)}
	}
	over := slices.Clone(entries)
	over[0].Value = append(bytes.Clone(over[0].Value), 0)

	for _, mode := range []string{"empty", "rotate"} {
		b.Run(mode, func(b *testing.B) {
			b.StopTimer()
			b.ReportAllocs()
			b.SetBytes(batchBytes)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dir := filepath.Join(b.TempDir(), "db")
				db, err := tarp.Open(dir, options)
				if err != nil {
					b.Fatal(err)
				}
				if mode == "rotate" {
					if err := db.Put(keys[0], []byte("old")); err != nil {
						b.Fatal(err)
					}
				}
				before := db.Stats()
				if err := db.BatchPut(over); !errors.Is(err, tarp.ErrTooLarge) || db.Stats() != before {
					b.Fatalf("one-byte-over preflight: err=%v before=%+v after=%+v", err, before, db.Stats())
				}

				b.StartTimer()
				err = db.BatchPut(entries)
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				after := db.Stats()
				segments := uint64(1)
				if mode == "rotate" {
					segments++
				}
				wantBytes := before.SelectedDataBytes + batchBytes + (segments-1)*format.SegmentHeaderBytes
				if after.LiveKeys != count || after.SelectedSegments != segments || after.SelectedDataBytes != wantBytes ||
					after.WriteCommits != before.WriteCommits+1 || after.WrittenRecords != before.WrittenRecords+count {
					b.Fatalf("boundary publication: before=%+v after=%+v, want %d segments and %d bytes", before, after, segments, wantBytes)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				verifyWriteGroup(b, dir, options, keys, entries, "Put")
			}
		})
	}
}
