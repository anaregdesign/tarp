package benchmark

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
)

// Each timed group starts from an independent, synced clone of the same closed
// seed. Only the chosen public calls are timed. The seed and one group fit in a
// single segment, keeping rotation and reclamation out of this comparison.
func BenchmarkBatchWrite(b *testing.B) {
	for _, count := range []int{1, 16} {
		keys, payload, config := measurementInput(1024, count)
		opts := tarp.Options(config)
		seed := filepath.Join(b.TempDir(), "seed")
		db, err := tarp.Open(seed, opts)
		if err != nil {
			b.Fatal(err)
		}
		base := make([]tarp.Entry, count)
		updates := make([]tarp.Entry, count)
		typedKeys := make([]string, count)
		typedUpdates := make([]tarp.TypedEntry[string, []byte], count)
		for i, key := range keys {
			base[i] = tarp.Entry{Key: key, Value: payload}
			value := bytes.Clone(payload)
			value[0] = byte(i + 1)
			updates[i] = tarp.Entry{Key: key, Value: value}
			typedKeys[i] = string(key)
			typedUpdates[i] = tarp.TypedEntry[string, []byte]{Key: typedKeys[i], Value: value}
		}
		if err := db.BatchPut(base); err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
		for _, api := range []string{"raw", "typed"} {
			if api == "typed" && count == 1 {
				continue
			}
			for _, operation := range []string{"Put", "Delete"} {
				for _, mode := range []string{"batch", "serial"} {
					name := fmt.Sprintf("%s/%s/keys=%d/%s", api, operation, count, mode)
					b.Run(name, func(b *testing.B) {
						b.StopTimer()
						b.ReportAllocs()
						b.ResetTimer()
						var commits uint64
						for i := 0; i < b.N; i++ {
							dir := filepath.Join(b.TempDir(), "db")
							measurementCloneClosed(b, seed, dir)
							raw, err := tarp.Open(dir, opts)
							if err != nil {
								b.Fatal(err)
							}
							if api == "typed" {
								typed, err := tarp.Bind(raw, tarp.StringCodec{}, tarp.BytesCodec{})
								if err != nil {
									b.Fatal(err)
								}
								timeWriteGroup(b, operation, mode, typedKeys, updates, typedUpdates,
									typed.Put, typed.Delete, typed.BatchPut, typed.BatchDelete)
							} else {
								timeWriteGroup(b, operation, mode, keys, updates, updates,
									raw.Put, raw.Delete, raw.BatchPut, raw.BatchDelete)
							}
							s := raw.Stats()
							wantCommits := 1
							if mode == "serial" {
								wantCommits = count
							}
							if s.WriteCommits != uint64(wantCommits) || s.WrittenRecords != uint64(count) || s.SelectedSegments != 1 {
								b.Fatalf("post-write stats: %+v, want %d commits, %d records in one segment", s, wantCommits, count)
							}
							commits += s.WriteCommits
							if err := raw.Close(); err != nil {
								b.Fatal(err)
							}
							verifyWriteGroup(b, dir, opts, keys, updates, operation)
						}
						b.ReportMetric(float64(commits)/float64(b.N), "commits/op")
						b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*count), "ns/key")
					})
				}
			}
		}
	}
}

func timeWriteGroup[K, E any](b *testing.B, operation, mode string, keys []K, values []tarp.Entry, entries []E,
	put func(K, []byte) error, del func(K) error, batchPut func([]E) error, batchDelete func([]K) error,
) {
	b.Helper()
	var err error
	switch {
	case operation == "Put" && mode == "batch":
		b.StartTimer()
		err = batchPut(entries)
	case operation == "Delete" && mode == "batch":
		b.StartTimer()
		err = batchDelete(keys)
	case operation == "Put":
		b.StartTimer()
		for i, key := range keys {
			if err = put(key, values[i].Value); err != nil {
				break
			}
		}
	default:
		b.StartTimer()
		for _, key := range keys {
			if err = del(key); err != nil {
				break
			}
		}
	}
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
}

func verifyWriteGroup(b *testing.B, dir string, opts tarp.Options, keys [][]byte, updates []tarp.Entry, operation string) {
	b.Helper()
	db, err := tarp.Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	}()
	got, err := db.BatchGet(keys)
	if err != nil || len(got) != len(keys) {
		b.Fatalf("reopened BatchGet: %d results, %v", len(got), err)
	}
	for i, r := range got {
		if operation == "Delete" {
			if r.Found {
				b.Fatalf("deleted key %q survived reopen", keys[i])
			}
		} else if !r.Found || !bytes.Equal(r.Value, updates[i].Value) {
			b.Fatalf("overwritten key %q did not match after reopen", keys[i])
		}
	}
}
