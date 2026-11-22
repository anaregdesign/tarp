# Prefix iteration for Lantern adjacency reads

[Issue #148](https://github.com/anaregdesign/tarp/issues/148) evaluated the `KeysPrefix` and `ScanPrefix` methods selected for adoption in [PR #149](https://github.com/anaregdesign/tarp/pull/149). The 2026-11-22 comparison uses production source at `59ddf199` for the original API and `678dd2c9` for the measured implementation. Both versions read the same storage and key formats.

For an interleaved graph of 16,385 edges, decoding the 56 outgoing edges for a typical vertex took **95.27 ms** on the original typed B+tree scan and **0.324 ms** on the prototype. The prototype can return just the heads through the key index. Fetching the weights also requires reading DATA. After evicting DATA, a query that returned 2,048 edges scattered throughout the fixture read nearly all its pages.

## API and access paths

`KeysPrefix(ctx, encodedPrefix)` returns keys. `ScanPrefix(ctx, encodedPrefix)` returns keys and values. Both raw and typed handles expose the same methods. Each call copies the binary prefix and each iteration captures a fresh snapshot, with caller-owned results and unspecified order. Empty or nil selects all keys. For a typed composite key, the encoded prefix must include the framing for each complete leading field. The test for the following approach includes overlapping Tail names, NUL and multibyte lengths.

```go
// edges is a TypedDB whose key is struct{ Tail, Head string }.
tailCodec, err := tarp.NewCodec[struct{ Tail string }]()
if err != nil {
    return err
}
prefix, err := tailCodec.Encode(struct{ Tail string }{Tail: tail})
if err != nil {
    return err
}
for key, err := range edges.KeysPrefix(ctx, prefix) {
    if err != nil {
        return err
    }
    visit(key.Head)
}
```

Both indexes use the same half-open encoded byte range. B+tree performs a lower-bound seek against a captured root and streams until the exclusive upper bound. The memory hash index visits its live keys while holding the snapshot read lock and copies matching key/address pairs into small slices. Matching happens before typed decode and before any DATA read. For value snapshots the engine still pins all current DATA files during setup. Key-only snapshots skip those pins.

## Decision measurements

The tables report medians of three independent rounds on the same Docker Desktop Linux/arm64 VM, ext4 volume, CPU 2, `GOMAXPROCS=1`, and `go version go1.27.4 linux/arm64`. Setup, correctness checks and initial warming precede timing. The warm filtered baseline is the original public iterator followed by a client-side predicate. The binary outcomes, allocations and timings for both write orders are in the [full matrix](prefix-148.csv). Each ratio is baseline elapsed time divided by prototype elapsed time.

### Typed outgoing edges

Each key is a 24-byte encoded `struct{Tail, Head string}`. Each 405-byte value has a Put floor and three synthetic Contributions with weight, expiration, HLC and identity. The baseline decodes rows before comparing Tail. The prototype uses a 12-byte framed Tail prefix and decodes matches. The fixture uses the same endpoint distribution and fixed random write order as the raw interleaved fixture. This is a storage API benchmark. It excludes the Lantern RPC, graph mutation, replication and ranking paths.

| Index | Iterator | Filter | Prefix | Ratio | Filter allocations | Prefix allocations |
| --- | --- | --- | --- | --- | --- | --- |
| BTree | Keys | 3.63 ms | 11.1 µs | 328× | 81,939 | 199 |
| BTree | Scan | 95.3 ms | 324 µs | 294× | 294,951 | 929 |
| Memory | Keys | 2.77 ms | 297 µs | 9.33× | 65,618 | 203 |
| Memory | Scan | 94.4 ms | 646 µs | 146× | 278,629 | 933 |

### Raw iterator selectivity

The raw fixture holds 16,385 keys across 258 tails, with 96 bytes per value. A missing interior Tail yields zero, sparse yields one, medium yields 56, and broad yields 2,048. First stops after one result from the 2,048-edge Tail. Empty scans all 16,385. The table shows the fixed random write order. The grouped arm writes each Tail consecutively. The oracle checks the keys, value contents and duplicate entries against the inserted records outside timing. Timed iterations also verify the match count.

| Index | Case | Keys + filter | KeysPrefix | Ratio | Scan + filter | ScanPrefix | Ratio |
| --- | --- | --- | --- | --- | --- | --- | --- |
| BTree | missing | 2.21 ms | 0.92 µs | 2,395× | 8.25 ms | 1.09 µs | 7,559× |
| BTree | sparse | 2.22 ms | 1.08 µs | 2,044× | 8.30 ms | 1.61 µs | 5,168× |
| BTree | medium | 2.19 ms | 5.65 µs | 388× | 8.45 ms | 25.7 µs | 329× |
| BTree | broad | 2.21 ms | 171 µs | 12.9× | 8.49 ms | 905 µs | 9.39× |
| BTree | first | 1.42 ms | 0.96 µs | 1,483× | 5.49 ms | 1.50 µs | 3,667× |
| BTree | empty | 2.18 ms | 2.11 ms | 1.03× | 8.36 ms | 7.97 ms | 1.05× |
| Memory | missing | 1.23 ms | 282 µs | 4.37× | 7.33 ms | 281 µs | 26.1× |
| Memory | sparse | 1.22 ms | 307 µs | 3.97× | 7.47 ms | 306 µs | 24.4× |
| Memory | medium | 1.19 ms | 283 µs | 4.22× | 7.81 ms | 300 µs | 26.0× |
| Memory | broad | 1.22 ms | 437 µs | 2.79× | 7.84 ms | 1.15 ms | 6.81× |
| Memory | first | 324 µs | 368 µs | 0.88× | 334 µs | 379 µs | 0.88× |
| Memory | empty | 1.22 ms | 1.08 ms | 1.13× | 7.59 ms | 6.90 ms | 1.10× |

For early termination, the memory index still captures the full matching subset before the first yield. In this fixture the client filtering its ordinary hash snapshot can encounter a member of the large Tail after only a few candidates. Capture on the prototype can hold the writer off while it traverses the live hash table. These runs measure one reader and do not quantify concurrent writer latency.

### B+tree working set

Each measured sample cycles once through all 256 ordinary tails, stepping by 73 and requesting 56 results per tail. The budgets are the configured B+tree memory budgets. The pager counts a page read when the index must fetch a page from its file. The OS can satisfy that read from cache.

| Budget | Iterator | Filter | Prefix | Ratio | Filter page reads/query | Prefix page reads/query |
| --- | --- | --- | --- | --- | --- | --- |
| 1MiB | Keys | 3.44 ms | 8.10 µs | 424× | 151 | 0.53 |
| 1MiB | Scan | 9.87 ms | 27.7 µs | 357× | 151 | 0.53 |
| 16MiB | Keys | 2.26 ms | 4.36 µs | 518× | 0.00 | 0.00 |
| 16MiB | Scan | 8.56 ms | 23.7 µs | 362× | 0.00 | 0.00 |

### DATA after fixture-only eviction

Both arms restore the same immutable backup for each write order. Each restored dataset has 13 DATA files with a 256 KiB target segment size. Before each trial the driver syncs those files and issues `POSIX_FADV_DONTNEED`. It leaves the B+tree cache warm. A subsequent full-read control charged 2,613,248 to 2,834,432 bytes to the process across the four arm/layout controls. The measurements below include Linux readahead and all process threads. Linux VM process `read_bytes` is the observation point. Physical SSD I/O was outside the measurement. All 24 key-only trials across the two arms reported zero process read bytes.

| Write order | Matches | Scan + filter | ScanPrefix | Ratio | Filter read KiB | Prefix read KiB |
| --- | --- | --- | --- | --- | --- | --- |
| grouped | 56 | 9.19 ms | 107 µs | 86.1× | 2,768 | 52.0 |
| grouped | 2,048 | 9.25 ms | 1.02 ms | 9.03× | 2,768 | 356 |
| interleaved | 56 | 24.5 ms | 1.34 ms | 18.4× | 2,768 | 240 |
| interleaved | 2,048 | 23.9 ms | 18.8 ms | 1.27× | 2,768 | 2,764 |

The encoded prefix groups the index entries. DATA records follow write order. This explains how the prototype can skip decoding thousands of unrelated rows yet still read across most of the selected DATA for a high-degree Tail written in shuffled order.

## B+tree tuning history

Each row below is a separately built and measured revision. These are earlier tuning runs on the pinned image. The paired decision measurements above were run again on the final code. Early stopping, iterator reuse, mutation of returned buffers, concurrent reads and close were checked separately from timing.

| B+tree version | 1 key | 56 keys | 2,048 keys | 2,048 key allocations | 2,048 values |
| --- | --- | --- | --- | --- | --- |
| v1 | 1.02 µs | 7.98 µs | 250 µs | 4,113 | 1.01 ms |
| v2 | 1.04 µs | 7.51 µs | 203 µs | 4,114 | 915 µs |
| v3 | 1.05 µs | 6.11 µs | 190 µs | 2,201 | 962 µs |
| v4 | 1.05 µs | 5.64 µs | 152 µs | 272 | 936 µs |

- **V1** (`bea50277`) added seek and streamed one item per reader permit. In a 2 s profile for 2,048 keys, the original per-entry cursor accounted for roughly half of sampled CPU time including descendants.
- **V2** (`07fac71a`) combined seek and the first result, then copied up to 16 short entries from one leaf per permit. Each callback runs with the permit released. Overflow keys are processed individually. The 2,048-key profile attributed 24% cumulative CPU time to short string concatenation.
- **V3** (`b9a835b2`) reconstructed short keys into one owned string per group. It used the last candidate to skip individual upper-bound comparisons when possible. The first match and overflow keys still take the single-item path.
- **V4** (`678dd2c9`) gave short caller-owned keys disjoint slices in a 512-byte key-copy buffer after four small results. Each returned slice has capacity equal to length. A retained short key can keep its key-copy buffer alive. This optimization is shared by the two index modes and both iterators.

| Tuning variant | First key | 56 keys | 2,048 keys | 2,048 values |
| --- | --- | --- | --- | --- |
| w8 | 0.99 µs | 6.24 µs | 189 µs | 946 µs |
| v3 | 0.96 µs | 6.11 µs | 190 µs | 962 µs |
| w32 | 1.01 µs | 6.14 µs | 187 µs | 912 µs |
| w64 | 1.02 µs | 5.96 µs | 179 µs | 991 µs |
| s256 | 0.93 µs | 5.62 µs | 158 µs | 948 µs |
| s512 | 0.99 µs | 5.33 µs | 149 µs | 912 µs |
| s1024 | 1.00 µs | 5.85 µs | 154 µs | 924 µs |

The w8/w32/w64 rows change only V3's leaf group width from 16 to 8/32/64. The s256/s512/s1024 rows compare 256/512/1,024-byte key-copy buffers on V4. The width sweep traded first-result latency and buffer size for modest changes to larger scans. In the final key-copy buffer sweep, 512 produced the lowest median for the 56-key, 2,048-key and 2,048-value queries among those three sizes. The prototype retains width 16 and a 512-byte key-copy buffer. The remaining cost for shuffled, DATA-evicted high-degree reads is the spread of matching records across DATA file offsets. Reorganizing DATA for graph locality would require evaluating a different write/compaction policy.

### Memory snapshot tuning

| Memory version | 56 keys | Bytes / 56 keys | 2,048 keys | Bytes / 2,048 keys | First of 2,048 |
| --- | --- | --- | --- | --- | --- |
| v1 | 703 µs | 2,100,928 | 763 µs | 2,164,672 | 301 µs |
| v2mem | 259 µs | 10,153 | 605 µs | 590,361 | 491 µs |
| v3mem | 275 µs | 6,625 | 444 µs | 206,817 | 342 µs |
| v4 | 278 µs | 6,545 | 438 µs | 199,825 | 371 µs |

V1 cloned the full hash table. The bounded map (`7a44bca`) copied only matching addresses. The bounded slices (`a10e6bc`) removed the rehash work for captures with many matches. V4 also includes the caller-owned short-key optimization. Memory measurements include transient allocations. The full matrix lists minimum and maximum round averages alongside the median. Concurrent service latency percentiles were outside this campaign.

## Reproduction and evidence

The runner pins the multi-platform Go image digest `sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. The tested arm64 image ID is `sha256:4cfad6c86f5248fedf89ff52d3e120187779bf26bfeeb8265047464d3e6c226c`. Go 1.27.4 was the latest stable toolchain at campaign start. [Go releases](https://go.dev/dl/)

From a checkout containing the runner, fetch the PR history to make the measured commit available and use a fresh result directory. It builds the original production code with only the four benchmark drivers overlaid and builds the prototype from its commit. It alternates warm arm order across rounds and seeds each cold fixture once. Warm samples use Go's 100 ms calibration except for the 256-query rotating workload.

```sh
git fetch origin refs/pull/149/head
benchmark/scripts/container-edge-prefix.sh /path/to/new-results 59ddf199 678dd2c9 3
```

The [raw evidence archive](prefix-148-raw.tar.gz) contains all 384 decision samples, VM and source metadata, tuning logs, CPU profiles and one-line parameter patches. The accompanying `summarize.py` validates sample counts, expected matches and fixture identities and regenerates the [matrix](prefix-148.csv). Within the unpacked archive, run `python3 summarize.py`. The decision sample count is 288 raw warm, 24 typed warm, 24 rotating and 48 DATA-evicted observations.
