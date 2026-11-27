# Application-controlled key-ordered compaction

The implementation from [Issue #150](https://github.com/anaregdesign/tarp/issues/150) uses the existing DATA and catalog formats. The order proof is process-local. A service that reads many values under one prefix can request a key-ordered full DATA checkpoint after a bulk import or during a maintenance window.

## Public API since v0.14.0

```go
func WithKeyOrder() CompactOption
func (db *DB) Compact(ctx context.Context, opts ...CompactOption) (CompactResult, error)
func (db *TypedDB[K, V]) Compact(ctx context.Context, opts ...CompactOption) (CompactResult, error)
```

Call `db.Compact(ctx, tarp.WithKeyOrder())` to build a key-ordered full DATA checkpoint. Both the raw and typed calls are synchronous and accept cancellation through the caller's context. With no options, `db.Compact(ctx)` keeps source order and validates all captured DATA, even when it reuses whole snapshot segments. A zero-value `CompactOption` has no effect, and passing `WithKeyOrder()` more than once is equivalent to passing it once.

The application chooses when to run an ordered checkpoint. Automatic cleaning
continues to reclaim obsolete bytes and repack small segments.

The checkpoint cutoff sequence `C` falls at a complete committed batch boundary. On success, the snapshot segments contain one Put record for every retained live key at `C`, in strict encoded-key order across the segments in catalog order. DATA records matching an encoded key prefix can span consecutive snapshot segments. Each retained Put keeps its original user sequence. An irrecoverable live value prevents publication of the checkpoint. Typed keys use their encoded bytes, including composite field framing. Full and prefix scans have unspecified order. `ScanRange` and `KeysRange` follow encoded-key order with either checkpoint layout.

Applications can keep writing during the build. New commits form a sequence-ordered DATA log suffix, so the layout guarantee applies through `C`. For example, overwriting the first key a thousand times leaves its latest value in the DATA log suffix and neighboring DATA records in their snapshot segments. Repeated reads of a single random key and key-only scans offer little reason to schedule this job. The target workload is a value-producing `ScanPrefix` over related records.

During concurrent durable writes, both manual modes yield briefly between 256 KiB of DATA validation, copy and PARITY build work. With key order and BTree, the remap also yields between batches of at most eight address changes while a writer is active. An idle checkpoint skips those pauses. Choose a quiet window when foreground tail latency matters.

Both layouts of `Compact` share the manual maintenance slot with `CheckpointIndex` and `RefreshIndex`. A competing manual request receives `ErrBusy`. While an automatic cleaner owns the slot, wait with the caller's context. Honor cancellation during validation, sorting and copy, and check again before publication. Once publication starts, finish the existing outcome protocol. An uncertain CURRENT switch requires validated reopen. A cleanup error after publication reports a committed checkpoint. A deadline therefore controls admission and staging and cannot guarantee an exact wall-clock stop. See the [durability contract](durability.md#failure-outcomes).

## Repeat calls and the ordered base

Let `B` be the checkpoint cutoff sequence of the last selected full DATA checkpoint whose snapshot segments are known to be globally key-ordered. Let `C` be the last user sequence at this call's capture. Decide the path under the manual maintenance slot and writer gate.

| State | Work performed |
| --- | --- |
| Ordered base, `B == C`, healthy, no retirement due | Return immediately. Zero DATA, PARITY, index or directory reads, sort runs, index clones, rotations, manifest publications and worker restarts caused by the call. |
| Ordered base, `B < C` | Validate the selected DATA log suffix through `C`. Sort its mutations and merge only affected snapshot segments or populated gaps. Carry unchanged segment IDs and addresses into the new checkpoint. |
| Global order is unknown | Establish it once from validated DATA. If the captured snapshot prefix is already ordered, adopt it and process the outstanding suffix. Otherwise build the initial sorted checkpoint. |
| Known selected DATA damage, an inconsistent input, or uncertain publication | Use existing checked repair, recovery or fail-stop semantics as appropriate. A layout hint cannot justify dropping a record. |

Keep the fast-path decision ahead of `Store.Capture`, `index.View`, `hasUnavailableSource` and `Store.Cleanup`. Today these operations can enumerate the catalog, clone the Memory index, or list the directory. Maintain selected-damage status and retirement work when their states change. If cleanup or a directory sync is due, retry it against the recorded objects, report its outcome and retain pinned files. An unchanged ordered call with neither damage nor pending work takes constant work relative to database size. A writer admitted after that check belongs to the next call. If a write committed while the first call was building, the second call processes it. [Current checkpoint entry](../internal/engine/compact.go) and [cleanup](../internal/storage/store.go).

### Remembering what was sorted

The open handle keeps each certified snapshot segment's identity, record count and checked key boundaries. It stores fixed-size boundary summaries and caches full key bytes under a byte budget, using the probe described below on eviction. The proof belongs to the selected snapshot prefix and `B`. Rotation, INDEX publication and log-only cleaning can change MANIFEST ID or the pruning boundary without changing that base. An uncertain CURRENT switch requires validated reopen.

Full Open can recover the proof in the same pass that decodes and validates the selected snapshot records. It checks order between segments as well as within each segment. A committed log suffix does not affect the proof for `B`. If a valid INDEX supplies the covered prefix, Full Open checks the covered file digests but does not decode the record keys; order stays unknown. Indexed Open continues to defer record validation of the covered prefix and does not infer its order from INDEX. An ordered checkpoint can check an unknown base when requested.

Cleaning snapshot segments can remove DATA records or concatenate consecutive snapshot segments. When it filters or joins certified inputs in order, it updates just their descriptors with the surviving key boundaries and record counts. An ordinary `Compact` that only cleans up keeps the base. A source-order build certifies order only if both reused and new segments are ordered within and across their boundaries. Reusing a whole unordered segment does not certify it. An empty database has a trivially ordered base.

For separate application runs, the proposed durable extension records an ordered-base flag and a fixed-size summary for each snapshot segment's catalog entry in the CURRENT-selected MANIFEST. The summary stores the record count, the last record's header offset, and a digest of the two length-framed boundary keys. The first record starts after the segment header. The existing entry already binds the ID, length and DATA digest. Populate a bounded fence-key cache while producing a base. After reopen, the flag and checkpoint cutoff sequence answer the unchanged call directly. For a changed call with a cold cache, binary-search ranges by probing the two keys. Check segment identity, header CRCs, Put kinds, lengths and offsets, require the last record to end at the selected file length, and verify the pair digest before routing. This probe reads headers and key bytes. Payload CRC checks belong to the subsequent checked record reads. This can add boundary-key reads on otherwise reused files. It avoids a DATA-wide warmup and avoids copying arbitrarily long keys into each rotation's MANIFEST.

Qualify the same-handle mechanism first. Until durable summaries are implemented, reopen or restore may require one streaming order check. The durable design changes the single canonical catalog codec and its validation, fingerprint rules, backup and restore together. Validate count, offsets, framing and arithmetic before using summaries. Check the summaries against DATA during full record validation and restore. Indexed open may consume the selected metadata under its existing deferred-validation contract. Missing certification requests bootstrap. If validation finds an ordered, unmarked base with no tail, its first durable certification needs only a metadata publication. Malformed selected metadata follows the existing corruption policy. DATA records keep their current encoding. Measure the extra catalog bytes on foreground rotations.

## Building the initial checkpoint

1. Reuse the seal, capture and pin steps under the writer gate. Capture the index view and its live count at the same boundary. Scan, sort, read values and prepare PARITY outside the gate. The existing Memory view clones its map during capture. Measure this pause and its memory separately from the sorter.
2. In the healthy path, stream the selected DATA in catalog and file-offset order. Validate each header, record CRC, sealed digest and sequence interval, including superseded Puts and Deletes. Feed key, address and record kinds to the existing stable external replay sorter in both index modes. Its reducer already checks per-key transitions at the pruned-through sequence and emits one winning address per surviving key in encoded-key order. Compare every emitted address, including lengths and sequence, with the captured view, and check the final count against the captured live count. This uses one history sort for validation and ordering. [Replay reducer](../internal/storage/replay_sort.go) and [current copy validation](../internal/storage/checkpoint.go).
3. Read the winning records from the pinned sources. Start with a byte-bounded window of key-ordered winners. Within a window, issue reads in source file and offset order, validate key, sequence, lengths and CRC, then emit in the original key order. Stream a single record larger than the window through a fixed-size copy buffer. Use the existing snapshot writer for segment rollover at the effective `TargetSegmentBytes`, digest, sync and old-to-new relocations. The cost relative to source-order copy is a second read of the live payload, potentially scattered, plus sort-run I/O. Sorting keys separately within each source file would leave frequently scanned prefixes spread across those files.
4. Keep values out of healthy sort runs. Charge keys and descriptors to a run budget, and descriptors and buffered records to a separate read-window budget. Each window has a 4 MiB logical allowance. Account for large individual keys and merge buffers separately. Derive a job-wide staging allowance from available space on the DB volume. Reserve space for a foreground segment and its sidecar, and charge growing outputs and their projected PARITY as the build advances. Charge concurrent sort runs, merge output, recovery scratch and relocation spools before extending them, and release charges as files close. Pinned generations keep consuming disk space. Free-space estimates are advisory, so handle both budget exhaustion and filesystem ENOSPC before publication. Benchmark these initial budgets before exposing tuning options.
5. For damaged sources, attempt repair and require a verified copy of every live value. A value verified using recovered blocks or the value cache can exist only during that recovery pass. Stage those verified record bytes in private scratch, keep their original address for relocation, and feed their references through the same key-ordered output. Validate scratch framing, identity and checksum on read. A scratch failure aborts the attempt. If an unreadable key was concurrently superseded, take a fresh capture. If the unreadable version is still live, refuse the checkpoint. Register any new temporary filename pattern with workspace cleanup. [Recovery copy](../internal/storage/checkpoint_recovery.go) and [workspace ownership](../internal/storage/workspace.go).
6. Protect the completed outputs, sync and publish one MANIFEST via CURRENT. Splice in the entire committed DATA log suffix after `C` and clear the INDEX descriptor tied to the replaced layout. Remap only exact old addresses, so newer Puts and Deletes win. Release superseded segments after their index references and reader or backup pins drain. Raw and cleaned logs retain the sequence order required for replay. See the current [DATA and catalog format](format.md#catalog-payload).

## Advancing an ordered base

1. Seal and capture at one complete batch boundary. Pin the selected input identities and the immutable range directory. Capture the live index count under the writer gate. Use the selected base records and committed log as the authority for the merge. The healthy delta path needs no database-wide Memory index clone or traversal. Foreground writes need no extra dirty-key journal.
2. Validate every selected raw or cleaned log interval covering `(B, C]`, including record CRCs, sealed digests, chronological constraints and metadata-only intervals. Feed only the physically retained suffix events into a bounded stable sort by encoded key, preserving replay order within a key. Stream each key's mutations through the merge, even when a single hot key accounts for the entire suffix. The existing final-live-Put reducer cannot be used directly on the suffix. A Delete may need a predecessor in the base, and a final Delete must reach the merge.
3. Route each group by the base segment's inclusive key fences or the gap between segments. Open each affected segment once for a sequential merge, verify its complete contents and strict order, and combine its DATA records with the groups routed there. Seed replay for a key with its base Put, or absence in a gap. Apply every surviving mutation using the captured pruned-through sequence, which allows a retained Delete of a pruned predecessor. Emit only the winning Put with its original sequence. Repeated overwrites generate one output record. A same-value Put at a newer sequence still changes the record. An insert followed by a Delete in a gap can leave no output at all.
4. Split changed output at record boundaries using the effective segment target. Keep existing range boundaries for unaffected neighbors. A surviving insertion inside a range rewrites that range. A surviving insertion in a gap creates new output between its neighbors. Defer creating replacement DATA until the first difference. Retain the equal prefix in a spillable address list and copy from its pinned source if a difference appears. A range with the same final records and original addresses then needs no new DATA or PARITY. For untouched ranges, reuse the same DATA and PARITY identities. Their values do not enter the sorter or copy path.
5. Verify coverage by summing the record counts of reused and newly written snapshot segments and comparing the total with the captured live key count. Strict key order, disjoint output spans, verified input groups and exact winning source addresses establish membership. The count provides an additional check. Record relocations only for moved winners. Split the complete selected output list from the privately owned staging list so that retry/discard, parity construction, stats and retirement operate on the right IDs. Publish at the single checkpoint cutoff sequence `C`, preserve every concurrent tail commit, and install the new directory only after durable publication. The full recovery path uses a captured live index to verify the recovered state.

Reusing a segment keeps its previously selected identity and digest. The delta call verifies the inputs it consumes. A latent error in an untouched value can surface on its next checked read or integrity verification. Keep this distinction in the public API contract. Existing full validation, backup verification and repair retain their own guarantees.

The work scales with the **retained suffix records and affected segment bytes**. One update in a 4 MiB segment can cost a whole segment rewrite. Updates scattered across all segments can approach a full checkpoint. Even a sparse publishing call currently needs a flat MANIFEST covering all selected segments, with segment-level bookkeeping and pins that can cost O(number of segments). The zero-change path skips that publication entirely.

### Estimating rewrite work

`db.EstimateKeyOrderCompaction(ctx)` on a raw or typed DB returns the current
sequence, checkpoint cutoff, known-order state and retained log record bytes.
Use `RewriteRecordBytesUpperBound` to decide whether to call `Compact` with
`WithKeyOrder` now or coalesce changes for a later checkpoint. While maintenance
updates the index to match a newly published layout, the estimate returns
`ErrBusy`.

The estimate looks at selected file sizes and live record counters, with work
proportional to the number of catalog entries and zero DATA I/O. It allows for
copying all live records from snapshot segments with overwritten or deleted
records. For other snapshot segments, each live suffix key can affect at most
one previously unchanged range. The bound charges the largest such range per
key, capped at the total bytes in those segments. It also includes live suffix
record bytes. Unknown order or known DATA damage uses all live record bytes.

The bound applies to encoded records emitted into new snapshot segments in a
single healthy attempt at the estimated cut. Reads, DATA headers, repairs,
retries, PARITY, scratch, INDEX, metadata, cleanup and filesystem write
amplification are separate costs. Writers and maintenance can change the layout
after estimation. The estimate does not impose a budget on `Compact`.

## Adoption and follow-up

In the experimental 16,384-record graph, the key-ordered layout reduced the mean number of distinct 4 KiB DATA file ranges covered by matching records per vertex prefix scan from 70.89 to 9.00. The key-ordered mean was 9.01 after 4,096 overwrites of the first key, and 24.82 after 4,096 distributed overwrites. Before updates, sampled cold prefix scans fell from 296 to 112 KiB in Linux VM process reads and from 2.34 to 0.084 ms in latency. The prototype staged up to 32 MiB and 65,536 records in RAM. [Experiment and sampling details](https://github.com/anaregdesign/tarp/blob/fedad012a6d88170f59741ddd1048bc37d54943b/docs/benchmarks/key-locality-150.md).

Compare foreground alone, source-order compaction, and key-ordered compaction on the same starting image. Use both index modes, variable-size edges, a database larger than the sort budget, and workloads with a repeatedly updated first key, hot ranges, and scattered updates. Include deletes and reinsertions. Measure cold prefix latency and process read bytes, distinct 4 KiB DATA file ranges per scan, full DATA checkpoint duration, validation and copy reads, DATA/PARITY and scratch writes, peak memory, scratch and pinned bytes. Record durable Put throughput and p99 during the overlapping window. Issue #150 proposes at least 1,000 overlapping Puts per pattern, throughput at least 95% of foreground alone, p99 at most 1.10× baseline and a measurable cold-read improvement. Report the ordinary-compaction comparison separately to isolate the cost of sorting.

### Hosted Linux qualification

Revision `2af4ec7d562f` ran in a 2-CPU container on ext4 with `go version go1.27.4 linux/amd64`. In the raw data, `source` selects the source-order layout and `ordered` selects the key-ordered layout. Every arm used image `sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. [Qualification run](https://github.com/anaregdesign/tarp/actions/runs/44983593909) · [Raw per-trial measurements and environment](benchmarks/key-ordered-checkpoint.jsonl.gz).

The fixture contained 16,385 variable-size DATA records and 4,653,340 encoded key bytes, exceeding the 4 MiB sort allowance. Each writer trial checked all final values after a Full reopen.

Median of three cold `ScanPrefix` calls after requesting eviction of fixture DATA from the OS page cache. Matching entries are the key-value pairs yielded by each scan. Read volumes are Linux process `read_bytes` reported in KiB. The full-read control registered I/O on each fixture.

| Index | Prefix | Matching entries | Source-order Linux process read volume (KiB) | Key-order Linux process read volume (KiB) | Source-order ms | Key-order ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTree | 0 | 56 | 244 | 64 | 8.24 | 0.68 |
| BTree | 192 | 56 | 264 | 88 | 8.99 | 0.90 |
| BTree | 384 | 2048 | 7980 | 1380 | 240.39 | 14.99 |
| Memory | 0 | 56 | 244 | 48 | 9.66 | 2.67 |
| Memory | 192 | 56 | 264 | 88 | 9.42 | 2.01 |
| Memory | 384 | 2048 | 8488 | 1508 | 212.82 | 38.51 |

Each writer trial issued four groups of 256 durable Puts with one simultaneous checkpoint per group. Ratios below compare the same foreground operation slots to a foreground-only trial, then take the median of three paired trial ratios. The joined rate divides Puts by total elapsed time including the wait for each checkpoint. The final column gives DATA and PARITY MiB written by four ordered checkpoints.

| Index | Updates | Overlapping Puts | Source-order rate / p99 | Key-order rate / p99 | Ordered joined rate | Ordered DATA / PARITY MiB |
| --- | --- | ---: | --- | --- | ---: | --- |
| BTree | first | 3072 | 73% / 2.62× | 97% / 1.66× | 75% | 16.00 / 1.03 |
| BTree | hot | 3072 | 75% / 2.70× | 97% / 1.79× | 75% | 16.00 / 1.03 |
| BTree | spread | 3072 | 81% / 1.57× | 96% / 1.12× | 63% | 41.57 / 2.68 |
| Memory | first | 2610 | 83% / 2.41× | 91% / 2.30× | 93% | 16.00 / 1.03 |
| Memory | hot | 2587 | 83% / 2.55× | 90% / 2.09× | 92% | 16.00 / 1.03 |
| Memory | spread | 3072 | 83% / 2.22× | 93% / 1.68× | 76% | 41.57 / 2.68 |

Cold prefix reads improved in all six cases. Ordered checkpoints kept 96–97% of foreground-only throughput in BTree and 90–93% in Memory during overlapping Puts. All six p99 ratios exceeded the 1.10× target. An application with a foreground latency requirement should schedule this work in a quiet window. This run measured physical output bytes and process read bytes. Peak memory, scratch and pinned space still need separate measurement.

### Additional qualification cases

Tests should walk physical DATA offsets across multiple output files, verify the exact keys and original sequences against a captured oracle, and exercise unchanged-cutoff calls, empty images and oversized records. Cover both index modes, concurrent cross-boundary batches, delete/reinsert, pinned scans and backups, Full and Indexed reopen, backup/restore, recovery from damaged DATA, scratch exhaustion, cancellation, uncertain publication and retry. Check file ownership and the selected image after failures.

Make work counts acceptance criteria. Call twice without a write on increasing numbers of keys and snapshot segments. Instrument the second call and work it schedules through completion, and require the zeroes in the repeat-call table. Cover an append during the first build, an ordinary cleanup-only checkpoint, INDEX publication, rotation, snapshot cleaning, pending retirement and known damage. Repeat with and without a durable base across reopen and restore. For one changed key with warm fences, require zero unchanged-segment DATA reads, writes, PARITY rebuilds and relocations. Distinguish boundary-key probes on a cold fence cache, segment-level metadata work and the cost of all retained delta events. Hold the delta fixed while increasing database size. Verify resulting contents against a full replay oracle outside timing, including single-record snapshot segments, long boundary keys, an empty cleaned interval, a retained Delete whose Put was pruned, insertions inside ranges and gaps, and batches updating multiple ranges.
