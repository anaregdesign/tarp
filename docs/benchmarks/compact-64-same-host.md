# Same-host Compact comparison with 64-byte values

On the same Linux/amd64 guest, the median manual Compact under concurrent reads and durable writes took **879 ms on v0.8.0 and 2,054 ms on v0.9.0**. v0.9.0 took longer in all six pairs. Both releases used the default in-memory index. The previous measurement on separate hosts gave 571 ms and 2,306 ms. The slowdown reproduced on this host, with a smaller ratio. [Original investigation](https://github.com/anaregdesign/tarp/issues/102) · [Earlier VM results](index-vm.md)

## Individual timings

Each row is one matched pair per condition. Times are milliseconds for the complete Compact call in uninstrumented release builds. The overlap condition runs one hot Get worker, one durable Put worker, and the original 5 ms inventory sampler. The parked condition has all three stopped. Both clone the same closed fixture, add the same writer key, warm eight keys and hold the automatic cleaner until the manual checkpoint completes.

| Round | First release | Overlap v0.8.0 | Overlap v0.9.0 | Parked v0.8.0 | Parked v0.9.0 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | v0.8.0 | 1,002.8 | 2,932.0 | 332.0 | 927.9 |
| 2 | v0.9.0 | 928.4 | 2,036.8 | 730.3 | 733.8 |
| 3 | v0.8.0 | 855.3 | 2,070.6 | 329.8 | 434.3 |
| 4 | v0.9.0 | 826.5 | 1,866.1 | 316.7 | 442.8 |
| 5 | v0.8.0 | 838.2 | 2,754.3 | 345.2 | 470.4 |
| 6 | v0.9.0 | 901.9 | 1,706.8 | 317.1 | 448.3 |
| **Median** | | **878.6** | **2,053.7** | **330.9** | **459.4** |

The median within-pair ratio was **2.34× with traffic** and **1.38× parked**. In the foreground-only control both releases completed reads and durable writes with zero records copied. In the six overlap windows, the median of the per-window durable Put p99s was 77.3 ms on v0.8.0 and 97.7 ms on v0.9.0. These short windows contain 21–30 and 57–127 Puts respectively, so their tails are sensitive to individual operations.

## Where the time went

Four additional pairs ran with stage timers. The table shows median wall time in milliseconds. The timers add measurable overhead, especially to v0.9.0. Use the six uninstrumented pairs above for the release comparison. Some intervals below nest or run concurrently.

| Diagnostic interval | v0.8.0 | v0.9.0 |
| --- | ---: | ---: |
| Whole Compact | 1,015 | 3,106 |
| Build checkpoint, including validated replay and copy | 876 | 1,544 |
| Validated replay, excluding the callback that emits live records | 160 | 265 |
| Copy the 4,097 live records, inside checkpoint construction | 658 | 861 |
| Relocate index entries after checkpoint publication | 9 | 1,420 |
| ↳ Waiting for the writer lock during relocation | 0 | 1,165 |
| ↳ Relocation work excluding that wait | 9 | 229 |
| Other Compact writer-lock waits, including capture, output reservation and publication | 64 | 78 |
| Foreground write syncs overlapping Compact | 578 | 1,887 |

**The largest measured increase was in relocation.** v0.9.0 processes up to 32 relocations at a time and acquired the writer lock **129 times** in every diagnostic run. Its median within-run share of relocation time waiting for that lock was **83%**. v0.8.0 remapped entries under the state lock and acquired the writer lock zero times during relocation. Foreground writes hold the writer lock across sync. Their sync time overlaps the Compact intervals, so it cannot be added to them or used to identify the holder behind every wait. Checkpoint construction also grew, and the parked measurements show that concurrent traffic explains only part of the slowdown. [v0.8.0 remap](https://github.com/anaregdesign/tarp/blob/v0.8.0/internal/engine/compact.go) · [v0.9.0 relocation](https://github.com/anaregdesign/tarp/blob/v0.9.0/internal/engine/index.go#L103)

The first optimization to test is coalescing bounded relocation batches before taking the writer lock for the memory index. Retain the byte bound, the old-address check and the atomic index/file-accounting update. Compare Compact duration *and* foreground Put latency and lock hold times; larger batches could make individual writers wait longer. The disk-index path was outside this comparison. [Optimization follow-up](https://github.com/anaregdesign/tarp/issues/108)

## Controls and reproduction

- All Compact calls copied **4,097 records / 524,480 bytes**. Both releases reopened every clone with the expected UUID, state digest, 4,097 live keys and a final sequence matching the acknowledged foreground Puts. In the pinned-backup control, each release restored its backup and unlinked every witnessed retired file after the lease was released.
- Both arms used one GitHub-hosted Intel Xeon Platinum 8573C runner, one Ubuntu 24.04 guest with Linux 6.8.0-147-generic, QEMU 7.2.22 TCG, two guest vCPUs, GOMAXPROCS=2 and ext4 ordered mode on the same disposable block device. The flush controls passed. This campaign measures this guest and fixture.
- The archives of v0.8.0 (`dbb553e`) and v0.9.0 (`d09d792`) were compiled with Go 1.27.4 in `golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. Each trial used a fresh process. The same guest and filesystem stayed up throughout, including both release orders.
- The [evidence archive](compact-64-same-host.tar.gz) retains the 37 original JSON rows, console log, flush/guest verdicts, binary hashes and host/toolchain metadata. Archive SHA-256 is `bf3c99eb4fea5bb52bd814909a60cf3446e8366556b8a68050573d5813e5c2f1`. [GitHub Actions run](https://github.com/anaregdesign/tarp/actions/runs/44756328658)
- To repeat the experiment, dispatch the VM qualification workflow with mode `compact-compare`, confirmation `disposable-vm-only`, and value size 64. For an exact reproduction, select the image digest above. The host controller validates the expected release, condition and round for every guest row before accepting the run.
