# Key-family DATA layout

[Issue #194](https://github.com/anaregdesign/tarp/issues/194) evaluates whether
key-family boundaries justify a storage layout change for Lantern-style data.
Ordinary key ordering provided most of the measured Vertex scan improvement.
Family boundaries made a particular mixed hot/cold snapshot reclaimable at the
cost of more DATA files. Keep ordinary key ordering as the default. Consider
an opt-in boundary policy once measurements from actual replay and maintenance
show that obsolete hot rows regularly remain in cold DATA files.

## Workload

One DB contains shuffled input for 1,536 cold Vertex rows at 16 KiB each, 4,096
hot Edge rows at 1 KiB, and 32 relay rows at 128 B. Keys use the first ten
encoded bytes for domain, generation and family. A durable overwrite and 128
deletions and reinserts add history before the common initial cut. Every arm
starts from a fresh copy of that closed image, with a 32 MiB DATA target, 8 MiB
B+tree memory budget, 1 MiB value cache and 16 read handles.

- **Interleaved** uses the normal chronological DATA log and its older rows.
- **Ordered** runs the existing `Compact(WithKeyOrder())`.
- **Family-boundary** runs the same checkpoint with a measurement-only Go
  overlay that rolls its ordered output file when the family changes.

The driver times whole manual checkpoints, including history replay, sorting,
copy, PARITY and publication. It checks the physical DATA family membership
and matches the whole logical state and sequence across all arms. Each arm then
scans Vertex and Edge values, applies one durable mixed Edge and relay update,
and checks that one pinned Snapshot sees the old pair and a new Snapshot sees
the new pair. The normal cleaner runs from its parked start to its first idle
result after Snapshot release. Full reopen checks the entire data digest and
the committed sequence. The layout experiment uses the same active DATA log
and the same publication protocol in all arms.

## Results

Measured 2026-11-28 at `55a89ff`, using the versioned measurement-only overlay
and driver on Apple M3 Max, Docker Desktop 29.5.3, Linux 6.12.76-linuxkit
arm64, and a private ext4 Docker volume. Every arm used two CPUs and
`GOMAXPROCS=2` with Go 1.27.4 from
`docker.io/library/golang@sha256:ce59b41415e2524ae99926fe1f37ce48ba8c194dcbd2a56a65436cdcefa2bd34`.
The results directory was on the external SSD. Docker did not complete
creation of a container with that SSD bind-mounted, so the fixture DATA was
stored on the Docker VM volume. The reported reads are Linux process I/O
counts, which include read-ahead and can be served from host caches.

Medians of three rotated rounds follow. The advised scans followed
fixture-only `POSIX_FADV_DONTNEED`. Immediate warm repeats had zero charged
storage read bytes in all 18 samples.

| Metric | Interleaved | Ordered | Family-boundary |
| --- | ---: | ---: | ---: |
| Nonempty initial DATA files / mixed files | 2 / 2 | 1 / 1 | 3 / 0 |
| Initial full ordered checkpoint | — | 114.09 ms | 114.82 ms |
| Vertex value scan, advised | 58.79 ms | 10.88 ms | 10.51 ms |
| Edge value scan, advised | 5.16 ms | 3.54 ms | 3.63 ms |
| Vertex value scan, warm repeat | 8.98 ms | 8.49 ms | 7.70 ms |
| Edge value scan, warm repeat | 3.75 ms | 2.77 ms | 2.51 ms |
| Mixed Apply, 4,128 records | 18.18 ms | 21.38 ms | 19.12 ms |
| Cleaner input DATA files scanned | 1 | 0 | 2 |
| Cleaner DATA bytes unlinked | 4,618,880 | 0 | 4,470,976 |
| Replacement DATA bytes written | 8,512 | 0 | 0 |
| Obsolete Put bytes remaining | 4,470,848 | 4,470,848 | 0 |

The interleaved cleaner reclaims history that the initial checkpoints already
removed. All three arms retain the same logical contents. Advice-conditioned
Vertex scans incurred 28.36, 24.23 and 24.10 MiB of process reads respectively.
The small difference between the ordered layouts and the extra files are a
maintenance tradeoff. The fixture intentionally puts more than half of the
mixed snapshot's bytes in cold Vertex rows, so the normal cleaner leaves it
in place after the Edge and relay overwrite. This geometry establishes when
isolation can help. Production savings depend on the frequency and lifetime
of these boundaries.

A separate write control uses identical 32-Edge and one-relay messages for 16
paired operations per arm. Median elapsed time was 1.31 ms for one mixed Apply
and 3.75 ms for two sequential Apply calls to independent DBs. Closing the
relay DB before the second call left the reopened graph on version 17 and
relay on version 16. Lantern's cross-family updates require a shared commit
and Snapshot boundary.

The prior production-sorter probe isolated sorting and scratch merges on
already family-grouped input. Its family-local runs reduced medians from
126.7 to 94.5 ms at 123,904 events and from 1,729.6 to 1,444.3 ms at
991,232 events. That probe excludes grouping mixed DATA, copying payloads and
publication. The end-to-end run above keeps the same sorter in both ordered
arms and evaluates only the output boundary. Actual Lantern replay is needed
to justify changing sorting as well. [Prior component measurements](https://github.com/anaregdesign/tarp/issues/194#issuecomment-7218560697)

## Reproduction

From the committed revision containing this driver, run the bounded profile
with Docker and a fresh results directory:

```sh
benchmark/scripts/container-key-family-layout.sh /path/to/new-results
```

The runner records the committed revision, latest stable Go release, pinned
Bookworm image digest, Docker context, selected mount and overlay hashes.
`results.jsonl` retains 75 observations, including the rotated rounds, exact
DATA bytes, whole-state digests and the independent-DB control.
The benchmark uses a disposable Docker volume and requires no host cache
eviction or disk exhaustion. [Driver](../../benchmark/key_family_layout_linux_test.go) · [Overlay](../../benchmark/scripts/key-family-layout-overlay.py)
