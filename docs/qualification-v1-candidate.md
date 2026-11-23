# Current-engine durability qualification

Tracking: [Issue #168](https://github.com/anaregdesign/tarp/issues/168).

## Checked source and scope

This campaign checks published revision
[`73d5cb8f73e78cb7701f5cb0bec5a3f8108e8dfc`](https://github.com/anaregdesign/tarp/commit/73d5cb8f73e78cb7701f5cb0bec5a3f8108e8dfc),
not an uncommitted working tree, a moving branch tip, or the historical
`347a1a1` implementation. All dispatches selected
`feature/compact-by-key-150`; the branch's exact SHA was checked before each
dispatch, and each run's actual `head_sha` must match the checked revision.
The source archive, raw Actions artifacts and archive SHA-256 values are
retained separately from this summary.

The existing phase matrix is exercised on both supported Go families, plus
the complete race-instrumented test suite and coverage-guided decoder fuzzing
inside the disposable guest. This does **not** by itself qualify every new
publication path for a v1 release. Dedicated missing paths are listed below.
Public API and compatibility-policy decisions are outside this campaign.

## Execution record

All 14 runs completed successfully on 2026-11-23. Independent reconciliation
of the downloaded artifacts also passed. The resolved toolchains were
Go 1.27.4 (`stable`) and Go 1.26.11 (`1.26.x`).

| Group | Cases per family | Go 1.27.4: pass | Go 1.26.11: pass |
| --- | ---: | --- | --- |
| Initialization | 25 | [45109494865](https://github.com/anaregdesign/tarp/actions/runs/45109494865) | [45109535761](https://github.com/anaregdesign/tarp/actions/runs/45109535761) |
| Mutations and rotation | 57 | [45109501156](https://github.com/anaregdesign/tarp/actions/runs/45109501156) | [45109541546](https://github.com/anaregdesign/tarp/actions/runs/45109541546) |
| Source-order DATA checkpoint | 26 | [45109507048](https://github.com/anaregdesign/tarp/actions/runs/45109507048) | [45109546866](https://github.com/anaregdesign/tarp/actions/runs/45109546866) |
| Backup and restore | 34 | [45109513350](https://github.com/anaregdesign/tarp/actions/runs/45109513350) | [45109552440](https://github.com/anaregdesign/tarp/actions/runs/45109552440) |
| Tail recovery and reader/writer/backup overlap | 14 | [45109519059](https://github.com/anaregdesign/tarp/actions/runs/45109519059) | [45109557770](https://github.com/anaregdesign/tarp/actions/runs/45109557770) |
| Actual DATA/checkpoint ENOSPC and post-retry cut | 1 | [45109524448](https://github.com/anaregdesign/tarp/actions/runs/45109524448) | [45109563324](https://github.com/anaregdesign/tarp/actions/runs/45109563324) |
| Full guest race suite and guided decoder fuzz | 15 packages + 15s fuzz | [45109529841](https://github.com/anaregdesign/tarp/actions/runs/45109529841) | [45109568975](https://github.com/anaregdesign/tarp/actions/runs/45109568975) |

This is **157 engine cuts and separate cold reboots per family, 314 total**,
including the two post-exhaustion cuts. The current list was enumerated and
checked against each executed case; the historical 330-cut count is not
reused. Fourteen independent device controls add 14 raw-device cuts, not
14 additional engine cases. Both 15-package full race suites passed.
Guided fuzz completed baseline gathering and 3,365 executions on Go 1.27.4,
and 4,963 executions on Go 1.26.11. Vet and the single-module audit passed
in both runtime jobs.

The common library-source manifest fingerprint is
`99912968b43b0dae2c0b4260a59201ff799715f58ebfaef82547c2772544f301`.
The retained GitHub source tarball has SHA-256
`ee6b8e5b9f749b26d092d4c2e662eeac9ee8d77e2d6fa6ad55c3dd22ae8e16dc`.
Every archive's library-source manifest was reconciled against that source.

Each job reruns the independent raw-device control: synced A must survive,
unsynced B must be discarded after a whole-QEMU SIGKILL and NBD termination.
An engine case is accepted only when its exact phase is witnessed externally,
the guest is killed, the backend is quiescent, the volatile overlay is lost,
and a separate cold boot validates file-only recovery.

The selected stack was Ubuntu 24.04.5 amd64, guest kernel
6.8.0-147-generic, QEMU 8.2.2, TCG with two guest CPUs and 1 GiB RAM,
and a guarded 256 MiB ext4 `data=ordered` device with normal barriers,
no DAX and no quotas. The Ubuntu image/kernel hashes and full superblock
features/mounts are retained in the raw records. Hosted Azure/AWS kernels
are recorded separately and are **not** the guest kernel.

Eighteen engine cuts discarded nonzero volatile overlay: 196,608
overlay-page bytes in total (allocated 4 KiB-page accounting, not a count
of distinct user-value bytes). Other engine cuts still lost the full
guest RAM/page cache. Device controls discarded another 57,344 overlay-page
bytes in total. Observed guest FUA traffic was zero.
For engine cuts the NBD server terminated with EOF in 298 cases and the
expected Unix-socket connection reset after SIGKILL in 16 cases.
The controller joins the server before discarding overlay state.
Neither connection reset nor a zero-dirty-overlay cut is silently treated
as data loss or used as proof that every device acknowledged only stable writes.

The witness checks acknowledged values, acknowledged Deletes, the permitted
whole old/new outcome of one pending mutation, its actual selected sequence,
the complete live-key cardinality and a SHA-256 state digest. Required
backup/restore outputs must validate at the captured sequence. Pre-commit
initialization can report an explicit uninitialized database; that outcome
is forbidden after the initial frontier's completed directory sync.

The exhaustion group fills only a regular file on a guarded 256 MiB guest
device. It checks pre-publication checkpoint failure with 128-256 KiB
headroom, source health, retry after freeing space, backup pins and reclamation,
admitted-write ENOSPC, uniform fail-stop behavior, repeated Close errors,
reopen, and an externally witnessed final cut/reboot.
Both runs observed zero available bytes and real write-time errno 28.
Exactly 131,072 available bytes produced a healthy `NotApplied` checkpoint
failure. Retry after releasing 2 MiB succeeded; the slow backup retained
1,048,384 retired DATA bytes and releasing its pins reclaimed exactly that
logical length. The subsequent admitted Put returned `NotApplied` with
ENOSPC and fail-stopped hot/miss requests. Reopen and the final cold boot
verified the acknowledged state and captured output.

## Evidence retention and reconciliation

Raw artifacts retain source and boot fingerprints, exact resolved Go patches,
host/QEMU details, guest image/kernel hashes, mounts/features, external
oracle journals, cut/recovery console logs, NBD counters and final verdicts.
The runtime jobs also retain every test binary's package/hash/build manifest
and actual guided-fuzz output.

The downloaded archives are checked against GitHub's artifact digests.
An independent standard-library Python verifier replays every oracle,
recomputes the state digest and cardinality at the reported recovered sequence,
checks whole-guest cut/reboot outcomes, and reconciles the source manifest
with the retained source archive. Its source and focused tests are retained
with the evidence. An Actions green check alone is not the evidence record.

| Raw archive run | SHA-256 |
| --- | --- |
| 45109494865 | `685a5468d6e86a6ebb8ec68bd63db4cd09d6bf2b78444f5f1a28f63e0d0ae8cb` |
| 45109501156 | `3e69ffe27ce2753cbe0015143b208787a518129bdec8dacdc1d24e816e17d563` |
| 45109507048 | `137d13e5bbe5eb862fb7806b2460ee57dd68d68e1ff4c4beaf492cffaa42f658` |
| 45109513350 | `93fe3de7f54269b24fa2aa3622b8224dcc987f4f55c0ee5a174e50d9bde00295` |
| 45109519059 | `08a31ef525063f4a64f8a933ed69cf3deef0bf0b4ea6be8bd0eb72905a622175` |
| 45109524448 | `3c0fb65c94019b0f79b99e33184098ad3af1b63c08c27882cd967a7960bc0f6f` |
| 45109529841 | `71d69900ff460a0102b30641b1578c7600c0566bdf52b1e344c70bcca05038fc` |
| 45109535761 | `48235c6d9e2188ab475b7dece97d4e46f79af7643913da8ccbe168da8f146cfa` |
| 45109541546 | `1958b68e4a6d2392025aedf23f8ad6d7c0f14b1418882861066dfbe11b7d873b` |
| 45109546866 | `d93b5421f1d353e843469632024e0ba438d902fd3a1944e4f7c3b08605e93a90` |
| 45109552440 | `c2d0de930bc411fae28ab9b3f70e126a8e37da0047e4fdeb4d78e2406a0fd6dd` |
| 45109557770 | `1bf084097ff5931205cadb92d9199224acc9f439958294206fabaef54e830baf` |
| 45109563324 | `0d8045d909a6fb8a8bec0f080ed8cbebe63e5656a2c6912c3449d1f25325d550` |
| 45109568975 | `fd61cde62e7d4565db2d4da99de896c4df486bde90d6b763d4e41ec027ed4e3a` |

Actions retention is 30 days. The downloaded raw archives and source archive
must be retained beyond that window for release provenance. The final bundle
contains a run manifest, per-file hashes and independent-verification results.

## Coverage limits and remaining v1 gate

The existing full guest suite includes deterministic fault injection and
process-crash coverage of current engine behavior. These are valuable but
are not equivalent to dedicated whole-VM power cuts.

| Path | Existing evidence exercised by this campaign | Dedicated whole-VM work still required |
| --- | --- | --- |
| BatchPut and BatchDelete | Atomic outcome, rotation/accounting, overlapping scan/checkpoint/backup and injected publication failures | An external batch-aware witness must reject every intermediate sequence and mixed old/new key set; cut append, rotation and CURRENT publication, including duplicates and delete/reinsert |
| Automatic incremental compaction | Cleaner publication process-crash matrix, fault injection, concurrent-tail rebasing and grouped/repacked recovery | Cut single-segment, grouped-log and small-segment-repacking output/PARITY, catalog/CURRENT publication, remapping and retirement; include a pinned reader/backup and concurrent tail |
| Key-ordered checkpoint | Initial/delta sorting, unchanged-range reuse, concurrent batches, corruption and failed-publication/retry tests | Cut initial and delta builds, reused-range publication, remapping and cleanup; independently check exact cutoff state and reused-file ownership |
| INDEX checkpoint and Indexed reopen | Packing/reopen, concurrent rotations, corruption/repair and injected publication failures | Cut INDEX output and selected descriptor publication, explicit CheckpointIndex and Close, then verify both Full and Indexed reopen |
| Online DATA repair and PARITY | Verified repair, uncertain replacement/retry, corruption and source/output ownership tests | Cut protected replacement/PARITY publication and directory sync with one damaged block; verify the acknowledged logical state after cold boot |
| B+tree private-pager exhaustion | Earlier focused current-format campaign is linked in the main qualification record | Repeat private-pager ENOSPC on the selected release candidate, including NotApplied batch retry and cold boot |

The matrix's ordinary cases use the default B+tree with a 16 MiB budget.
The full guest suites exercise both index modes. Those full-suite results
must not be described as a complete whole-VM phase matrix for both modes.
The exhaustion fixture parks automatic maintenance to attribute staging
failure, retry and reclamation to its manual checkpoints; it does not prove
automatic-maintenance ENOSPC behavior by itself.

Extending the remote matrix requires a reviewed, published harness on a fixed
revision. This campaign leaves those dedicated paths explicitly unqualified.
This is an evidence gap, not an observed corruption verdict. The tracking
issue remains open; the passing existing matrix is not a completed v1 gate.

Only the precisely recorded disposable Ubuntu amd64, ext4 `data=ordered`,
honest-flush volatile NBD stack is in scope. No physical SSD/controller
certification, other filesystem/kernel guarantee, quota-enabled filesystem,
lying-flush tolerance or generic macOS power-loss guarantee is implied.
See the [durability contract](durability.md) and
[qualification procedure](qualification.md).
